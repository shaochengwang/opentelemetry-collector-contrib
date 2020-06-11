package awspmdexporter

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awspmdexporter/handler"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/consumer/pdatautil"
	"go.uber.org/zap"
)

const (
	defaultMaxDatumsPerCall        = 20     // PutMetricData only supports up to 20 data metrics per call by default
	defaultMaxValuesPerDatum       = 150    // By default only these number of values can be inserted into the value list
	bottomLinePayloadSizeToPublish = 200000 // Leave 9600B for the last datum buffer. 200KB payload size, 5:1 compression ratio estimate.
	metricChanBufferSize           = 10000
	datumBatchChanBufferSize       = 50 // the number of requests we buffer
	maxConcurrentPublisher         = 10 // the number of CloudWatch clients send request concurrently
	pushIntervalInSec              = 60 // 60 sec
	highResolutionTagKey           = "aws:StorageResolution"
	defaultRetryCount              = 5 // this is the retry count, the total attempts would be retry count + 1 at most.
	backoffRetryBase               = 200
)

// LogsClient contains the CloudWatch API calls used by this plugin
type LogsClient interface {
	PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
}


type cwClient struct {
	logger 		*zap.Logger
	client 		cloudwatchiface.CloudWatchAPI
	config 		configmodels.Exporter
	retries 	int
	rateMap		map[string]float64
}

// NewCloudWatchClient create cwClient
func NewCloudWatchClient(logger *zap.Logger, awsConfig *aws.Config, session *session.Session) *cwClient {

	svc := cloudwatch.New(session, awsConfig)
	svc.Handlers.Build.PushBackNamed(handler.UserAgentRequestHandler)

	cwClient := &cwClient{
		logger: logger,
		client: svc,
		rateMap: make(map[string]float64),
	}
	return cwClient
}

func (cw *cwClient) pushMetricsData(
	_ context.Context,
	md pdata.Metrics,
) (int, error) {
	imd := pdatautil.MetricsToInternalMetrics(md)
	cw.logger.Info("MetricsExporter", zap.Int("#metrics", imd.MetricCount()))
	rms := imd.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if rm.IsNil() {
			// buf.logEntry("* Nil ResourceMetrics")
			continue
		}
		if !rm.Resource().IsNil() {
			// buf.logAttributeMap("Resource labels", rm.Resource().Attributes())
		}
		ilms := rm.InstrumentationLibraryMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ilm := ilms.At(j)
			if ilm.IsNil() {
				// buf.logEntry("* Nil InstrumentationLibraryMetrics")
				continue
			}
			if !ilm.InstrumentationLibrary().IsNil() {
				// TODO: use instrument lib name as namespace
				// buf.logInstrumentationLibrary(ilm.InstrumentationLibrary())
			}
			metrics := ilm.Metrics()
			var metricDatums []*cloudwatch.MetricDatum
			var int64Datums []*cloudwatch.MetricDatum
			var summaryDatums []*cloudwatch.MetricDatum
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if metric.IsNil() {
					// buf.logEntry("* Nil Metric")
					continue
				}
				mDesc := metric.MetricDescriptor()
				if mDesc.IsNil() {
					continue
				}
				idp := metric.Int64DataPoints()
				if idp.Len() > 0 {
					for m := 0; m < idp.Len(); m++ {
						dp := idp.At(m)
						int64Datums = append(int64Datums, buildMetricDatumValue(dp, mDesc, cw.rateMap)...)
					}
				}
				sdp := metric.SummaryDataPoints()
				if sdp.Len() > 0 {
					for n := 0; n < sdp.Len(); n++ {
						dp := sdp.At(n)
						summaryDatums = append(summaryDatums, buildMetricDatumSummary(dp, mDesc)...)
					}
				}
			}
			//merge datums and write to CW
			metricDatums = append(metricDatums, int64Datums...)
			metricDatums = append(metricDatums, summaryDatums...)
			if len(metricDatums) > 0 {
				var metricsSent string
				for _, metricDatum := range metricDatums {
					metricsSent = fmt.Sprintf("%s%s,", metricsSent, *metricDatum.MetricName)
				}
				fmt.Println("Sending metrics to CW: %s", metricsSent)
				cw.writeToCloudWatch(metricDatums)
			}
		}
	}

	return 0, nil
}

// Create MetricDatums according to metric roll up requirement for each field in a Point. Only fields with values that can be
// converted to float64 are supported. Non-supported fields are skipped.
func buildMetricDatumValue(point pdata.Int64DataPoint, mDesc pdata.MetricDescriptor, rateMap map[string]float64) []*cloudwatch.MetricDatum {

	rawDimensions := BuildDimensions(point.LabelsMap())
	dimensionsList := [][]*cloudwatch.Dimension{rawDimensions}
	var datums []*cloudwatch.MetricDatum
	var unit string
	var value float64
	var rate float64
	var metricNameRate *string
	value = float64(point.Value())
	metricName := aws.String(mDesc.Name())
	//calculate rate if necessary
	if pdata.MetricTypeCounterInt64 == mDesc.Type() || pdata.MetricTypeCounterDouble == mDesc.Type() {
		metricNameRate = aws.String(mDesc.Name() + "Rate")
		var cancatLabels string
		point.LabelsMap().ForEach(func(k string, v pdata.StringValue) {
			cancatLabels = cancatLabels + k + ":" + v.Value() + ","
		})
		if v, ok := rateMap[mDesc.Name() + "Rate" + cancatLabels]; ok {
			rate = value - v
		}
		rateMap[mDesc.Name() + "Rate" + cancatLabels] = value
	}
	for _, dimensions := range dimensionsList {
		datum := &cloudwatch.MetricDatum{
			MetricName: metricName,
			Dimensions: dimensions,
			Timestamp:  aws.Time(time.Unix(0, int64(point.Timestamp()))),
			Unit: aws.String(mDesc.Unit()),
			Value:      aws.Float64(value),
		}
		if unit != "" {
			datum.SetUnit(unit)
		}
		datums = append(datums, datum)
		//add rate if necessary
		if pdata.MetricTypeCounterInt64 == mDesc.Type() || pdata.MetricTypeCounterDouble == mDesc.Type() {
			datum = &cloudwatch.MetricDatum{
				MetricName: metricNameRate,
				Dimensions: dimensions,
				Timestamp:  aws.Time(time.Unix(0, int64(point.Timestamp()))),
				Unit:       aws.String(mDesc.Unit()),
				Value:      aws.Float64(rate),
			}
			datums = append(datums, datum)
		}
	}
	return datums
}

func buildMetricDatumSummary(point pdata.SummaryDataPoint, mDesc pdata.MetricDescriptor) []*cloudwatch.MetricDatum {

	rawDimensions := BuildDimensions(point.LabelsMap())
	dimensionsList := [][]*cloudwatch.Dimension{rawDimensions}
	var datums []*cloudwatch.MetricDatum
	var min float64
	var max float64
	//var value float64
	//value = float64(point.Value())
	var percentileSlice pdata.SummaryValueAtPercentileSlice
	percentileSlice = point.ValueAtPercentiles()
	for i := 0; i < percentileSlice.Len(); i++ {
		summaryValue := percentileSlice.At(i)
		if summaryValue.Percentile() == 0.0 {
			min = summaryValue.Value()
		}
		if summaryValue.Percentile() == 100.0 {
			max = summaryValue.Value()
		}
	}
	metricName := aws.String(mDesc.Name())
	fmt.Printf("Metric %s has max %f min %f sample count %d and sum %f \n", mDesc.Name(), max, min, point.Count(), point.Sum())
	for _, dimensions := range dimensionsList {
		datum := &cloudwatch.MetricDatum{
			MetricName: metricName,
			Dimensions: dimensions,
			Timestamp:  aws.Time(time.Unix(0, int64(point.Timestamp()))),
			Unit: aws.String(mDesc.Unit()),
			StatisticValues: &cloudwatch.StatisticSet{
				Maximum: aws.Float64(max),
				Minimum: aws.Float64(min),
				SampleCount: aws.Float64(float64(point.Count())),
				Sum: aws.Float64(point.Sum()),
			},
		}
		datums = append(datums, datum)
	}
	return datums
}

// Make a list of Dimensions by using a Point's tags. CloudWatch supports up to
// 10 dimensions per metric so we only keep up to the first 10 alphabetically.
// This always includes the "host" tag if it exists.
func BuildDimensions(mTags pdata.StringMap) []*cloudwatch.Dimension {

	const MaxDimensions = 10
	dimensions := make([]*cloudwatch.Dimension, int(math.Min(float64(mTags.Len()), MaxDimensions)))

	i := 0

	// This is pretty ugly but we always want to include the "host" tag if it exists.
	if host, ok := mTags.Get("host"); ok {
		dimensions[i] = &cloudwatch.Dimension{
			Name:  aws.String("host"),
			Value: aws.String(host.Value()),
		}
		i += 1
	}

	var keys []string
	mTags.ForEach(func(k string, v pdata.StringValue) {
		if k != "host" {
			keys = append(keys, k)
		}
	})
	sort.Strings(keys)

	for _, k := range keys {
		if i >= MaxDimensions {
			break
		}
		v, _ := mTags.Get(k)
		dimensions[i] = &cloudwatch.Dimension{
			Name:  aws.String(k),
			Value: aws.String(v.Value()),
		}
		i += 1
	}

	return dimensions
}

func (cw *cwClient) writeToCloudWatch(req interface{}) {
	datums := req.([]*cloudwatch.MetricDatum)
	params := &cloudwatch.PutMetricDataInput{
		MetricData: datums,
		Namespace:  aws.String("pmd-exporter-test"),
	}
	var err error
	for i := 0; i < defaultRetryCount; i++ {
		_, err = cw.client.PutMetricData(params)

		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok {
				log.Printf("E! Cannot cast PutMetricData error %v into awserr.Error.", err)
				cw.backoffSleep()
				continue
			}
			switch awsErr.Code() {
			case cloudwatch.ErrCodeLimitExceededFault, cloudwatch.ErrCodeInternalServiceFault:
				log.Printf("W! cloudwatch putmetricdate met issue: %s, message: %s",
					awsErr.Code(),
					awsErr.Message())
				cw.backoffSleep()
				continue

			default:
				log.Printf("E! cloudwatch: code: %s, message: %s, original error: %+v", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
				cw.backoffSleep()
			}
		} else {
			cw.retries = 0
		}
		break
	}
	if err != nil {
		log.Println("E! WriteToCloudWatch failure, err: ", err)
	}
}

//sleep some back off time before retries.
func (cw *cwClient) backoffSleep() {
	var backoffInMillis int64 = 60 * 1000 // 1 minute
	if cw.retries <= defaultRetryCount {
		backoffInMillis = int64(backoffRetryBase * math.Pow(2, float64(cw.retries)))
	}
	sleepDuration := time.Millisecond * time.Duration(backoffInMillis)
	log.Printf("W! %v retries, going to sleep %v before retrying.", cw.retries, sleepDuration)
	cw.retries++
	time.Sleep(sleepDuration)
}
