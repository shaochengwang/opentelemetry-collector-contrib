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
	overallConstPerRequestSize     = 39
	namespaceOverheads 			   = 11

	// &MetricData.member.100.StatisticValues.Maximum=1558.3086995967291&MetricData.member.100.StatisticValues.Minimum=1558.3086995967291&MetricData.member.100.StatisticValues.SampleCount=1000&MetricData.member.100.StatisticValues.Sum=1558.3086995967291
	statisticsSize = 246
	// &MetricData.member.100.Timestamp=2018-05-29T21%3A14%3A00Z
	timestampSize = 57
	// &MetricData.member.100.Dimensions.member.1.Name= &MetricData.member.100.Dimensions.member.1.Value=
	dimensionOverheads = 48 + 49
	// &MetricData.member.100.MetricName=
	metricNameOverheads = 34
	// &MetricData.member.100.StorageResolution=1
	highResolutionOverheads = 42
	// &MetricData.member.100.Values.member.100=1558.3086995967291 &MetricData.member.100.Counts.member.100=1000
	valuesCountsOverheads = 59 + 45
	// &MetricData.member.100.Value=1558.3086995967291
	valueOverheads = 47
	// &MetricData.member.1.Unit=Kilobytes/Second
	unitOverheads = 42
)

// LogsClient contains the CloudWatch API calls used by this plugin
type LogsClient interface {
	PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
}

type MetricDatumBatch struct {
	MaxDatumsPerCall    int
	Partition           []*cloudwatch.MetricDatum
	BeginTime           time.Time
	Size                int
	perRequestConstSize int
}

func newMetricDatumBatch(maxDatumsPerCall, perRequestConstSize int) *MetricDatumBatch {
	return &MetricDatumBatch{
		MaxDatumsPerCall:    maxDatumsPerCall,
		Partition:           make([]*cloudwatch.MetricDatum, 0, maxDatumsPerCall),
		BeginTime:           time.Now(),
		Size:                perRequestConstSize,
		perRequestConstSize: perRequestConstSize,
	}
}

func (b *MetricDatumBatch) clear() {
	b.Partition = make([]*cloudwatch.MetricDatum, 0, b.MaxDatumsPerCall)
	b.BeginTime = time.Now()
	b.Size = b.perRequestConstSize
}

func (b *MetricDatumBatch) isFull() bool {
	return len(b.Partition) >= b.MaxDatumsPerCall || b.Size >= bottomLinePayloadSizeToPublish
}


type cwClient struct {
	logger 				*zap.Logger
	client 				cloudwatchiface.CloudWatchAPI
	config 				configmodels.Exporter
	retries 			int
	rateMap				map[string]float64
	forceFlushInterval  time.Duration
	metricChan      	chan *cloudwatch.MetricDatum
	datumBatchChan      chan []*cloudwatch.MetricDatum
	metricDatumBatch	*MetricDatumBatch
	Namespace          	string            // CloudWatch Metrics Namespace

}

// NewCloudWatchClient create cwClient
func NewCloudWatchClient(logger *zap.Logger, awsConfig *aws.Config, session *session.Session, cfg *Config) *cwClient {

	svc := cloudwatch.New(session, awsConfig)
	svc.Handlers.Build.PushBackNamed(handler.UserAgentRequestHandler)

	cwClient := &cwClient{
		logger: logger,
		client: svc,
		rateMap: make(map[string]float64),
		forceFlushInterval: time.Duration(cfg.FlushInterval) * time.Second,
	}
	cwClient.metricChan = make(chan *cloudwatch.MetricDatum, metricChanBufferSize)
	cwClient.datumBatchChan = make(chan []*cloudwatch.MetricDatum, datumBatchChanBufferSize)
	perRequestConstSize := overallConstPerRequestSize + len(cwClient.Namespace) + namespaceOverheads
	cwClient.metricDatumBatch = newMetricDatumBatch(defaultMaxDatumsPerCall, perRequestConstSize)

	if cwClient.forceFlushInterval == 0 {
		cwClient.forceFlushInterval = pushIntervalInSec * time.Second
	}
	fmt.Printf("The forceFlushInterval is %f seconds \n", cwClient.forceFlushInterval.Seconds())
	go cwClient.pushMetricDatum()
	go cwClient.pushMetricDatumBatch()
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
				//var metricsSent string
				for _, metricDatum := range metricDatums {
					//metricsSent = fmt.Sprintf("%s%s,", metricsSent, *metricDatum.MetricName)
					cw.metricChan <- metricDatum
				}

				//fmt.Println("Batching metrics to CW: ", metricsSent)
				//cw.writeToCloudWatch(metricDatums)
			}
		}
	}

	return 0, nil
}

// Write data for a single point. A point can have many fields and one field
// is equal to one MetricDatum. There is a limit on how many MetricDatums a
// request can have so we process one Point at a time.
func (cw *cwClient) pushMetricDatum() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case datum := <-cw.metricChan:
			cw.metricDatumBatch.Partition = append(cw.metricDatumBatch.Partition, datum)
			cw.metricDatumBatch.Size += payload(datum)
			if cw.metricDatumBatch.isFull() {
				// if batch is full
				cw.datumBatchChan <- cw.metricDatumBatch.Partition
				cw.metricDatumBatch.clear()
			}
		case <-ticker.C:
			if cw.timeToPublish(cw.metricDatumBatch) {
				// if the time to publish comes
				cw.datumBatchChan <- cw.metricDatumBatch.Partition
				cw.metricDatumBatch.clear()
			}
		//case <-cw.shutdownChan:
		//	return
		}
	}
}

func (cw *cwClient) pushMetricDatumBatch() {
	for {
		select {
		case datumBatch := <-cw.datumBatchChan:
			fmt.Println("Publishing metrics to CW")

			var metricsSentMap = make(map[string]float64)
			for _, metricDatum := range datumBatch {
				var dim string
				for _, dimension := range metricDatum.Dimensions {
					dim = fmt.Sprintf("%s%s:%s,", dim, *dimension.Name, *dimension.Value)
				}
				key := fmt.Sprintf("%s;%s", *metricDatum.MetricName, dim)
				if val, ok := metricsSentMap[key]; ok {
					metricsSentMap[key] = val + getSampleCountOrDefault(metricDatum)
				} else {
					metricsSentMap[key] = getSampleCountOrDefault(metricDatum)
				}
			}
			var metricsSent string
			for key, val := range metricsSentMap {
				metricsSent = fmt.Sprintf("%s%s-%f|",metricsSent, key, val)
			}
			fmt.Println("Publishing metrics: " + metricsSent)

			cw.writeToCloudWatch(datumBatch)
		default:
		}
	}
}

func getSampleCountOrDefault(m *cloudwatch.MetricDatum) float64 {
	if m.StatisticValues == nil {
		return float64(1)
	}
	return *m.StatisticValues.SampleCount
}

func (cw *cwClient) timeToPublish(b *MetricDatumBatch) bool {
	return len(b.Partition) > 0 && time.Now().Sub(b.BeginTime) >= cw.forceFlushInterval
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

func payload(datum *cloudwatch.MetricDatum) (size int) {
	size += timestampSize

	for _, dimension := range datum.Dimensions {
		size += len(*dimension.Name) + len(*dimension.Value) + dimensionOverheads
	}

	if datum.MetricName != nil {
		// The metric name won't be nil, but it should fail in the validation instead of panic here.
		size += len(*datum.MetricName) + metricNameOverheads
	}

	if datum.StorageResolution != nil {
		size += highResolutionOverheads
	}

	valuesCountsLen := len(datum.Values)
	if valuesCountsLen != 0 {
		size += valuesCountsLen*valuesCountsOverheads + statisticsSize
	} else {
		size += valueOverheads
	}

	if datum.Unit != nil {
		size += unitOverheads
	}

	return
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
