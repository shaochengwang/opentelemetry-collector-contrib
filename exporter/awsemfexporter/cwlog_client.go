package awsemfexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/handler"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/translator"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/consumer/pdatautil"
	"go.uber.org/zap"
)

const (
	// See: http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	perEventBytes          = 26
	maximumBytesPerPut     = 1048576
	maximumLogEventsPerPut = 10000
	maximumTimeSpanPerPut  = time.Hour * 24
	// Log stream objects that are empty and inactive for longer than the timeout get cleaned up
	logStreamInactivityTimeout = time.Hour
	// Check for expired log streams every 10 minutes
	logStreamInactivityCheckInterval = 10 * time.Minute
	// this is the retry count, the total attempts would be retry count + 1 at most.
	defaultRetryCount = 5
)

var (
	// backoff retry 6 times
	sleeps = []time.Duration{
		time.Millisecond * 200, time.Millisecond * 400, time.Millisecond * 800,
		time.Millisecond * 1600, time.Millisecond * 3200, time.Millisecond * 6400}
)

// LogsClient contains the CloudWatch API calls used by this plugin
type LogsClient interface {
	PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
}

type logStream struct {
	logEvents         []*cloudwatchlogs.InputLogEvent
	currentByteLength int
	currentBatchStart *time.Time
	currentBatchEnd   *time.Time
	nextSequenceToken *string
	logStreamName     string
	expiration        time.Time
}

type emfLogData struct {
	logGroupName                  string
	logStreamName                 string
	streams                       map[string]*logStream
	nextLogStreamCleanUpCheckTime time.Time
	logGroupCreated               bool
}

type cwlClient struct {
	logger *zap.Logger
	client *cloudwatchlogs.CloudWatchLogs
	emfLogData
	config     configmodels.Exporter
	translator *translator.EmfTranslator
}

func (cwl *cwlClient) pushMetricsData(
	_ context.Context,
	md pdata.Metrics,
) (int, error) {
	imd := pdatautil.MetricsToInternalMetrics(md)
	cwl.logger.Info("EMFMetricsExporter", zap.Int("#metrics count", imd.MetricCount()))
	rms := imd.ResourceMetrics()
	cwMetricLists := []*translator.CWMetrics{}

	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if rm.IsNil() {
			continue
		}
		// translate OT metric datapoints into CWMetricLists
		cwm, err := cwl.translator.TranslateOtToCWMetric(&rm, &cwl.config)
		if err != nil || cwm == nil {
			return 0, nil
		}
		// append all datapoint metrics in the request into CWMetric list
		for _, v := range cwm {
			cwMetricLists = append(cwMetricLists, v)
		}
	}

	// convert CWMetric into map format for compatible with PLE input
	ples := make([]*cloudwatchlogs.InputLogEvent, 0, maximumLogEventsPerPut)
	for _, met := range cwMetricLists {
		cwmMap := make(map[string]interface{})
		fieldMap := met.Fields
		cwmMap["CloudWatchMetrics"] = met.Measurements
		cwmMap["Timestamp"] = met.Timestamp
		fieldMap["_aws"] = cwmMap

		pleMsg, err := json.Marshal(fieldMap)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(pleMsg))
		ples = append(ples, &cloudwatchlogs.InputLogEvent{
			// Message:   aws.String("event" + strconv.Itoa(rand.Intn(100))),
			Message:   aws.String(string(pleMsg)),
			Timestamp: aws.Int64(time.Now().UnixNano() / 1e6), // CloudWatch uses milliseconds since epoch
		})

	}

	expConfig := cwl.config.(*Config)
	logGroup := "otel-lg"
	logStream := "otel-stream"
	// override log group if found it in exp configuraiton
	if len(expConfig.LogGroupName) > 0 {
		logGroup = expConfig.LogGroupName
	}
	if len(expConfig.LogStreamName) > 0 {
		logStream = expConfig.LogStreamName
	}
	// hardcode log group and log stream for now
	token, _ := cwl.CreateStream(aws.String(logGroup), aws.String(logStream))

	response, err := cwl.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     ples,
		LogGroupName:  aws.String(logGroup),
		LogStreamName: aws.String(logStream),
		// SequenceToken: aws.String(token),
	})
	if err != nil {
		// cwl.logger.Error("PutLogEvents errout", zap.Error(err))
		awsErr, _ := err.(awserr.Error)
		if awsErr.Code() == cloudwatchlogs.ErrCodeInvalidSequenceTokenException {
			parts := strings.Split(awsErr.Message(), " ")
			token = parts[len(parts)-1]
			// cwl.logger.Info("ErrCodeInvalidSequenceTokenException==", zap.String("#token", token))
			cwl.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
				LogEvents:     ples,
				LogGroupName:  aws.String(logGroup),
				LogStreamName: aws.String(logStream),
				SequenceToken: aws.String(token),
			})
			if err != nil {
				// cwl.logger.Error("PutLogEvents errout again", zap.Error(err))
				return 0, err
			}
		}
		return 0, err
	}

	// will need to save nextseq token somewhere
	if response != nil {
		if response.NextSequenceToken != nil {
			token = *response.NextSequenceToken
			cwl.logger.Error("PutLogEvents NextSequenceToken", zap.String("NextSequenceToken", token))
		}
	}

	return 0, nil
}

// NewCloudWatchLogsClient create cwlog
func NewCloudWatchLogsClient(logger *zap.Logger, awsConfig *aws.Config, sess *session.Session) *cloudwatchlogs.CloudWatchLogs {
	client := cloudwatchlogs.New(sess, awsConfig)
	client.Handlers.Build.PushBackNamed(handler.RequestStructuredLogHandler)
	return client
}

//Prepare the readiness for the log group and log stream.
func (cwl *cwlClient) CreateStream(logGroup, streamName *string) (token string, e error) {
	//CreateLogStream / CreateLogGroup
	_, e = callFuncWithRetries(
		func() (string, error) {
			_, err := cwl.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
				LogGroupName:  logGroup,
				LogStreamName: streamName,
			})
			if err != nil {
				log.Printf("D! creating stream fail due to : %v \n", err)
				if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == cloudwatchlogs.ErrCodeResourceNotFoundException {
					_, err = cwl.client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
						LogGroupName: logGroup,
					})
				}

			}
			return " ", err
		},
		cloudwatchlogs.ErrCodeResourceAlreadyExistsException,
		fmt.Sprintf("E! CreateLogStream / CreateLogGroup with log group name %s stream name %s has errors.", *logGroup, *streamName))

	if e != nil {
		log.Printf("D! error != nil, return token: %s with error: %v \n", token, e)
		return token, e
	}

	//After a log stream is created the token is always empty.
	return " ", nil
}

//encapsulate the retry logic in this separate method.
func callFuncWithRetries(fn func() (string, error), ignoreException string, errorMsg string) (result string, err error) {
	for i := 0; i <= defaultRetryCount; i++ {
		result, err = fn()
		if err == nil {
			return result, nil
		}
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == ignoreException {
			return result, nil
		}
		log.Printf("%s Will retry the request: %s", errorMsg, err.Error())
		backoffSleep(i)
	}
	return
}

//sleep some back off time before retries.
func backoffSleep(i int) {
	//save the sleep time for the last occurrence since it will exit the loop immediately after the sleep
	backoffDuration := time.Duration(time.Minute * 1)
	if i <= defaultRetryCount {
		backoffDuration = sleeps[i]
	}

	log.Printf("W! It is the %v time, going to sleep %v before retrying.", i, backoffDuration)
	time.Sleep(backoffDuration)
}
