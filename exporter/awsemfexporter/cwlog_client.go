package awsemfexporter

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/handler"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/translator"
	"go.opentelemetry.io/collector/config/configmodels"
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
	ErrCodeThrottlingException = "ThrottlingException"
)

var (
	// backoff retry 6 times
	sleeps = []time.Duration{
		time.Millisecond * 200, time.Millisecond * 400, time.Millisecond * 800,
		time.Millisecond * 1600, time.Millisecond * 3200, time.Millisecond * 6400}
)

//The log client will perform the necessary operations for publishing log events use case.
type LogClient interface {
	PutLogEvents(input *cloudwatchlogs.PutLogEventsInput, retryCnt int) *string
	CreateStream(logGroup, streamName *string) (token string, e error)
}

// Possible exceptions are combination of common errors (https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/CommonErrors.html)
// and API specific erros (e.g. https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html#API_PutLogEvents_Errors)
type CloudWatchLogClient struct {
	svc cloudwatchlogsiface.CloudWatchLogsAPI
	logger *zap.Logger
	emfLogData
	config     configmodels.Exporter
	translator *translator.EmfTranslator
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

//Create a log client based on the actual cloudwatch logs client.
func newCloudWatchLogClient(svc cloudwatchlogsiface.CloudWatchLogsAPI, logger *zap.Logger) *CloudWatchLogClient {
	logClient := &CloudWatchLogClient{svc: svc,
									  logger: logger}
	return logClient
}

// NewCloudWatchLogsClient create cwlog
func NewCloudWatchLogsClient(logger *zap.Logger, awsConfig *aws.Config, sess *session.Session) LogClient {
	client := cloudwatchlogs.New(sess, awsConfig)
	client.Handlers.Build.PushBackNamed(handler.RequestStructuredLogHandler)
	return newCloudWatchLogClient(client, logger)
}

//Put log events. The method mainly handles different possible error could be returned from server side, and retries them
//if necessary.
func (client *CloudWatchLogClient) PutLogEvents(input *cloudwatchlogs.PutLogEventsInput, retryCnt int) *string {
	var response *cloudwatchlogs.PutLogEventsOutput
	var err error
	var token = input.SequenceToken

	for i := 0; i <= retryCnt; i++ {
		input.SequenceToken = token
		response, err = client.svc.PutLogEvents(input)
		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok {
				log.Printf("E! Cannot cast PutLogEvents error %#v into awserr.Error.", err)
				return token
			}
			switch e := awsErr.(type) {
			case *cloudwatchlogs.InvalidParameterException:
				log.Printf("E! cloudwatchlogs: %s for log group %s log stream %s, will not retry the request: %s, original error: %#v, %#v",
					e.Code(),
					*input.LogGroupName,
					*input.LogStreamName,
					e.Message(),
					e.Error(),
					e)
				return token
			case *cloudwatchlogs.InvalidSequenceTokenException: //Resend log events with new sequence token when InvalidSequenceTokenException happens
				log.Printf("W! cloudwatchlogs: %s, will search the next token and retry the request: %s, original error: %#v, %#v",
					e.Code(),
					e.Message(),
					e.Error(),
					e)
				backoffSleep(i)
				token = e.ExpectedSequenceToken
				continue
			case *cloudwatchlogs.DataAlreadyAcceptedException: //Skip batch if DataAlreadyAcceptedException happens
				log.Printf("W! cloudwatchlogs: %s, drop this request and continue to the next request: %s, original error: %#v, %#v",
					e.Code(),
					e.Message(),
					e.Error(),
					e)
				token = e.ExpectedSequenceToken
				return token
			case *cloudwatchlogs.OperationAbortedException: //Retry request if OperationAbortedException happens
				log.Printf("W! cloudwatchlogs: %s, will retry the request: %s, original error: %#v, %#v",
					e.Code(),
					e.Message(),
					e.Error(),
					e)
				backoffSleep(i)
				continue
			case *cloudwatchlogs.ServiceUnavailableException: //Retry request if ServiceUnavailableException happens
				log.Printf("W! cloudwatchlogs: %s, will retry the request: %s, original error: %#v, %#v",
					e.Code(),
					e.Message(),
					e.Error(),
					e)
				backoffSleep(i)
				continue
			case *cloudwatchlogs.ResourceNotFoundException:
				tmpToken, tmpErr := client.CreateStream(input.LogGroupName, input.LogStreamName)
				if tmpErr == nil {
					if tmpToken == "" {
						token = nil
					} else {
						token = &tmpToken
					}
				}
				backoffSleep(i)
				continue
			default:
				// ThrottlingException is handled here because the type cloudwatch.ThrottlingException is not yet available in public SDK
				// Retry request if ThrottlingException happens
				if awsErr.Code() == ErrCodeThrottlingException {
					log.Printf("W! cloudwatchlogs: %s for log group %s log stream %s, will retry the request: %s, original error: %#v, %#v",
						awsErr.Code(),
						*input.LogGroupName,
						*input.LogStreamName,
						awsErr.Message(),
						awsErr.Error(),
						awsErr)
					backoffSleep(i)
					continue
				}
				log.Printf("E! cloudwatchlogs: code: %s, message: %s, original error: %#v, %#v", awsErr.Code(), awsErr.Message(), awsErr.OrigErr(), err)
				backoffSleep(i)
				continue
			}

		}

		if response != nil {
			if response.RejectedLogEventsInfo != nil {
				rejectedLogEventsInfo := response.RejectedLogEventsInfo
				if rejectedLogEventsInfo.TooOldLogEventEndIndex != nil {
					log.Printf("W! %d log events for log group name '%s' are too old", *rejectedLogEventsInfo.TooOldLogEventEndIndex, *input.LogGroupName)
				}
				if rejectedLogEventsInfo.TooNewLogEventStartIndex != nil {
					log.Printf("W! %d log events for log group name '%s' are too new", *rejectedLogEventsInfo.TooNewLogEventStartIndex, *input.LogGroupName)
				}
				if rejectedLogEventsInfo.ExpiredLogEventEndIndex != nil {
					log.Printf("W! %d log events for log group name '%s' are expired", *rejectedLogEventsInfo.ExpiredLogEventEndIndex, *input.LogGroupName)
				}
			}

			if response.NextSequenceToken != nil {
				token = response.NextSequenceToken
				break
			}
		}
	}
	if err != nil {
		log.Printf("E! All retries failed for PutLogEvents. Drop this request.")
	}
	return token
}

//Prepare the readiness for the log group and log stream.
func (client *CloudWatchLogClient) CreateStream(logGroup, streamName *string) (token string, e error) {
	//CreateLogStream / CreateLogGroup
	_, e = callFuncWithRetries(
		func() (string, error) {
			_, err := client.svc.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
				LogGroupName:  logGroup,
				LogStreamName: streamName,
			})
			if err != nil {
				log.Printf("D! creating stream fail due to : %v \n", err)
				if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == cloudwatchlogs.ErrCodeResourceNotFoundException {
					_, err = client.svc.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
						LogGroupName: logGroup,
					})
				}

			}
			return "", err
		},
		cloudwatchlogs.ErrCodeResourceAlreadyExistsException,
		fmt.Sprintf("E! CreateLogStream / CreateLogGroup with log group name %s stream name %s has errors.", *logGroup, *streamName))

	if e != nil {
		log.Printf("D! error != nil, return token: %s with error: %v \n", token, e)
		return token, e
	}

	//After a log stream is created the token is always empty.
	return "", nil
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