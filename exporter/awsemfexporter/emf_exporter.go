package awsemfexporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/translator"
	"go.opentelemetry.io/collector/consumer/pdatautil"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/obsreport"
)

const (
	defaultForceFlushInterval = 60 * time.Second
	defaultForceFlushIntervalInSeconds = 60
)

type emfExporter struct {

	//Each (log group, log stream) keeps a separate Pusher because of each (log group, log stream) requires separate stream token.
	groupStreamToPusherMap map[string]map[string]Pusher
	svcStructuredLog LogClient
	config configmodels.Exporter

	pusherMapLock sync.Mutex
	pusherWG         sync.WaitGroup
	ForceFlushInterval time.Duration // unit is second
	shutdownChan chan bool
	retryCnt int

}

// New func creates an EMF Exporter instance with data push callback func
func New(
	config configmodels.Exporter,
	params component.ExporterCreateParams,
) (component.MetricsExporter, error) {
	if config == nil {
		return nil, errors.New("emf exporter config is nil")
	}

	logger := params.Logger
	// create AWS session
	awsConfig, session, err := GetAWSConfigSession(logger, &Conn{}, config.(*Config))
	if err != nil {
		return nil, err
	}

	// create CWLogs client with aws session config
	svcStructuredLog := NewCloudWatchLogsClient(logger, awsConfig, session)

	emfExporter := &emfExporter{
		svcStructuredLog: svcStructuredLog,
		config: config,
		ForceFlushInterval: defaultForceFlushInterval,
		retryCnt: *awsConfig.MaxRetries,
	}
	if config.(*Config).ForceFlushInterval > 0 {
		emfExporter.ForceFlushInterval = time.Duration(config.(*Config).ForceFlushInterval) * time.Second
		fmt.Printf("D! Override ForceFlushInterval to %d nanoseconds, %d seconds\n", emfExporter.ForceFlushInterval, config.(*Config).ForceFlushInterval)
	}
	emfExporter.groupStreamToPusherMap = map[string]map[string]Pusher{}
	emfExporter.shutdownChan = make(chan bool)

	return emfExporter, nil
}

func (emf *emfExporter) pushMetricsData(_ context.Context, md pdata.Metrics) (droppedTimeSeries int, err error) {
	expConfig := emf.config.(*Config)
	logGroup := "otel-lg"
	logStream := "otel-stream"
	// override log group if found it in exp configuraiton
	if len(expConfig.LogGroupName) > 0 {
		logGroup = expConfig.LogGroupName
	}
	if len(expConfig.LogStreamName) > 0 {
		logStream = expConfig.LogStreamName
	}
	pusher := emf.getPusher(logGroup, logStream, nil, NonBlocking)
	if pusher != nil {
		putLogEvents := generateLogEventFromMetric(md)
		for _, ple := range putLogEvents {
			pusher.AddLogEntry(ple)
		}
	}
	return 0, nil
}

func (emf *emfExporter) getPusher(logGroup, logStream string, stateFolder *string, mode string) Pusher {
	emf.pusherMapLock.Lock()
	defer emf.pusherMapLock.Unlock()

	var ok bool
	var streamToPusherMap map[string]Pusher
	if streamToPusherMap, ok = emf.groupStreamToPusherMap[logGroup]; !ok {
		streamToPusherMap = map[string]Pusher{}
		emf.groupStreamToPusherMap[logGroup] = streamToPusherMap
	}

	var pusher Pusher
	if pusher, ok = streamToPusherMap[logStream]; !ok {
		pusher = NewPusher(
			aws.String(logGroup), aws.String(logStream), stateFolder,
			emf.ForceFlushInterval, emf.retryCnt, emf.svcStructuredLog, emf.shutdownChan, &emf.pusherWG, mode)
		streamToPusherMap[logStream] = pusher
	} else if pusher.GetMode() != mode {
		fmt.Printf("E! LogStream conflict between metric_collected and logs_collected, for logGroup %s, logStream %s and mode %s \n", logGroup, logStream, mode)
		return nil
	}

	return pusher
}

func (emf *emfExporter) ConsumeMetrics(ctx context.Context, md pdata.Metrics) error {
	exporterCtx := obsreport.ExporterContext(ctx, "emf.exporterFullName")

	_, err := emf.pushMetricsData(exporterCtx, md)
	return err
}

// Shutdown stops the exporter and is invoked during shutdown.
func (emf *emfExporter) Shutdown(ctx context.Context) error {
	return nil
}

// Start
func (emf *emfExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}


func generateLogEventFromMetric(metric pdata.Metrics) []*LogEvent {
	imd := pdatautil.MetricsToInternalMetrics(metric)
	rms := imd.ResourceMetrics()
	cwMetricLists := []*translator.CWMetrics{}
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if rm.IsNil() {
			continue
		}
		// translate OT metric datapoints into CWMetricLists
		cwm, err := translator.TranslateOtToCWMetric(&rm)
		if err != nil || cwm == nil {
			return nil
		}
		//fmt.Printf("I! the translated cwm is %v \n", cwm)
		// append all datapoint metrics in the request into CWMetric list
		for _, v := range cwm {
			cwMetricLists = append(cwMetricLists, v)
			//fmt.Printf("I! cloudwatch metric: %v \n", v)
		}
	}

	// convert CWMetric into map format for compatible with PLE input
	ples := make([]*LogEvent, 0, maximumLogEventsPerPut)
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
		metricCreationTime := met.Timestamp

		logEvent := NewLogEvent(
			metricCreationTime,
			string(pleMsg),
			"",
			0,
			Structured,
		)
		logEvent.multiLineStart = true
		logEvent.LogGeneratedTime = time.Unix(0, metricCreationTime * int64(time.Millisecond))
		ples = append(ples, logEvent)
	}
	return ples
}