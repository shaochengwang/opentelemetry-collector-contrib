package awsemfexporter

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)


//func TestCloudWatchLogs_generateLogEventFromMetric(t *testing.T) {
//	unixSecs := int64(1574092046)
//	unixNSecs := int64(11 * time.Millisecond)
//	tsUnix := time.Unix(unixSecs, unixNSecs)
//
//	doubleVal := 1234.5678
//	m := metricstestutil.Double(tsUnix, doubleVal)
//
//	structuredlogscommon.AppendAttributesInFields("attribute", 0, m)
//	logEvent := generateLogEventFromMetricPath(m)
//	expectedMessage := `{"attribute":0,"log_group_name":"SomeLogGroup","log_stream_name":"SomeLogStream","tag1":"value1","value1":"TestLogMessage"}`
//	assert.Equal(t, expectedMessage, *logEvent.InputLogEvent.Message)
//	assert.Equal(t, true, logEvent.multiLineStart)
//
//	m.AddField(logscommon.LogEntryField, "TestLogMessage")
//	logEvent = generateLogEventFromMetricPath(m)
//	expectedMessage = "TestLogMessage"
//	assert.Equal(t, expectedMessage, *logEvent.InputLogEvent.Message)
//	assert.Equal(t, true, logEvent.multiLineStart)
//}
//
func TestCloudWatchLogs_getPusher(t *testing.T) {
	logs := &emfExporter{
		groupStreamToPusherMap: map[string]map[string]Pusher{},
		ForceFlushInterval:     time.Second,
	}
	pusher := logs.getPusher("test_log_group", "test_log_stream", nil, Blocking)
	assert.NotEqual(t, nil, pusher)
	pusher = logs.getPusher("test_log_group", "test_log_stream", nil, NonBlocking)
	assert.Equal(t, nil, pusher)
}
