package awsemfexporter

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

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
