package handler

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awspmdexporter/util"
	"github.com/stretchr/testify/assert"
)

func TestAddUserAgentCWAgent(t *testing.T) {
	expectedUserAgent := fmt.Sprintf("%s/%s (%s; %s; %s)",
		CWAgent, util.Version(),
		runtime.Version(), runtime.GOOS, runtime.GOARCH)
	log.Printf("I! Expected user agent as %s", expectedUserAgent)
	httpReq, _ := http.NewRequest("POST", "", nil)
	r := &request.Request{
		HTTPRequest: httpReq,
		Body:        nil,
	}
	r.SetBufferBody([]byte{})

	AddUserAgentCWAgent(r)

	userAgent := r.HTTPRequest.Header.Get("User-Agent")
	log.Printf("I! Got user agent as %s", userAgent)
	assert.Equal(t, expectedUserAgent, userAgent)
}
