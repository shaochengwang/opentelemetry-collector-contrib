package handler

import (
	"fmt"
	"runtime"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awspmdexporter/util"
)

const (
	CWAgent = "CWAgent"
)

var UserAgentRequestHandler = request.NamedHandler{
	Name: "UserAgentRequestHandler", Fn: AddUserAgentCWAgent,
}

func AddUserAgentCWAgent(req *request.Request) {
	userAgent := fmt.Sprintf("%s/%s (%s; %s; %s)", CWAgent, util.Version(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
	req.HTTPRequest.Header.Set("User-Agent", userAgent)
}

