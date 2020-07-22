module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter

go 1.14

require (
	github.com/aws/aws-sdk-go v1.33.4
	github.com/census-instrumentation/opencensus-proto v0.2.1
	github.com/docker/docker v1.13.1
	github.com/golang/protobuf v1.3.5
	github.com/mattn/go-isatty v0.0.10 // indirect
	github.com/stretchr/testify v1.5.1
	go.opentelemetry.io/collector v0.4.1-0.20200629224201-e7a7690e21fc
	go.uber.org/zap v1.13.0
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
)
