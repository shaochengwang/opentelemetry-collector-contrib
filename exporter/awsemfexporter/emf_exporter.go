package awsemfexporter

import (
	"context"
	"errors"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/translator"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/obsreport"
)

type emfExporter struct {
	pushMetricsData func(ctx context.Context, md pdata.Metrics) (droppedTimeSeries int, err error)
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
	client := NewCloudWatchLogsClient(logger, awsConfig, session)
	// create emf translator for OT metric to CW EMF
	mTranslator := translator.NewEmfTranslator()
	cwlClient := &cwlClient{
		logger:     logger,
		client:     client,
		config:     config,
		translator: mTranslator,
	}

	return &emfExporter{
		pushMetricsData: cwlClient.pushMetricsData,
	}, nil
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
