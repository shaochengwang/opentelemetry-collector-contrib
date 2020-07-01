package awspmdexporter

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/obsreport"
)

type pmdExporter struct {
	pushMetricsData func(ctx context.Context, md pdata.Metrics) (droppedTimeSeries int, err error)
}

// New returns test pmd
func New(
	config configmodels.Exporter,
	params component.ExporterCreateParams,
) (component.MetricsExporter, error) {
	if config == nil {
		return nil, errors.New("nil config")
	}

	logger := params.Logger
	awsConfig, session, err := GetAWSConfigSession(logger, &Conn{}, config.(*Config))
	if err != nil {
		return nil, err
	}
	client := NewCloudWatchClient(logger, awsConfig, session, config.(*Config))
	client.config = config

	return &pmdExporter{
		pushMetricsData: client.pushMetricsData,
	}, nil
}

func (emf *pmdExporter) ConsumeMetrics(ctx context.Context, md pdata.Metrics) error {
	exporterCtx := obsreport.ExporterContext(ctx, "emf.exporterFullName")
	fmt.Println("Consume Metrics from pmd Exporter.")
	_, err := emf.pushMetricsData(exporterCtx, md)
	return err
}

// Shutdown stops the exporter and is invoked during shutdown.
func (emf *pmdExporter) Shutdown(ctx context.Context) error {
	return nil
}

// Start
func (emf *pmdExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}