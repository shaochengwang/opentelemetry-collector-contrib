package translator

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"sort"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/mapWithExpiry"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
)

const (
	CleanInteval = 5 * time.Minute
	MinTimeDiff  = 50 // We assume 50 micro-seconds is the minimal gap between two collected data sample to be valid to calculate delta
)

type EmfTranslator struct {
	currentState *mapWithExpiry.MapWithExpiry
}

type rateState struct {
	value     interface{}
	timestamp int64
}

// CWMetrics defines
type CWMetrics struct {
	Measurements []CwMeasurement
	Timestamp    int64
	Fields       map[string]interface{}
}

// CwMeasurement defines
type CwMeasurement struct {
	Namespace  string
	Dimensions [][]string
	Metrics    []map[string]string
}

// TranslateOtToCWMetric converts OT metrics to CloudWatch Metric format
func (t *EmfTranslator) TranslateOtToCWMetric(rm *pdata.ResourceMetrics, config *configmodels.Exporter) ([]*CWMetrics, error) {
	cwMetricLists := []*CWMetrics{}
	var namespace string

	if !rm.Resource().IsNil() {
		// TODO: handle resource data
	}
	ilms := rm.InstrumentationLibraryMetrics()
	for j := 0; j < ilms.Len(); j++ {
		ilm := ilms.At(j)
		if ilm.IsNil() {
			continue
		}
		if ilm.InstrumentationLibrary().IsNil() {
			continue
		}
		namespace = ilm.InstrumentationLibrary().Name()
		metrics := ilm.Metrics()
		for k := 0; k < metrics.Len(); k++ {
			metric := metrics.At(k)
			if metric.IsNil() {
				continue
			}
			cwMetricList, err := t.getMeasurements(&metric, namespace)
			if err != nil {
				continue
			}
			for _, v := range cwMetricList {
				cwMetricLists = append(cwMetricLists, v)
			}
		}
	}

	return cwMetricLists, nil
}

func (t *EmfTranslator) getMeasurements(metric *pdata.Metric, namespace string) ([]*CWMetrics, error) {
	// only support counter data points for EMF now
	if metric.Int64DataPoints().Len() == 0 && metric.DoubleDataPoints().Len() == 0 {
		return nil, nil
	}

	mDesc := metric.MetricDescriptor()
	if mDesc.IsNil() {
		return nil, nil
	}

	result := []*CWMetrics{}
	// metric measure data from OT
	metricMeasure := make(map[string]string)
	// meture measure slice could include multiple metric measures
	metricSlice := []map[string]string{}
	metricMeasure["Name"] = mDesc.Name()
	metricMeasure["Unit"] = mDesc.Unit()
	metricSlice = append(metricSlice, metricMeasure)

	// TODO: saparate Int64 and Double Datapoint
	// get all int64 datapoints
	idp := metric.Int64DataPoints()
	for m := 0; m < idp.Len(); m++ {
		dp := idp.At(m)
		if dp.IsNil() {
			continue
		}

		// fields contains metric and dimensions key/value pairs
		fieldsPairs := make(map[string]interface{})
		// Dimensions Slice
		dimensionSlice := []string{}
		dimensionKV := dp.LabelsMap()
		dimensionKV.ForEach(func(k string, v pdata.StringValue) {
			fieldsPairs[k] = v.Value()
			dimensionSlice = append(dimensionSlice, k)
		})
		fieldsPairs[mDesc.Name()] = dp.Value()
		timestamp := time.Now().UnixNano() / int64(time.Millisecond)
		metricVal := t.calculateRate(fieldsPairs, dp.Value(), timestamp)
		if metricVal == nil {
			return result, nil
		}
		fieldsPairs[mDesc.Name()] = metricVal
		fmt.Println(fmt.Sprintf("%s%d", "MetricValSent=================", metricVal))
		// timestamp := dp.StartTime() / 1e6
		// EMF dimension attr takes list of list on dimensions
		dimensionArray := [][]string{}
		dimensionArray = append(dimensionArray, dimensionSlice)
		cwme := &CwMeasurement{
			Namespace:  namespace,
			Dimensions: dimensionArray,
			Metrics:    metricSlice,
		}
		metricList := make([]CwMeasurement, 1)
		metricList[0] = *cwme
		cwm := &CWMetrics{
			Measurements: metricList,
			Timestamp:    timestamp,
			Fields:       fieldsPairs,
		}
		result = append(result, cwm)
	}

	return result, nil
}

// rate is calculated by valDelta / timeDelta
func (t *EmfTranslator) calculateRate(fields map[string]interface{}, val int64, timestamp int64) interface{} {
	keys := make([]string, 0, len(fields))
	var b bytes.Buffer
	var metricRate float64
	// hash the key of str: metric + dimension key/value pairs (sorted alpha)
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		switch v := fields[k].(type) {
		case int64:
			b.WriteString(k)
			continue
		case string:
			b.WriteString(k)
			b.WriteString(v)
		default:
			continue
		}
	}
	h := sha1.New()
	h.Write(b.Bytes())
	bs := h.Sum(nil)
	hashStr := string(bs)

	// get previous Metric content from map
	if state, ok := t.currentState.Get(hashStr); ok {
		prevStats := state.(*rateState)
		deltaTime := timestamp - prevStats.timestamp
		deltaVal := val - prevStats.value.(int64)
		fmt.Println(fmt.Sprintf("metric delta=================%d", deltaVal))
		if deltaTime > MinTimeDiff && deltaVal >= 0 {
			metricRate = float64(deltaVal*1e3) / float64(deltaTime)
		}
	}
	content := &rateState{
		value:     val,
		timestamp: timestamp,
	}
	t.currentState.Set(hashStr, content)

	return metricRate
}

// NewEmfTranslator define EMFTranslator with
func NewEmfTranslator() *EmfTranslator {
	return &EmfTranslator{
		currentState: mapWithExpiry.NewMapWithExpiry(CleanInteval),
	}
}
