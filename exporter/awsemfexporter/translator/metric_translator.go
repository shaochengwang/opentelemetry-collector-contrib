package translator

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"sort"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/mapWithExpiry"
	"go.opentelemetry.io/collector/consumer/pdata"
)

const (
	CleanInteval = 5 * time.Minute
	MinTimeDiff  = 50 // We assume 50 micro-seconds is the minimal gap between two collected data sample to be valid to calculate delta
)

type EmfTranslator struct {
	currentState *mapWithExpiry.MapWithExpiry
}
var currentState = mapWithExpiry.NewMapWithExpiry(CleanInteval)


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

// CWMetric stats defines
type CWMetricStats struct {
	Max 		 float64
	Min 		 float64
	Count    	 uint64
	Sum			 float64
}

// TranslateOtToCWMetric converts OT metrics to CloudWatch Metric format
func TranslateOtToCWMetric(rm *pdata.ResourceMetrics) ([]*CWMetrics, error) {
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
			cwMetricList, err := getMeasurements(&metric, namespace)
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

func getMeasurements(metric *pdata.Metric, namespace string) ([]*CWMetrics, error) {
	// only support counter data points for EMF now
	if metric.Int64DataPoints().Len() == 0 && metric.DoubleDataPoints().Len() == 0 && metric.SummaryDataPoints().Len() == 0 {
		return nil, nil
	}

	mDesc := metric.MetricDescriptor()
	if mDesc.IsNil() {
		return nil, nil
	}

	var result []*CWMetrics
	// metric measure data from OT
	metricMeasure := make(map[string]string)
	// meture measure slice could include multiple metric measures
	metricSlice := []map[string]string{}
	metricMeasure["Name"] = mDesc.Name()
	//TODO: Need to add unit conversion from OT to CW
	metricMeasure["Unit"] = mDesc.Unit()
	metricSlice = append(metricSlice, metricMeasure)

	// get all double datapoints
	ddp := metric.DoubleDataPoints()
	for m := 0; m < ddp.Len(); m++ {
		dp := ddp.At(m)
		if dp.IsNil() {
			continue
		}
		cwMetric := buildCWMetricFromDDP(dp, mDesc, namespace, metricSlice)
		if cwMetric != nil {
			result = append(result, cwMetric)
		}
	}
	// get all int64 datapoints
	idp := metric.Int64DataPoints()
	for m := 0; m < idp.Len(); m++ {
		dp := idp.At(m)
		if dp.IsNil() {
			continue
		}
		cwMetric := buildCWMetricFromIDP(dp, mDesc, namespace, metricSlice)
		if cwMetric != nil {
			result = append(result, cwMetric)
		}
	}
	// get all summary datapoints
	sdp := metric.SummaryDataPoints()
	if sdp.Len() > 0 {
		fmt.Printf("*********SummaryDataPoints has %d\n", sdp.Len())
	}
	for m := 0; m < sdp.Len(); m++ {
		dp := sdp.At(m)
		if dp.IsNil() {
			continue
		}
		cwMetric := buildCWMetricFromSDP(dp, mDesc, namespace, metricSlice)
		if cwMetric != nil {
			result = append(result, cwMetric)
		}
	}
	return result, nil
}

func buildCWMetricFromDDP(metric pdata.DoubleDataPoint, mDesc pdata.MetricDescriptor, namespace string, metricSlice []map[string]string) *CWMetrics {

	// fields contains metric and dimensions key/value pairs
	fieldsPairs := make(map[string]interface{})
	// Dimensions Slice
	var dimensionSlice []string
	dimensionKV := metric.LabelsMap()
	dimensionKV.ForEach(func(k string, v pdata.StringValue) {
		fieldsPairs[k] = v.Value()
		dimensionSlice = append(dimensionSlice, k)
	})
	fieldsPairs[mDesc.Name()] = metric.Value()
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	metricVal := calculateRate(fieldsPairs, metric.Value(), timestamp)
	if metricVal == nil {
		return nil
	}
	fieldsPairs[mDesc.Name()] = metricVal
	fmt.Println(fmt.Sprintf("%s%v", "MetricValSent=================", metricVal))
	// timestamp := dp.StartTime() / 1e6

	// EMF dimension attr takes list of list on dimensions TODO: add single/zero dimension rollup here
	var dimensionArray [][]string
	dimensionArray = append(dimensionArray, dimensionSlice)
	cwMeasurement := &CwMeasurement{
		Namespace:  namespace,
		Dimensions: dimensionArray,
		Metrics:    metricSlice,
	}
	metricList := make([]CwMeasurement, 1)
	metricList[0] = *cwMeasurement
	cwMetric := &CWMetrics{
		Measurements: metricList,
		Timestamp:    timestamp,
		Fields:       fieldsPairs,
	}
	return cwMetric
}


func buildCWMetricFromIDP(metric pdata.Int64DataPoint, mDesc pdata.MetricDescriptor, namespace string, metricSlice []map[string]string) *CWMetrics {

	// fields contains metric and dimensions key/value pairs
	fieldsPairs := make(map[string]interface{})
	// Dimensions Slice
	var dimensionSlice []string
	dimensionKV := metric.LabelsMap()
	dimensionKV.ForEach(func(k string, v pdata.StringValue) {
		fieldsPairs[k] = v.Value()
		dimensionSlice = append(dimensionSlice, k)
	})
	fieldsPairs[mDesc.Name()] = metric.Value()
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	metricVal := calculateRate(fieldsPairs, metric.Value(), timestamp)
	if metricVal == nil {
		return nil
	}
	fieldsPairs[mDesc.Name()] = metricVal
	fmt.Println(fmt.Sprintf("%s%v", "MetricValSent=================", metricVal))
	// timestamp := dp.StartTime() / 1e6

	// EMF dimension attr takes list of list on dimensions TODO: add single/zero dimension rollup here
	var dimensionArray [][]string
	dimensionArray = append(dimensionArray, dimensionSlice)
	cwMeasurement := &CwMeasurement{
		Namespace:  namespace,
		Dimensions: dimensionArray,
		Metrics:    metricSlice,
	}
	metricList := make([]CwMeasurement, 1)
	metricList[0] = *cwMeasurement
	cwMetric := &CWMetrics{
		Measurements: metricList,
		Timestamp:    timestamp,
		Fields:       fieldsPairs,
	}
	return cwMetric
}

func buildCWMetricFromSDP(metric pdata.SummaryDataPoint, mDesc pdata.MetricDescriptor, namespace string, metricSlice []map[string]string) *CWMetrics {

	// fields contains metric and dimensions key/value pairs
	fieldsPairs := make(map[string]interface{})
	// Dimensions Slice
	var dimensionSlice []string
	dimensionKV := metric.LabelsMap()
	dimensionKV.ForEach(func(k string, v pdata.StringValue) {
		fieldsPairs[k] = v.Value()
		dimensionSlice = append(dimensionSlice, k)
	})
	summaryValueAtPercentileSlice := metric.ValueAtPercentiles()
	metricStats := &CWMetricStats{
		Min: summaryValueAtPercentileSlice.At(0).Value(),
		Max: summaryValueAtPercentileSlice.At(summaryValueAtPercentileSlice.Len() - 1).Value(),
		Count: metric.Count(),
		Sum: metric.Sum(),
	}
	fieldsPairs[mDesc.Name()] = metricStats
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	//metricVal := calculateRate(fieldsPairs, metric.Value(), timestamp)
	//if metricVal == nil {
	//	return nil
	//}
	//fieldsPairs[mDesc.Name()] = metricVal
	//fmt.Println(fmt.Sprintf("%s%v", "MetricValSent=================", metricVal))
	// timestamp := dp.StartTime() / 1e6

	// EMF dimension attr takes list of list on dimensions TODO: add single/zero dimension rollup here
	var dimensionArray [][]string
	dimensionArray = append(dimensionArray, dimensionSlice)
	cwMeasurement := &CwMeasurement{
		Namespace:  namespace,
		Dimensions: dimensionArray,
		Metrics:    metricSlice,
	}
	metricList := make([]CwMeasurement, 1)
	metricList[0] = *cwMeasurement
	cwMetric := &CWMetrics{
		Measurements: metricList,
		Timestamp:    timestamp,
		Fields:       fieldsPairs,
	}
	return cwMetric
}

// rate is calculated by valDelta / timeDelta
func calculateRate(fields map[string]interface{}, val interface{}, timestamp int64) interface{} {
	keys := make([]string, 0, len(fields))
	var b bytes.Buffer
	var metricRate interface{}
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
	if state, ok := currentState.Get(hashStr); ok {
		prevStats := state.(*rateState)
		deltaTime := timestamp - prevStats.timestamp
		var deltaVal interface{}
		if _, ok := val.(float64); ok {
			deltaVal = val.(float64) - prevStats.value.(float64)
			if deltaTime > MinTimeDiff && deltaVal.(float64) >= 0 {
				metricRate = deltaVal.(float64)*1e3 / float64(deltaTime)
			}
		} else {
			deltaVal = val.(int64) - prevStats.value.(int64)
			if deltaTime > MinTimeDiff && deltaVal.(int64) >= 0 {
				metricRate = deltaVal.(int64)*1e3 / deltaTime
			}
		}
	}
	content := &rateState{
		value:     val,
		timestamp: timestamp,
	}
	currentState.Set(hashStr, content)
	return metricRate
}

// NewEmfTranslator define EMFTranslator with
func NewEmfTranslator() *EmfTranslator {
	return &EmfTranslator{
		currentState: mapWithExpiry.NewMapWithExpiry(CleanInteval),
	}
}