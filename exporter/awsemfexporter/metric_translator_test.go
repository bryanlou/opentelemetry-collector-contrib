// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package awsemfexporter

import (
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"
	"testing"
	"time"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumerdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/translator/conventions"
	"go.opentelemetry.io/collector/translator/internaldata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func readFromFile(filename string) string {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	str := string(data)
	return str
}

func createMetricTestData() consumerdata.MetricsData {
	return consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceName:      "myServiceName",
				conventions.AttributeServiceNamespace: "myServiceNS",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_DOUBLE,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanTimer",
					Description: "How long the spans take",
					Unit:        "Seconds",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Sum:   15.0,
										Count: 5,
										BucketOptions: &metricspb.DistributionValue_BucketOptions{
											Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{0, 10},
												},
											},
										},
										Buckets: []*metricspb.DistributionValue_Bucket{
											{
												Count: 0,
											},
											{
												Count: 4,
											},
											{
												Count: 1,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// Asserts whether dimension sets are equal (i.e. has same sets of dimensions)
func assertDimsEqual(t *testing.T, expected, actual [][]string) {
	// Convert to string for easier sorting
	expectedStringified := make([]string, len(expected))
	actualStringified := make([]string, len(actual))
	for i, v := range expected {
		sort.Strings(v)
		expectedStringified[i] = strings.Join(v, ",")
	}
	for i, v := range actual {
		sort.Strings(v)
		actualStringified[i] = strings.Join(v, ",")
	}
	// Sort across dimension sets for equality checking
	sort.Strings(expectedStringified)
	sort.Strings(actualStringified)
	assert.Equal(t, expectedStringified, actualStringified)
}

// Asserts whether CW Measurements are equal.
func assertCWMeasurementEqual(t *testing.T, expected, actual CWMeasurement) {
	assert.Equal(t, expected.Namespace, actual.Namespace)
	assert.Equal(t, expected.Metrics, actual.Metrics)
	assertDimsEqual(t, expected.Dimensions, actual.Dimensions)
}

func TestGetNamespace(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
	md := createMetricTestData()
	rms := internaldata.OCToMetrics(md)
	rm := rms.ResourceMetrics().At(0)
	namespace := getNamespace(&rm, config.Namespace)
	assert.Equal(t, "myServiceNS/myServiceName", namespace)
}

// Asserts whether GroupedMetric Labels and Metrics are equal
func TestTranslateOtToGroupedMetric(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
	groupedMetricMap := make(map[string]*GroupedMetric)
	md := createMetricTestData()
	rm := internaldata.OCToMetrics(md)
	totalDroppedMetrics := 0
	groupedMetricMap, totalDroppedMetrics = TranslateOtToGroupedMetric(rm, config)

	assert.Equal(t, 0, totalDroppedMetrics)
	assert.NotNil(t, groupedMetricMap)

	for k, v := range groupedMetricMap{
		if strings.Contains(k, "isItAnError") {
			assert.Equal(t, 2, len(v.Labels))
			assert.Equal(t, 4, len(v.Metrics))
			assert.Equal(t, map[string]string{"isItAnError":"false", "spanName":"testSpan"}, v.Labels)
		} else {
			assert.Equal(t, 1, len(v.Labels))
			assert.Equal(t, 1, len(v.Metrics))
			assert.Equal(t, map[string]string{"spanName":"testSpan"}, v.Labels)
		}
	}
}

// Asserts whether GroupedMetric Label contains InstrLibName
func TestTranslateOtToGroupedMetricWithInstrLibrary(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
		logger:                zap.NewNop(),
	}
	groupedMetricMap := make(map[string]*GroupedMetric)
	md := createMetricTestData()
	rms := internaldata.OCToMetrics(md)
	rm := rms.ResourceMetrics().At(0)
	ilms := rm.InstrumentationLibraryMetrics()
	ilm := ilms.At(0)
	ilm.InstrumentationLibrary().InitEmpty()
	ilm.InstrumentationLibrary().SetName("cloudwatch-lib")
	groupedMetricMap, totalDroppedMetrics := TranslateOtToGroupedMetric(rms, config)
	assert.Equal(t, 0, totalDroppedMetrics)
	for k,v := range groupedMetricMap{
		if strings.Contains(k, "isItAnError") {
			assert.Equal(t, "cloudwatch-lib", v.Labels[OTellibDimensionKey])
			assert.Equal(t, map[string]string{"OTelLib":"cloudwatch-lib", "isItAnError":"false", "spanName":"testSpan"}, v.Labels)
		} else {
			assert.Equal(t, "cloudwatch-lib", v.Labels[OTellibDimensionKey])
			assert.Equal(t, map[string]string{"OTelLib":"cloudwatch-lib", "spanName":"testSpan"}, v.Labels)
		}
	}
}

// Asserts whether GroupedMetric Namespace equals defined NS value
func TestTranslateOtToGroupedMetricWithNameSpace(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}

	md := consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceNamespace: "myServiceNS",
				conventions.AttributeServiceName: "myServiceName",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
		},
	}
	rm := internaldata.OCToMetrics(md)
	groupedMetricMap := make(map[string]*GroupedMetric)
	groupedMetricMap, totalDroppedMetrics := TranslateOtToGroupedMetric(rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)

	for _, v := range groupedMetricMap{
		assert.Equal(t, "myServiceNS/myServiceName", v.Namespace)
	}
}

// Asserts whether GroupedMetric Namespace equals default if undefined 
func TestTranslateOtToGroupedMetricNoNameSpace(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
	md := consumerdata.MetricsData{
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
		},
	}
	rm := internaldata.OCToMetrics(md)
	groupedMetricMap := make(map[string]*GroupedMetric)
	groupedMetricMap, totalDroppedMetrics := TranslateOtToGroupedMetric(rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)

	for _, v := range groupedMetricMap{
		assert.Equal(t, "default", v.Namespace)
	}
}

func TestGetGroupedMetrics(t *testing.T) {
	namespace := "nginxFoo"
	instrumentationLibName := "InstrLibName"
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: "",
	}
	
	metricResults := []MetricInfo {
		MetricInfo {
			Value: int64(1),
			Unit: "Count",
		},
		MetricInfo {
			Value: 0.1,
			Unit: "Count",
		},
		MetricInfo {
			Value: 0,
			Unit: "Count",
		},
		MetricInfo{
			Value: &CWMetricStats{
					Min:   0,
					Max:   10,
					Count: 18,
					Sum:   35.0,
				},
			Unit: "Seconds",
		},
	}

	testCases := []struct {
		testName string
		metric   *metricspb.Metric
		expected map[string]*MetricInfo
	}{
		{
			"Int gauge",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_GAUGE_INT64,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			map[string]*MetricInfo {
				"foo": &metricResults[0],
			},
		},
		{
			"Double gauge",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_GAUGE_DOUBLE,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			map[string]*MetricInfo {
				"foo": &metricResults[1],
			},
		},
		{
			"Int sum",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_INT64,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			map[string]*MetricInfo {
				"foo": &metricResults[2],
			},
		},
		{
			"Double sum",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			map[string]*MetricInfo {
				"foo": &metricResults[2],
			},
		},
		{
			"Double histogram",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
					Unit: "Seconds",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Sum:   35.0,
										Count: 18,
										BucketOptions: &metricspb.DistributionValue_BucketOptions{
											Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{0, 10},
												},
											},
										},
										Buckets: []*metricspb.DistributionValue_Bucket{
											{
												Count: 5,
											},
											{
												Count: 6,
											},
											{
												Count: 7,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			map[string]*MetricInfo {
				"foo": &metricResults[3],
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			groupedMetricMap := make(map[string]*GroupedMetric)
			oc := consumerdata.MetricsData{
				Node: &commonpb.Node{},
				Resource: &resourcepb.Resource{
					Labels: map[string]string{
						conventions.AttributeServiceName:      "myServiceName",
						conventions.AttributeServiceNamespace: "myServiceNS",
					},
				},
				Metrics: []*metricspb.Metric{tc.metric},
			}

			// Retrieve *pdata.Metric
			rm := internaldata.OCToMetrics(oc)
			rms := rm.ResourceMetrics()
			assert.Equal(t, 1, rms.Len())
			ilms := rms.At(0).InstrumentationLibraryMetrics()
			assert.Equal(t, 1, ilms.Len())
			metrics := ilms.At(0).Metrics()
			assert.Equal(t, 1, metrics.Len())
			metric := metrics.At(0)

			getGroupedMetrics(&metric, namespace, instrumentationLibName, timestamp, groupedMetricMap, config)

			for _, v := range groupedMetricMap{
				assert.Equal(t, namespace, v.Namespace)
				assert.Equal(t, timestamp, v.Timestamp)
				assert.Equal(t, len(tc.expected), len(v.Metrics))
				assert.Equal(t, tc.expected, v.Metrics)
				assert.Equal(t, 2, len(v.Labels))
				assert.Equal(t, map[string]string{"OTelLib":"InstrLibName", "label1":"value1",}, v.Labels)
			}
		})
	}

	t.Run("Unhandled metric type", func(t *testing.T) {
		namespace := "nginx"
		instrumentationLibName := "InstrLibName"
		groupedMetricMap := make(map[string]*GroupedMetric)
		md := pdata.NewMetrics()
		rms := md.ResourceMetrics()
		rms.Resize(1)
		rms.At(0).InstrumentationLibraryMetrics().Resize(1)
		rms.At(0).InstrumentationLibraryMetrics().At(0).Metrics().Resize(1)
		metric := rms.At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0)
		metric.InitEmpty()
		metric.SetName("foo")
		metric.SetUnit("Count")
		metric.SetDataType(pdata.MetricDataTypeIntHistogram)

		obs, logs := observer.New(zap.WarnLevel)
		obsConfig := &Config{
			DimensionRollupOption: "",
			logger:                zap.New(obs),
		}
		getGroupedMetrics(&metric, namespace, instrumentationLibName, timestamp, groupedMetricMap, obsConfig)
		assert.Equal(t, 0, len(groupedMetricMap))

		// Test output warning logs
		expectedLogs := []observer.LoggedEntry{
			{
				Entry: zapcore.Entry{Level: zap.WarnLevel, Message: "Unhandled metric data type."},
				Context: []zapcore.Field{
					zap.String("DataType", "IntHistogram"),
					zap.String("Name", "foo"),
					zap.String("Unit", "Count"),
				},
			},
		}
		assert.Equal(t, 1, logs.Len())
		assert.Equal(t, expectedLogs, logs.AllUntimed())
	})

	t.Run("Nil metric", func(t *testing.T) {
		groupedMetricMap := make(map[string]*GroupedMetric)
		groupedMetrics := getGroupedMetrics(nil, namespace, instrumentationLibName, timestamp, groupedMetricMap, config)
		assert.Nil(t, groupedMetrics)
	})
}

func TestBuildGroupedMetric(t *testing.T) {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	instrLibName := "fooLibName"
	OTellibDimensionKey := "fooOTelLib"
	namespace := "fooNamespace"
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("foo")
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}

	t.Run("Int gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(int64(-17))

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetricMap := make(map[string]*GroupedMetric)
		key := getGroupedMetricKey(namespace, timestamp, labels)
		groupedMetric := buildGroupedMetric(dp, namespace, timestamp, labels, &metric, key, groupedMetricMap, config)
		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		metricInfo := MetricInfo {
			Value: int64(-17),
			Unit: "",
		}
		expectedMetrics := map[string]*MetricInfo{
			"foo": &metricInfo,
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"fooOTelLib": "fooLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Double gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleGauge)
		dp := pdata.NewDoubleDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(0.3)

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetricMap := make(map[string]*GroupedMetric)
		key := getGroupedMetricKey(namespace, timestamp, labels)
		groupedMetric := buildGroupedMetric(dp, namespace, timestamp, labels, &metric, key, groupedMetricMap, config)

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		metricInfo := MetricInfo {
			Value: float64(0.3),
			Unit: "",
		}
		expectedMetrics := map[string]*MetricInfo{
			"foo": &metricInfo,
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"fooOTelLib": "fooLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Int sum", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntSum)
		metric.IntSum().InitEmpty()
		metric.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp := pdata.NewIntDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(int64(-9))

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetricMap := make(map[string]*GroupedMetric)
		key := getGroupedMetricKey(namespace, timestamp, labels)
		groupedMetric := buildGroupedMetric(dp, namespace, timestamp, labels, &metric, key, groupedMetricMap, config)

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		metricInfo := MetricInfo {
			Value: int(0),
			Unit: "",
		}
		expectedMetrics := map[string]*MetricInfo{
			"foo": &metricInfo,
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"fooOTelLib": "fooLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Double sum", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleSum)
		metric.DoubleSum().InitEmpty()
		metric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp := pdata.NewDoubleDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(0.3)

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetricMap := make(map[string]*GroupedMetric)
		key := getGroupedMetricKey(namespace, timestamp, labels)
		groupedMetric := buildGroupedMetric(dp, namespace, timestamp, labels, &metric, key, groupedMetricMap, config)

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		metricInfo := MetricInfo {
			Value: int(0),
			Unit: "",
		}
		expectedMetrics := map[string]*MetricInfo{
			"foo": &metricInfo,
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"fooOTelLib": "fooLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Double histogram", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleHistogram)
		dp := pdata.NewDoubleHistogramDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetCount(uint64(17))
		dp.SetSum(float64(17.13))
		dp.SetBucketCounts([]uint64{1, 2, 3})
		dp.SetExplicitBounds([]float64{1, 2, 3})

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetricMap := make(map[string]*GroupedMetric)
		key := getGroupedMetricKey(namespace, timestamp, labels)
		groupedMetric := buildGroupedMetric(dp, namespace, timestamp, labels, &metric, key, groupedMetricMap, config)

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		metricInfo := MetricInfo {
			Value: &CWMetricStats{
				Min:   1,
				Max:   3,
				Sum:   17.13,
				Count: 17,
			},
			Unit: "",
		}
		expectedMetrics := map[string]*MetricInfo{
			"foo": &metricInfo,
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"fooOTelLib": "fooLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})	

	t.Run("Invalid datapoint type", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntHistogramDataPoint()
		dp.InitEmpty()

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetricMap := make(map[string]*GroupedMetric)
		key := getGroupedMetricKey(namespace, timestamp, labels)
		groupedMetric := buildGroupedMetric(dp, namespace, timestamp, labels, &metric, key, groupedMetricMap, config)
		assert.Nil(t, groupedMetric)
	})
}

func TestUpdateGroupedMetric(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}

	md := consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceNamespace: "myServiceNS",
				conventions.AttributeServiceName: "myServiceName",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "foo1",
					Description: "test",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 6,
								},
							},
						},
					},
				},
			},
		},
	}
	rm := internaldata.OCToMetrics(md)
	groupedMetricMap := make(map[string]*GroupedMetric)
	groupedMetricMap, _ = TranslateOtToGroupedMetric(rm, config)

	var key string
	for k, _ := range groupedMetricMap {
		key = k
	}

	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	instrLibName := "fooLibName"
	OTellibDimensionKey := "fooOTelLib"
	namespace := "fooNamespace"

	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("foo2")
	metric.SetDataType(pdata.MetricDataTypeIntGauge)
	dp := pdata.NewIntDataPoint()
	dp.InitEmpty()
	dp.SetValue(int64(6))

	labels := map[string]string{
		OTellibDimensionKey: instrLibName,
		"spanName": "testSpan",
		"isItAnError": "true",
	}

	buildGroupedMetric(dp, namespace, timestamp, labels, &metric, key, groupedMetricMap, config)

	assert.NotNil(t, groupedMetricMap)
	assert.Equal(t, 1, len(groupedMetricMap))
	assert.Equal(t, 2, len(groupedMetricMap[key].Metrics))

	metricInfo := MetricInfo {
		Value: int64(6),
		Unit: "",
	}
	assert.Equal(t,&metricInfo, groupedMetricMap[key].Metrics["foo2"])
}

func TestCalculateMetricValue(t *testing.T) {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	instrLibName := "fooLibName"
	OTellibDimensionKey := "fooOTelLib"
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("foo")

	t.Run("Int gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(int64(-17))

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		metricVal := calculateMetricValue(dp, timestamp, labels, &metric) 
		assert.NotNil(t, metricVal)
		assert.Equal(t, int64(-17), metricVal)
	})

	t.Run("Double gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleGauge)
		dp := pdata.NewDoubleDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(0.6)

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		metricVal := calculateMetricValue(dp, timestamp, labels, &metric) 
		assert.NotNil(t, metricVal)
		assert.Equal(t, 0.6, metricVal)
	})

	t.Run("Invalid datapoint type", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntHistogramDataPoint()
		dp.InitEmpty()

		labels := map[string]string{
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		metricVal := calculateMetricValue(dp, timestamp, labels, &metric) 
		assert.Nil(t, metricVal)
		assert.Equal(t, nil, metricVal)
	})
}

func TestTranslateOtToCWMetricWithInstrLibrary(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
		logger:                zap.NewNop(),
	}
	md := createMetricTestData()
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	ilms := rm.InstrumentationLibraryMetrics()
	ilm := ilms.At(0)
	ilm.InstrumentationLibrary().InitEmpty()
	ilm.InstrumentationLibrary().SetName("cloudwatch-lib")
	cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)
	assert.NotNil(t, cwm)
	assert.Equal(t, 5, len(cwm))
	assert.Equal(t, 1, len(cwm[0].Measurements))

	met := cwm[0]

	assert.Equal(t, met.Fields["spanCounter"], 0)

	expectedMeasurement := CWMeasurement{
		Namespace: "myServiceNS/myServiceName",
		Dimensions: [][]string{
			{OTellibDimensionKey, "isItAnError", "spanName"},
			{OTellibDimensionKey},
			{OTellibDimensionKey, "spanName"},
			{OTellibDimensionKey, "isItAnError"},
		},
		Metrics: []map[string]string{
			{
				"Name": "spanCounter",
				"Unit": "Count",
			},
		},
	}
	assertCWMeasurementEqual(t, expectedMeasurement, met.Measurements[0])
}

func TestTranslateOtToCWMetricWithoutInstrLibrary(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
		logger:                zap.NewNop(),
	}
	md := createMetricTestData()
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)
	assert.NotNil(t, cwm)
	assert.Equal(t, 5, len(cwm))
	assert.Equal(t, 1, len(cwm[0].Measurements))

	met := cwm[0]
	assert.NotContains(t, met.Fields, OTellibDimensionKey)
	assert.Equal(t, met.Fields["spanCounter"], 0)

	expectedMeasurement := CWMeasurement{
		Namespace: "myServiceNS/myServiceName",
		Dimensions: [][]string{
			{"isItAnError", "spanName"},
			{},
			{"spanName"},
			{"isItAnError"},
		},
		Metrics: []map[string]string{
			{
				"Name": "spanCounter",
				"Unit": "Count",
			},
		},
	}
	assertCWMeasurementEqual(t, expectedMeasurement, met.Measurements[0])
}

func TestTranslateOtToCWMetricWithNameSpace(t *testing.T) {
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
	md := consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceName: "myServiceName",
			},
		},
		Metrics: []*metricspb.Metric{},
	}
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)
	assert.Nil(t, cwm)
	assert.Equal(t, 0, len(cwm))
	md = consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceNamespace: "myServiceNS",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_INT64,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_DOUBLE,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
		},
	}
	rm = internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	cwm, totalDroppedMetrics = TranslateOtToCWMetric(&rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)
	assert.NotNil(t, cwm)
	assert.Equal(t, 1, len(cwm))

	met := cwm[0]
	assert.Equal(t, "myServiceNS", met.Measurements[0].Namespace)
}

func TestTranslateOtToCWMetricWithFiltering(t *testing.T) {
	md := consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceName:      "myServiceName",
				conventions.AttributeServiceNamespace: "myServiceNS",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
		},
	}

	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	ilm := rm.InstrumentationLibraryMetrics().At(0)
	ilm.InstrumentationLibrary().InitEmpty()
	ilm.InstrumentationLibrary().SetName("cloudwatch-lib")

	testCases := []struct {
		testName              string
		metricNameSelectors   []string
		labelMatchers         []*LabelMatcher
		dimensionRollupOption string
		expectedDimensions    [][]string
		numMeasurements       int
	}{
		{
			"has match w/ Zero + Single dim rollup",
			[]string{"spanCounter"},
			nil,
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"spanName", "isItAnError"},
				{"spanName", OTellibDimensionKey},
				{OTellibDimensionKey, "isItAnError"},
				{OTellibDimensionKey},
			},
			1,
		},
		{
			"has match w/ no dim rollup",
			[]string{"spanCounter"},
			nil,
			"",
			[][]string{
				{"spanName", "isItAnError"},
				{"spanName", OTellibDimensionKey},
			},
			1,
		},
		{
			"has label match w/ no dim rollup",
			[]string{"spanCounter"},
			[]*LabelMatcher{
				{
					LabelNames: []string{"isItAnError", "spanName"},
					Regex:      "false;testSpan",
				},
			},
			"",
			[][]string{
				{"spanName", "isItAnError"},
				{"spanName", OTellibDimensionKey},
			},
			1,
		},
		{
			"no label match w/ no dim rollup",
			[]string{"spanCounter"},
			[]*LabelMatcher{
				{
					LabelNames: []string{"isItAnError", "spanName"},
					Regex:      "true;testSpan",
				},
			},
			"",
			nil,
			0,
		},
		{
			"No match w/ rollup",
			[]string{"invalid"},
			nil,
			ZeroAndSingleDimensionRollup,
			[][]string{
				{OTellibDimensionKey, "spanName"},
				{OTellibDimensionKey, "isItAnError"},
				{OTellibDimensionKey},
			},
			1,
		},
		{
			"No match w/ no rollup",
			[]string{"invalid"},
			nil,
			"",
			nil,
			0,
		},
	}
	logger := zap.NewNop()

	for _, tc := range testCases {
		m := MetricDeclaration{
			Dimensions:          [][]string{{"isItAnError", "spanName"}, {"spanName", OTellibDimensionKey}},
			MetricNameSelectors: tc.metricNameSelectors,
			LabelMatchers:       tc.labelMatchers,
		}
		config := &Config{
			Namespace:             "",
			DimensionRollupOption: tc.dimensionRollupOption,
			MetricDeclarations:    []*MetricDeclaration{&m},
		}
		t.Run(tc.testName, func(t *testing.T) {
			err := m.Init(logger)
			assert.Nil(t, err)
			cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
			assert.Equal(t, 0, totalDroppedMetrics)
			assert.Equal(t, 1, len(cwm))
			assert.NotNil(t, cwm)

			assert.Equal(t, tc.numMeasurements, len(cwm[0].Measurements))

			if tc.numMeasurements > 0 {
				dimensions := cwm[0].Measurements[0].Dimensions
				assertDimsEqual(t, tc.expectedDimensions, dimensions)
			}
		})
	}

	t.Run("No instrumentation library name w/ no dim rollup", func(t *testing.T) {
		rm = internaldata.OCToMetrics(md).ResourceMetrics().At(0)
		m := MetricDeclaration{
			Dimensions:          [][]string{{"isItAnError", "spanName"}, {"spanName", OTellibDimensionKey}},
			MetricNameSelectors: []string{"spanCounter"},
		}
		config := &Config{
			Namespace:             "",
			DimensionRollupOption: "",
			MetricDeclarations:    []*MetricDeclaration{&m},
		}
		err := m.Init(logger)
		assert.Nil(t, err)
		cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
		assert.Equal(t, 0, totalDroppedMetrics)
		assert.Equal(t, 1, len(cwm))
		assert.NotNil(t, cwm)

		assert.Equal(t, 1, len(cwm[0].Measurements))

		// No OTelLib present
		expectedDims := [][]string{
			{"spanName", "isItAnError"},
		}
		dimensions := cwm[0].Measurements[0].Dimensions
		assertDimsEqual(t, expectedDims, dimensions)
	})
}

func TestTranslateCWMetricToEMF(t *testing.T) {
	cwMeasurement := CWMeasurement{
		Namespace:  "test-emf",
		Dimensions: [][]string{{OTellibDimensionKey}, {OTellibDimensionKey, "spanName"}},
		Metrics: []map[string]string{{
			"Name": "spanCounter",
			"Unit": "Count",
		}},
	}
	timestamp := int64(1596151098037)
	fields := make(map[string]interface{})
	fields[OTellibDimensionKey] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = 0

	met := &CWMetrics{
		Timestamp:    timestamp,
		Fields:       fields,
		Measurements: []CWMeasurement{cwMeasurement},
	}
	logger := zap.NewNop()
	inputLogEvent := TranslateCWMetricToEMF([]*CWMetrics{met}, logger)

	assert.Equal(t, readFromFile("testdata/testTranslateCWMetricToEMF.json"), *inputLogEvent[0].InputLogEvent.Message, "Expect to be equal")
}

func TestTranslateCWMetricToEMFNoMeasurements(t *testing.T) {
	timestamp := int64(1596151098037)
	fields := make(map[string]interface{})
	fields[OTellibDimensionKey] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = 0

	met := &CWMetrics{
		Timestamp:    timestamp,
		Fields:       fields,
		Measurements: nil,
	}
	obs, logs := observer.New(zap.DebugLevel)
	logger := zap.New(obs)
	inputLogEvent := TranslateCWMetricToEMF([]*CWMetrics{met}, logger)
	expected := "{\"OTelLib\":\"cloudwatch-otel\",\"spanCounter\":0,\"spanName\":\"test\"}"

	assert.Equal(t, expected, *inputLogEvent[0].InputLogEvent.Message)

	// Check logged warning message
	fieldsStr, _ := json.Marshal(fields)
	expectedLogs := []observer.LoggedEntry{{
		Entry:   zapcore.Entry{Level: zap.DebugLevel, Message: "Dropped metric due to no matching metric declarations"},
		Context: []zapcore.Field{zap.String("labels", string(fieldsStr))},
	}}
	assert.Equal(t, 1, logs.Len())
	assert.Equal(t, expectedLogs, logs.AllUntimed())
}

func TestGetCWMetrics(t *testing.T) {
	namespace := "Namespace"
	OTelLib := OTellibDimensionKey
	instrumentationLibName := "InstrLibName"
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: "",
	}

	testCases := []struct {
		testName string
		metric   *metricspb.Metric
		expected []*CWMetrics
	}{
		{
			"Int gauge",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_GAUGE_INT64,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
						{Key: "label2"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
					{
						LabelValues: []*metricspb.LabelValue{
							{HasValue: false},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 3,
								},
							},
						},
					},
				},
			},
			[]*CWMetrics{
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label1", "label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    int64(1),
						"label1": "value1",
						"label2": "value2",
					},
				},
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    int64(3),
						"label2": "value2",
					},
				},
			},
		},
		{
			"Double gauge",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_GAUGE_DOUBLE,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
						{Key: "label2"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
					{
						LabelValues: []*metricspb.LabelValue{
							{HasValue: false},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.3,
								},
							},
						},
					},
				},
			},
			[]*CWMetrics{
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label1", "label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    0.1,
						"label1": "value1",
						"label2": "value2",
					},
				},
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    0.3,
						"label2": "value2",
					},
				},
			},
		},
		{
			"Int sum",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_INT64,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
						{Key: "label2"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
					{
						LabelValues: []*metricspb.LabelValue{
							{HasValue: false},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 3,
								},
							},
						},
					},
				},
			},
			[]*CWMetrics{
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label1", "label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    0,
						"label1": "value1",
						"label2": "value2",
					},
				},
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    0,
						"label2": "value2",
					},
				},
			},
		},
		{
			"Double sum",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
						{Key: "label2"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
					{
						LabelValues: []*metricspb.LabelValue{
							{HasValue: false},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.3,
								},
							},
						},
					},
				},
			},
			[]*CWMetrics{
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label1", "label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    0,
						"label1": "value1",
						"label2": "value2",
					},
				},
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Count"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib:  instrumentationLibName,
						"foo":    0,
						"label2": "value2",
					},
				},
			},
		},
		{
			"Double histogram",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
					Unit: "Seconds",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
						{Key: "label2"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Sum:   15.0,
										Count: 5,
										BucketOptions: &metricspb.DistributionValue_BucketOptions{
											Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{0, 10},
												},
											},
										},
										Buckets: []*metricspb.DistributionValue_Bucket{
											{
												Count: 0,
											},
											{
												Count: 4,
											},
											{
												Count: 1,
											},
										},
									},
								},
							},
						},
					},
					{
						LabelValues: []*metricspb.LabelValue{
							{HasValue: false},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Sum:   35.0,
										Count: 18,
										BucketOptions: &metricspb.DistributionValue_BucketOptions{
											Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{0, 10},
												},
											},
										},
										Buckets: []*metricspb.DistributionValue_Bucket{
											{
												Count: 5,
											},
											{
												Count: 6,
											},
											{
												Count: 7,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			[]*CWMetrics{
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label1", "label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Seconds"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib: instrumentationLibName,
						"foo": &CWMetricStats{
							Sum:   15.0,
							Count: 5,
						},
						"label1": "value1",
						"label2": "value2",
					},
				},
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Seconds"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib: instrumentationLibName,
						"foo": &CWMetricStats{
							Sum:   35.0,
							Count: 18,
						},
						"label2": "value2",
					},
				},
			},
		},
		{
			"Double summary",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_SUMMARY,
					Unit: "Seconds",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
						{Key: "label2"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_SummaryValue{
									SummaryValue: &metricspb.SummaryValue{
										Sum: &wrappers.DoubleValue{
											Value: 15.0,
										},
										Count: &wrappers.Int64Value{
											Value: 5,
										},
										Snapshot: &metricspb.SummaryValue_Snapshot{
											Count: &wrappers.Int64Value{
												Value: 5,
											},
											Sum: &wrappers.DoubleValue{
												Value: 15.0,
											},
											PercentileValues: []*metricspb.SummaryValue_Snapshot_ValueAtPercentile{
												{
													Percentile: 0.0,
													Value:      1,
												},
												{
													Percentile: 100.0,
													Value:      5,
												},
											},
										},
									},
								},
							},
						},
					},
					{
						LabelValues: []*metricspb.LabelValue{
							{HasValue: false},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_SummaryValue{
									SummaryValue: &metricspb.SummaryValue{
										Sum: &wrappers.DoubleValue{
											Value: 35.0,
										},
										Count: &wrappers.Int64Value{
											Value: 18,
										},
										Snapshot: &metricspb.SummaryValue_Snapshot{
											Count: &wrappers.Int64Value{
												Value: 18,
											},
											Sum: &wrappers.DoubleValue{
												Value: 35.0,
											},
											PercentileValues: []*metricspb.SummaryValue_Snapshot_ValueAtPercentile{
												{
													Percentile: 0.0,
													Value:      0,
												},
												{
													Percentile: 100.0,
													Value:      5,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			[]*CWMetrics{
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label1", "label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Seconds"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib: instrumentationLibName,
						"foo": &CWMetricStats{
							Min:   1,
							Max:   5,
							Sum:   15.0,
							Count: 5,
						},
						"label1": "value1",
						"label2": "value2",
					},
				},
				{
					Measurements: []CWMeasurement{
						{
							Namespace: namespace,
							Dimensions: [][]string{
								{"label2", OTelLib},
							},
							Metrics: []map[string]string{
								{"Name": "foo", "Unit": "Seconds"},
							},
						},
					},
					Fields: map[string]interface{}{
						OTelLib: instrumentationLibName,
						"foo": &CWMetricStats{
							Min:   0,
							Max:   5,
							Sum:   35.0,
							Count: 18,
						},
						"label2": "value2",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			oc := consumerdata.MetricsData{
				Node: &commonpb.Node{},
				Resource: &resourcepb.Resource{
					Labels: map[string]string{
						conventions.AttributeServiceName:      "myServiceName",
						conventions.AttributeServiceNamespace: "myServiceNS",
					},
				},
				Metrics: []*metricspb.Metric{tc.metric},
			}

			// Retrieve *pdata.Metric
			rms := internaldata.OCToMetrics(oc).ResourceMetrics()
			assert.Equal(t, 1, rms.Len())
			ilms := rms.At(0).InstrumentationLibraryMetrics()
			assert.Equal(t, 1, ilms.Len())
			metrics := ilms.At(0).Metrics()
			assert.Equal(t, 1, metrics.Len())
			metric := metrics.At(0)

			cwMetrics := getCWMetrics(&metric, namespace, instrumentationLibName, config)
			assert.Equal(t, len(tc.expected), len(cwMetrics))

			for i, expected := range tc.expected {
				cwMetric := cwMetrics[i]
				assert.Equal(t, len(expected.Measurements), len(cwMetric.Measurements))
				for i, expectedMeasurement := range expected.Measurements {
					assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[i])
				}
				assert.Equal(t, len(expected.Fields), len(cwMetric.Fields))
				assert.Equal(t, expected.Fields, cwMetric.Fields)
			}
		})
	}

	t.Run("Unhandled metric type", func(t *testing.T) {
		metric := pdata.NewMetric()
		metric.InitEmpty()
		metric.SetName("foo")
		metric.SetUnit("Count")
		metric.SetDataType(pdata.MetricDataTypeIntHistogram)

		obs, logs := observer.New(zap.WarnLevel)
		obsConfig := &Config{
			DimensionRollupOption: "",
			logger:                zap.New(obs),
		}

		cwMetrics := getCWMetrics(&metric, namespace, instrumentationLibName, obsConfig)
		assert.Nil(t, cwMetrics)

		// Test output warning logs
		expectedLogs := []observer.LoggedEntry{
			{
				Entry: zapcore.Entry{Level: zap.WarnLevel, Message: "Unhandled metric data type."},
				Context: []zapcore.Field{
					zap.String("DataType", "IntHistogram"),
					zap.String("Name", "foo"),
					zap.String("Unit", "Count"),
				},
			},
		}
		assert.Equal(t, 1, logs.Len())
		assert.Equal(t, expectedLogs, logs.AllUntimed())
	})

	t.Run("Nil metric", func(t *testing.T) {
		cwMetrics := getCWMetrics(nil, namespace, instrumentationLibName, config)
		assert.Nil(t, cwMetrics)
	})
}

func TestBuildCWMetric(t *testing.T) {
	namespace := "Namespace"
	instrLibName := "InstrLibName"
	OTelLib := OTellibDimensionKey
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: "",
	}
	metricSlice := []map[string]string{
		{
			"Name": "foo",
			"Unit": "",
		},
	}

	// Test data types
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("foo")

	t.Run("Int gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(int64(-17))

		cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, instrLibName, config)

		assert.NotNil(t, cwMetric)
		assert.Equal(t, 1, len(cwMetric.Measurements))
		expectedMeasurement := CWMeasurement{
			Namespace:  namespace,
			Dimensions: [][]string{{"label1", OTelLib}},
			Metrics:    metricSlice,
		}
		assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[0])
		expectedFields := map[string]interface{}{
			OTelLib:  instrLibName,
			"foo":    int64(-17),
			"label1": "value1",
		}
		assert.Equal(t, expectedFields, cwMetric.Fields)
	})

	t.Run("Double gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleGauge)
		dp := pdata.NewDoubleDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(0.3)

		cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, instrLibName, config)

		assert.NotNil(t, cwMetric)
		assert.Equal(t, 1, len(cwMetric.Measurements))
		expectedMeasurement := CWMeasurement{
			Namespace:  namespace,
			Dimensions: [][]string{{"label1", OTelLib}},
			Metrics:    metricSlice,
		}
		assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[0])
		expectedFields := map[string]interface{}{
			OTelLib:  instrLibName,
			"foo":    0.3,
			"label1": "value1",
		}
		assert.Equal(t, expectedFields, cwMetric.Fields)
	})

	t.Run("Int sum", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntSum)
		metric.IntSum().InitEmpty()
		metric.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp := pdata.NewIntDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(int64(-17))

		cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, instrLibName, config)

		assert.NotNil(t, cwMetric)
		assert.Equal(t, 1, len(cwMetric.Measurements))
		expectedMeasurement := CWMeasurement{
			Namespace:  namespace,
			Dimensions: [][]string{{"label1", OTelLib}},
			Metrics:    metricSlice,
		}
		assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[0])
		expectedFields := map[string]interface{}{
			OTelLib:  instrLibName,
			"foo":    0,
			"label1": "value1",
		}
		assert.Equal(t, expectedFields, cwMetric.Fields)
	})

	t.Run("Double sum", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleSum)
		metric.DoubleSum().InitEmpty()
		metric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp := pdata.NewDoubleDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(0.3)

		cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, instrLibName, config)

		assert.NotNil(t, cwMetric)
		assert.Equal(t, 1, len(cwMetric.Measurements))
		expectedMeasurement := CWMeasurement{
			Namespace:  namespace,
			Dimensions: [][]string{{"label1", OTelLib}},
			Metrics:    metricSlice,
		}
		assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[0])
		expectedFields := map[string]interface{}{
			OTelLib:  instrLibName,
			"foo":    0,
			"label1": "value1",
		}
		assert.Equal(t, expectedFields, cwMetric.Fields)
	})

	t.Run("Double histogram", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleHistogram)
		dp := pdata.NewDoubleHistogramDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetCount(uint64(17))
		dp.SetSum(17.13)
		dp.SetBucketCounts([]uint64{1, 2, 3})
		dp.SetExplicitBounds([]float64{1, 2, 3})

		cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, instrLibName, config)

		assert.NotNil(t, cwMetric)
		assert.Equal(t, 1, len(cwMetric.Measurements))
		expectedMeasurement := CWMeasurement{
			Namespace:  namespace,
			Dimensions: [][]string{{"label1", OTelLib}},
			Metrics:    metricSlice,
		}
		assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[0])
		expectedFields := map[string]interface{}{
			OTelLib: instrLibName,
			"foo": &CWMetricStats{
				Sum:   17.13,
				Count: 17,
			},
			"label1": "value1",
		}
		assert.Equal(t, expectedFields, cwMetric.Fields)
	})

	t.Run("Invalid datapoint type", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntHistogramDataPoint()
		dp.InitEmpty()

		cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, instrLibName, config)
		assert.Nil(t, cwMetric)
	})

	// Test rollup options and labels
	testCases := []struct {
		testName              string
		labels                map[string]string
		dimensionRollupOption string
		expectedDims          [][]string
	}{
		{
			"Single label w/ no rollup",
			map[string]string{"a": "foo"},
			"",
			[][]string{
				{"a", OTelLib},
			},
		},
		{
			"Single label w/ single rollup",
			map[string]string{"a": "foo"},
			SingleDimensionRollupOnly,
			[][]string{
				{"a", OTelLib},
			},
		},
		{
			"Single label w/ zero + single rollup",
			map[string]string{"a": "foo"},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a", OTelLib},
				{OTelLib},
			},
		},
		{
			"Multiple label w/ no rollup",
			map[string]string{
				"a": "foo",
				"b": "bar",
				"c": "car",
			},
			"",
			[][]string{
				{"a", "b", "c", OTelLib},
			},
		},
		{
			"Multiple label w/ rollup",
			map[string]string{
				"a": "foo",
				"b": "bar",
				"c": "car",
			},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a", "b", "c", OTelLib},
				{OTelLib, "a"},
				{OTelLib, "b"},
				{OTelLib, "c"},
				{OTelLib},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			dp := pdata.NewIntDataPoint()
			dp.InitEmpty()
			dp.LabelsMap().InitFromMap(tc.labels)
			dp.SetValue(int64(-17))
			config = &Config{
				Namespace:             namespace,
				DimensionRollupOption: tc.dimensionRollupOption,
			}

			expectedFields := map[string]interface{}{
				OTellibDimensionKey: OTelLib,
				"foo":               int64(-17),
			}
			for k, v := range tc.labels {
				expectedFields[k] = v
			}
			expectedMeasurement := CWMeasurement{
				Namespace:  namespace,
				Dimensions: tc.expectedDims,
				Metrics:    metricSlice,
			}

			cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, OTelLib, config)

			// Check fields
			assert.Equal(t, expectedFields, cwMetric.Fields)

			// Check CW measurement
			assert.Equal(t, 1, len(cwMetric.Measurements))
			assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[0])
		})
	}
}

func TestBuildCWMetricWithMetricDeclarations(t *testing.T) {
	namespace := "Namespace"
	OTelLib := OTellibDimensionKey
	instrumentationLibName := "cloudwatch-otel"
	metricName := "metric1"
	metricValue := int64(-17)
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName(metricName)
	metricSlice := []map[string]string{{"Name": metricName}}
	testCases := []struct {
		testName              string
		labels                map[string]string
		metricDeclarations    []*MetricDeclaration
		dimensionRollupOption string
		expectedDims          [][]string
	}{
		{
			"Single label w/ no rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][]string{{"a"}},
		},
		{
			"Single label + OTelLib w/ no rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", OTelLib}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][]string{{"a", OTelLib}},
		},
		{
			"Single label w/ single rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			SingleDimensionRollupOnly,
			[][]string{{"a"}, {"a", OTelLib}},
		},
		{
			"Single label w/ zero/single rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][]string{{"a"}, {"a", OTelLib}, {OTelLib}},
		},
		{
			"No matching metric name",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a"}},
					MetricNameSelectors: []string{"invalid"},
				},
			},
			"",
			nil,
		},
		{
			"multiple labels w/ no rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][]string{{"a"}},
		},
		{
			"multiple labels w/ rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a"},
				{OTelLib, "a"},
				{OTelLib, "b"},
				{OTelLib},
			},
		},
		{
			"multiple labels + multiple dimensions w/ no rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][]string{{"a", "b"}, {"b"}},
		},
		{
			"multiple labels + multiple dimensions + OTelLib w/ no rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b"}, {"b", OTelLib}, {OTelLib}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][]string{{"a", "b"}, {"b", OTelLib}, {OTelLib}},
		},
		{
			"multiple labels + multiple dimensions w/ rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a", "b"},
				{"b"},
				{OTelLib, "a"},
				{OTelLib, "b"},
				{OTelLib},
			},
		},
		{
			"multiple labels, multiple dimensions w/ invalid dimension",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b", "c"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"b"},
				{OTelLib, "a"},
				{OTelLib, "b"},
				{OTelLib},
			},
		},
		{
			"multiple labels, multiple dimensions w/ missing dimension",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a", "b"},
				{"b"},
				{OTelLib, "a"},
				{OTelLib, "b"},
				{OTelLib, "c"},
				{OTelLib},
			},
		},
		{
			"multiple metric declarations w/ no rollup",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions:          [][]string{{"a", "c"}, {"b"}, {"c"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions:          [][]string{{"a", "d"}, {"b", "c"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][]string{
				{"a", "b"},
				{"b"},
				{"a", "c"},
				{"c"},
				{"b", "c"},
			},
		},
		{
			"multiple metric declarations w/ rollup",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions:          [][]string{{"a", "c"}, {"b"}, {"c"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions:          [][]string{{"a", "d"}, {"b", "c"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a", "b"},
				{"b"},
				{OTelLib, "a"},
				{OTelLib, "b"},
				{OTelLib, "c"},
				{OTelLib},
				{"a", "c"},
				{"c"},
				{"b", "c"},
			},
		},
		{
			"remove measurements with no dimensions",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions:          [][]string{{"a", "d"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][]string{
				{"a", "b"},
				{"b"},
			},
		},
		{
			"multiple declarations w/ no dimensions",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "e"}, {"d"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions:          [][]string{{"a", "d"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			nil,
		},
		{
			"no labels",
			map[string]string{},
			[]*MetricDeclaration{
				{
					Dimensions:          [][]string{{"a", "b", "c"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			dp := pdata.NewIntDataPoint()
			dp.InitEmpty()
			dp.LabelsMap().InitFromMap(tc.labels)
			dp.SetValue(metricValue)
			config := &Config{
				Namespace:             namespace,
				DimensionRollupOption: tc.dimensionRollupOption,
				MetricDeclarations:    tc.metricDeclarations,
			}
			logger := zap.NewNop()
			for _, m := range tc.metricDeclarations {
				err := m.Init(logger)
				assert.Nil(t, err)
			}

			expectedFields := map[string]interface{}{
				OTellibDimensionKey: instrumentationLibName,
				metricName:          metricValue,
			}
			for k, v := range tc.labels {
				expectedFields[k] = v
			}

			cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, instrumentationLibName, config)

			// Check fields
			assert.Equal(t, expectedFields, cwMetric.Fields)

			// Check CW measurement
			if tc.expectedDims == nil {
				assert.Equal(t, 0, len(cwMetric.Measurements))
			} else {
				assert.Equal(t, 1, len(cwMetric.Measurements))
				expectedMeasurement := CWMeasurement{
					Namespace:  namespace,
					Dimensions: tc.expectedDims,
					Metrics:    metricSlice,
				}
				assertCWMeasurementEqual(t, expectedMeasurement, cwMetric.Measurements[0])
			}
		})
	}
}

func TestCalculateRate(t *testing.T) {
	metricName := "newMetric"
	labels := make(map[string]string)
	labels[OTellibDimensionKey] = "cloudwatch-otel"
	labels["spanName"] = "test"
	labels["spanCounter"] = "foo"
	labels["type"] = "Int64"
	prevTime := time.Now().UnixNano() / int64(time.Millisecond)
	curTime := time.Unix(0, prevTime*int64(time.Millisecond)).Add(time.Second*10).UnixNano() / int64(time.Millisecond)

	prevValue := int64(0)
	curValue := int64(10)
	rate := calculateRate(labels, metricName, prevValue, prevTime)
	assert.Equal(t, 0, rate)
	rate = calculateRate(labels, metricName, curValue, curTime)
	assert.Equal(t, int64(1), rate)

	prevDoubleValue := 0.0
	curDoubleValue := 5.0
	labels["type"] = "Float64"
	rate = calculateRate(labels, metricName, prevDoubleValue, prevTime)
	assert.Equal(t, 0, rate)
	rate = calculateRate(labels, metricName, curDoubleValue, curTime)
	assert.Equal(t, 0.5, rate)
}

func TestDimensionRollup(t *testing.T) {
	testCases := []struct {
		testName               string
		dimensionRollupOption  string
		dims                   []string
		instrumentationLibName string
		expected               [][]string
	}{
		{
			"no rollup w/o instrumentation library name",
			"",
			[]string{"a", "b", "c"},
			noInstrumentationLibraryName,
			nil,
		},
		{
			"no rollup w/ instrumentation library name",
			"",
			[]string{"a", "b", "c"},
			"cloudwatch-otel",
			nil,
		},
		{
			"single dim w/o instrumentation library name",
			SingleDimensionRollupOnly,
			[]string{"a", "b", "c"},
			noInstrumentationLibraryName,
			[][]string{
				{"a"},
				{"b"},
				{"c"},
			},
		},
		{
			"single dim w/ instrumentation library name",
			SingleDimensionRollupOnly,
			[]string{"a", "b", "c"},
			"cloudwatch-otel",
			[][]string{
				{OTellibDimensionKey, "a"},
				{OTellibDimensionKey, "b"},
				{OTellibDimensionKey, "c"},
			},
		},
		{
			"single dim w/o instrumentation library name and only one label",
			SingleDimensionRollupOnly,
			[]string{"a"},
			noInstrumentationLibraryName,
			[][]string{{"a"}},
		},
		{
			"single dim w/ instrumentation library name and only one label",
			SingleDimensionRollupOnly,
			[]string{"a"},
			"cloudwatch-otel",
			[][]string{{OTellibDimensionKey, "a"}},
		},
		{
			"zero + single dim w/o instrumentation library name",
			ZeroAndSingleDimensionRollup,
			[]string{"a", "b", "c"},
			noInstrumentationLibraryName,
			[][]string{
				{},
				{"a"},
				{"b"},
				{"c"},
			},
		},
		{
			"zero + single dim w/ instrumentation library name",
			ZeroAndSingleDimensionRollup,
			[]string{"a", "b", "c", "A"},
			"cloudwatch-otel",
			[][]string{
				{OTellibDimensionKey},
				{OTellibDimensionKey, "a"},
				{OTellibDimensionKey, "b"},
				{OTellibDimensionKey, "c"},
				{OTellibDimensionKey, "A"},
			},
		},
		{
			"zero dim rollup w/o instrumentation library name and no labels",
			ZeroAndSingleDimensionRollup,
			[]string{},
			noInstrumentationLibraryName,
			nil,
		},
		{
			"zero dim rollup w/ instrumentation library name and no labels",
			ZeroAndSingleDimensionRollup,
			[]string{},
			"cloudwatch-otel",
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			rolledUp := dimensionRollup(tc.dimensionRollupOption, tc.dims, tc.instrumentationLibName)
			assertDimsEqual(t, tc.expected, rolledUp)
		})
	}
}

func TestNeedsCalculateRate(t *testing.T) {
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetDataType(pdata.MetricDataTypeIntGauge)
	assert.False(t, needsCalculateRate(&metric))
	metric.SetDataType(pdata.MetricDataTypeDoubleGauge)
	assert.False(t, needsCalculateRate(&metric))

	metric.SetDataType(pdata.MetricDataTypeIntHistogram)
	assert.False(t, needsCalculateRate(&metric))
	metric.SetDataType(pdata.MetricDataTypeDoubleHistogram)
	assert.False(t, needsCalculateRate(&metric))

	metric.SetDataType(pdata.MetricDataTypeIntSum)
	metric.IntSum().InitEmpty()
	metric.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	assert.True(t, needsCalculateRate(&metric))
	metric.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityDelta)
	assert.False(t, needsCalculateRate(&metric))

	metric.SetDataType(pdata.MetricDataTypeDoubleSum)
	metric.DoubleSum().InitEmpty()
	metric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	assert.True(t, needsCalculateRate(&metric))
	metric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityDelta)
	assert.False(t, needsCalculateRate(&metric))
}

func BenchmarkTranslateOtToCWMetricWithInstrLibrary(b *testing.B) {
	md := createMetricTestData()
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	ilms := rm.InstrumentationLibraryMetrics()
	ilm := ilms.At(0)
	ilm.InstrumentationLibrary().InitEmpty()
	ilm.InstrumentationLibrary().SetName("cloudwatch-lib")
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		TranslateOtToCWMetric(&rm, config)
	}
}

func BenchmarkTranslateOtToCWMetricWithoutInstrLibrary(b *testing.B) {
	md := createMetricTestData()
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		TranslateOtToCWMetric(&rm, config)
	}
}

func BenchmarkTranslateOtToCWMetricWithFiltering(b *testing.B) {
	md := createMetricTestData()
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	ilms := rm.InstrumentationLibraryMetrics()
	ilm := ilms.At(0)
	ilm.InstrumentationLibrary().InitEmpty()
	ilm.InstrumentationLibrary().SetName("cloudwatch-lib")
	m := MetricDeclaration{
		Dimensions:          [][]string{{"spanName"}},
		MetricNameSelectors: []string{"spanCounter", "spanGaugeCounter"},
	}
	logger := zap.NewNop()
	m.Init(logger)
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
		MetricDeclarations:    []*MetricDeclaration{&m},
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		TranslateOtToCWMetric(&rm, config)
	}
}

func BenchmarkTranslateCWMetricToEMF(b *testing.B) {
	cwMeasurement := CWMeasurement{
		Namespace:  "test-emf",
		Dimensions: [][]string{{OTellibDimensionKey}, {OTellibDimensionKey, "spanName"}},
		Metrics: []map[string]string{{
			"Name": "spanCounter",
			"Unit": "Count",
		}},
	}
	timestamp := int64(1596151098037)
	fields := make(map[string]interface{})
	fields[OTellibDimensionKey] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = 0

	met := &CWMetrics{
		Timestamp:    timestamp,
		Fields:       fields,
		Measurements: []CWMeasurement{cwMeasurement},
	}
	logger := zap.NewNop()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		TranslateCWMetricToEMF([]*CWMetrics{met}, logger)
	}
}

func BenchmarkDimensionRollup(b *testing.B) {
	dimensions := []string{"a", "b", "c"}
	for n := 0; n < b.N; n++ {
		dimensionRollup(ZeroAndSingleDimensionRollup, dimensions, "cloudwatch-otel")
	}
}
