package collector

import "github.com/prometheus/client_golang/prometheus"

// MetricInfo contains Desc & Type
type MetricInfo struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}

// NewMetric creates new MetricInfo
func NewMetric(namespace, subsystem, metricName, docString string, t prometheus.ValueType, variableLabels []string) MetricInfo {
	return MetricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, metricName),
			docString,
			variableLabels,
			nil,
		),
		Type: t,
	}
}

// APIMeter impl API if
type APIMeter interface {
	Subsystem() string
	Scrape(ch chan<- prometheus.Metric, getDataFunc func() ([]byte, error)) error
	DefMetricsInfo(namespace string) map[string]MetricInfo
}
