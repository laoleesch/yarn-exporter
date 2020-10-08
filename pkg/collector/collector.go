package collector

import (
	"errors"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter collects YARN stats from the given API URL and exports them using
// the prometheus metrics package.
type Exporter struct {
	metricsInfo map[string]MetricInfo
	fetch       func() ([]byte, error)
	scrape      func(chan<- prometheus.Metric, func() ([]byte, error)) error

	up                   prometheus.Gauge
	totalFetches         prometheus.Counter
	totalFetchesFailures prometheus.Counter

	mutex  sync.RWMutex
	logger log.Logger
}

// NewExporter returns an initialized Exporter.
func NewExporter(namespace string, api APIMeter, fetchDataFunc func() ([]byte, error), logger log.Logger) (*Exporter, error) {

	if fetchDataFunc == nil || logger == nil {
		return nil, errors.New("wrong Exporter init")
	}

	metricsInfo := api.DefMetricsInfo(namespace)

	return &Exporter{
		metricsInfo: metricsInfo,
		fetch:       fetchDataFunc,
		scrape:      api.Scrape,

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace + "_" + api.Subsystem(),
			Name:      "up",
			Help:      "Was the last scrape of YARN " + api.Subsystem() + " API successful.",
		}),
		totalFetches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace + "_" + api.Subsystem(),
			Name:      "fetches_total",
			Help:      "Current total YARN " + api.Subsystem() + " API fetches.",
		}),
		totalFetchesFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace + "_" + api.Subsystem(),
			Name:      "fetches_failures_total",
			Help:      "Current total YARN " + api.Subsystem() + " API fetches failures.",
		}),
		logger: logger,
	}, nil
}

// Describe describes all the metrics ever exported by the YARN exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range e.metricsInfo {
		ch <- m.Desc
	}
	ch <- e.up.Desc()
	ch <- e.totalFetches.Desc()
	ch <- e.totalFetchesFailures.Desc()
}

// Collect fetches the stats from configured YARN API handlers and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	up := 1.0
	e.totalFetches.Inc()
	if err := e.scrape(ch, e.fetch); err != nil {
		up = 0.0
		e.totalFetchesFailures.Inc()
		level.Error(e.logger).Log("msg", "Can't scrape metrics", "err", err)
	}

	ch <- prometheus.MustNewConstMetric(e.up.Desc(), prometheus.GaugeValue, up)
	ch <- e.totalFetches
	ch <- e.totalFetchesFailures
}
