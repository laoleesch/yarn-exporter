package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	listenAddress         = kingpin.Flag("web.listen-address", "Address to listen on.").Default(":9911").String()
	metricsPath           = kingpin.Flag("web.metrics-path", "Path under which to expose metrics.").Default("/metrics").String()
	yarnURL               = kingpin.Flag("yarn.url", "URL on which to scrape yarn.").Default("http://localhost:8088").String()
	yarnSSLVerify         = kingpin.Flag("yarn.ssl-verify", "Flag that enables SSL certificate verification for the scrape URI").Default("false").Bool()
	yarnScrapeScheduler   = kingpin.Flag("yarn.scrape-scheduler", "Flag that enables scheduler metrics /ws/v1/cluster/scheduler").Default("false").Bool()
	yarnScrapeAppsRunning = kingpin.Flag("yarn.scrape-apps-running", "Flag that enables running apps metrics /ws/v1/cluster/apps?state=RUNNING").Default("false").Bool()
	yarnTimeout           = kingpin.Flag("yarn.timeout", "Timeout for trying to get stats from yarn.").Default("5s").Duration()
)

func main() {
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)
	level.Info(logger).Log("msg", "Starting yarn-exporter")
	level.Info(logger).Log("msg", "Listening on ", "address", *listenAddress)

	exporter, err := NewExporter(*yarnURL, *yarnSSLVerify, *yarnScrapeScheduler, *yarnScrapeAppsRunning, *yarnTimeout, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(exporter)

	r := http.NewServeMux()
	r.Handle(*metricsPath, promhttp.Handler())
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>YARN Exporter</title></head>
             <body>
             <h1>YARN Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	srv := &http.Server{
		Handler:      r,
		Addr:         *listenAddress,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			level.Error(logger).Log("msg", "http listener error", "err", err)
			os.Exit(2)
		}
	}()

	chStop := make(chan os.Signal, 1)
	signal.Notify(chStop, os.Interrupt, syscall.SIGTERM)

	signal := <-chStop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		level.Error(logger).Log("msg", "error during server shutdown", "err", err)
	}
	level.Info(logger).Log("msg", "server has been shutted down", "signal", signal)
	os.Exit(0)

}

const (
	namespace = "yarn"
)

// Exporter collects YARN stats from the given API URL and exports them using
// the prometheus metrics package.
type Exporter struct {
	URL                                *url.URL
	scrapeScheduler, scrapeAppsRunning bool
	timeout                            time.Duration

	up                   prometheus.Gauge
	totalScrapes         prometheus.Counter
	totalScrapesFailures prometheus.Counter

	mutex  sync.RWMutex
	logger log.Logger
}

// NewExporter returns an initialized Exporter.
func NewExporter(targetURL string, sslVerify bool, yarnScrapeScheduler bool, yarnScrapeAppsRunning bool, timeout time.Duration, logger log.Logger) (*Exporter, error) {
	baseURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}

	return &Exporter{
		URL:               baseURL,
		scrapeScheduler:   yarnScrapeScheduler,
		scrapeAppsRunning: yarnScrapeAppsRunning,
		timeout:           timeout,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Was the last scrape of YARN successful.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total YARN scrapes.",
		}),
		totalScrapesFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_failures_total",
			Help:      "Current total YARN scrapes failures.",
		}),
		logger: logger,
	}, nil
}

type metricInfo struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}

func newClusterMetric(metricName string, docString string, t prometheus.ValueType, variableLabels []string, constLabels prometheus.Labels) metricInfo {
	return metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cluster", metricName),
			docString,
			variableLabels,
			constLabels,
		),
		Type: t,
	}
}

var (
	// metrics Info

	clusterMetrics = map[string]metricInfo{
		"appsSubmitted":         newClusterMetric("applications_submitted", "Total applications submitted", prometheus.CounterValue, nil, nil),
		"appsCompleted":         newClusterMetric("applications_completed", "Total applications completed", prometheus.CounterValue, nil, nil),
		"appsPending":           newClusterMetric("applications_pending", "Applications pending", prometheus.GaugeValue, nil, nil),
		"appsRunning":           newClusterMetric("applications_running", "Applications running", prometheus.GaugeValue, nil, nil),
		"appsFailed":            newClusterMetric("applications_failed", "Total application failed", prometheus.CounterValue, nil, nil),
		"appsKilled":            newClusterMetric("applications_killed", "Total application killed", prometheus.CounterValue, nil, nil),
		"reservedMB":            newClusterMetric("memory_reserved", "Memory reserved", prometheus.GaugeValue, nil, nil),
		"availableMB":           newClusterMetric("memory_available", "Memory available", prometheus.GaugeValue, nil, nil),
		"allocatedMB":           newClusterMetric("memory_allocated", "Memory allocated", prometheus.GaugeValue, nil, nil),
		"totalMB":               newClusterMetric("memory_total", "Total memory", prometheus.GaugeValue, nil, nil),
		"reservedVirtualCores":  newClusterMetric("virtual_cores_reserved", "Virtual cores reserved", prometheus.GaugeValue, nil, nil),
		"availableVirtualCores": newClusterMetric("virtual_cores_available", "Virtual cores available", prometheus.GaugeValue, nil, nil),
		"allocatedVirtualCores": newClusterMetric("virtual_cores_allocated", "Virtual cores allocated", prometheus.GaugeValue, nil, nil),
		"totalVirtualCores":     newClusterMetric("virtual_cores_total", "Total virtual cores", prometheus.GaugeValue, nil, nil),
		"containersAllocated":   newClusterMetric("containers_allocated", "Containers allocated", prometheus.GaugeValue, nil, nil),
		"containersReserved":    newClusterMetric("containers_reserved", "Containers reserved", prometheus.GaugeValue, nil, nil),
		"containersPending":     newClusterMetric("containers_pending", "Containers pending", prometheus.GaugeValue, nil, nil),
		"totalNodes":            newClusterMetric("nodes_total", "Nodes total", prometheus.GaugeValue, nil, nil),
		"lostNodes":             newClusterMetric("nodes_lost", "Nodes lost", prometheus.GaugeValue, nil, nil),
		"unhealthyNodes":        newClusterMetric("nodes_unhealthy", "Nodes unhealthy", prometheus.GaugeValue, nil, nil),
		"decommissionedNodes":   newClusterMetric("nodes_decommissioned", "Nodes decommissioned", prometheus.GaugeValue, nil, nil),
		"decommissioningNodes":  newClusterMetric("nodes_decommissioning", "Nodes decommissioning", prometheus.GaugeValue, nil, nil),
		"rebootedNodes":         newClusterMetric("nodes_rebooted", "Nodes rebooted", prometheus.GaugeValue, nil, nil),
		"activeNodes":           newClusterMetric("nodes_active", "Nodes active", prometheus.GaugeValue, nil, nil),
	}
)

// Describe describes all the metrics ever exported by the YARN exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range clusterMetrics {
		ch <- m.Desc
	}
	ch <- e.up.Desc()
	ch <- e.totalScrapes.Desc()
}

// Collect fetches the stats from configured YARN API handlers and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	up := e.scrapeClusterMetrics(ch)

	ch <- prometheus.MustNewConstMetric(e.up.Desc(), prometheus.GaugeValue, up)
	ch <- e.totalScrapes
}

func (e *Exporter) scrapeClusterMetrics(ch chan<- prometheus.Metric) (up float64) {
	e.totalScrapes.Inc()

	var data map[string]map[string]float64
	body, err := e.fetchPath("/ws/v1/cluster/metrics")
	if err != nil {
		e.totalScrapesFailures.Inc()
		level.Error(e.logger).Log("msg", "Can't scrape YARN", "err", err)
		return 0
	}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e.totalScrapesFailures.Inc()
		level.Error(e.logger).Log("msg", "Can't unmarshal cluster metrics", "err", err)
		return 0
	}

	for name, metric := range clusterMetrics {
		ch <- prometheus.MustNewConstMetric(metric.Desc, metric.Type, data["clusterMetrics"][name])
	}

	return 1.0
}

func (e *Exporter) fetchPath(subpath string) ([]byte, error) {
	targetURL, err := e.URL.Parse(subpath)
	if err != nil {
		level.Error(e.logger).Log("msg", "Can't parse target url", "err", err)
		return nil, err
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: !*yarnSSLVerify}}
	client := http.Client{
		Timeout:   e.timeout,
		Transport: tr,
	}
	req := http.Request{
		Method:     "GET",
		URL:        targetURL,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       targetURL.Host,
	}
	resp, err := client.Do(&req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil, errors.New("unexpected HTTP status: " + string(resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
