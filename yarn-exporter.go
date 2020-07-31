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
	listenAddress       = kingpin.Flag("web.listen-address", "Address to listen on.").Default(":9113").String()
	metricsPath         = kingpin.Flag("web.metrics-path", "Path under which to expose metrics.").Default("/metrics").String()
	yarnURL             = kingpin.Flag("yarn.url", "URL on which to scrape yarn.").Default("http://localhost:8088").String()
	yarnSSLVerify       = kingpin.Flag("yarn.ssl-verify", "Flag that enables SSL certificate verification for the scrape URI").Default("false").Bool()
	yarnScrapeScheduler = kingpin.Flag("yarn.scrape-scheduler", "Flag that enables scheduler metrics /ws/v1/cluster/scheduler").Default("false").Bool()
	// yarnScrapeAppsRunning = kingpin.Flag("yarn.scrape-apps-running", "Flag that enables running apps metrics /ws/v1/cluster/apps?state=RUNNING").Default("false").Bool()
	yarnTimeout = kingpin.Flag("yarn.timeout", "Timeout for trying to get stats from yarn.").Default("5s").Duration()
)

const (
	namespace          = "yarn"
	yarn_api_cluster   = "/ws/v1/cluster/metrics"
	yarn_api_scheduler = "/ws/v1/cluster/scheduler"
)

func main() {
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)
	level.Info(logger).Log("msg", "Starting yarn-exporter")
	level.Info(logger).Log("msg", "Listening on ", "address", *listenAddress)

	_, err := url.Parse(*yarnURL)
	if err != nil {
		level.Error(logger).Log("msg", "Error parse url", "err", err)
		os.Exit(1)
	}

	var fetchClusterMetricData func() ([]byte, error)
	var fetchSchedulerMetricsData func() ([]byte, error)

	fetchClusterMetricData = fetchURLFunc(*yarnURL + yarn_api_cluster)
	if *yarnScrapeScheduler {
		fetchSchedulerMetricsData = fetchURLFunc(*yarnURL + yarn_api_scheduler)
	}

	exporter, err := NewExporter(fetchClusterMetricData, fetchSchedulerMetricsData, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(exporter)

	r := http.NewServeMux()
	r.Handle(*metricsPath, promhttp.Handler())
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`<html>
             <head><title>YARN Exporter</title></head>
             <body>
             <h1>YARN Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
			 </html>`))
		if err != nil {
			level.Error(logger).Log("msg", "Error generate page", "err", err)
		}
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

// Exporter collects YARN stats from the given API URL and exports them using
// the prometheus metrics package.
type Exporter struct {
	fetchClusterMetricsData, fetchSchedulerMetricsData func() ([]byte, error)

	up                   prometheus.Gauge
	totalScrapes         prometheus.Counter
	totalFetches         prometheus.Counter
	totalFetchesFailures prometheus.Counter

	mutex  sync.RWMutex
	logger log.Logger
}

// NewExporter returns an initialized Exporter.
func NewExporter(fetchClusterMetricData func() ([]byte, error), fetchSchedulerMetricsData func() ([]byte, error), logger log.Logger) (*Exporter, error) {
	if fetchClusterMetricData == nil && fetchSchedulerMetricsData == nil || logger == nil {
		return nil, errors.New("wrong Exporter init")
	}

	return &Exporter{
		fetchClusterMetricsData:   fetchClusterMetricData,
		fetchSchedulerMetricsData: fetchSchedulerMetricsData,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_backend_up",
			Help:      "Was the last scrape of YARN successful.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total YARN exporter scrapes.",
		}),
		totalFetches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_fetches_total",
			Help:      "Current total YARN API fetches.",
		}),
		totalFetchesFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_fetches_failures_total",
			Help:      "Current total YARN API fetches failures.",
		}),
		logger: logger,
	}, nil
}

type metricInfo struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}

func newMetric(subsystem string, metricName string, docString string, t prometheus.ValueType, variableLabels []string) metricInfo {
	return metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, metricName),
			docString,
			variableLabels,
			nil,
		),
		Type: t,
	}
}

var (
	// metrics Info

	clusterMetrics = map[string]metricInfo{
		"appsSubmitted":         newMetric("cluster", "applications_submitted", "Total applications submitted", prometheus.CounterValue, nil),
		"appsCompleted":         newMetric("cluster", "applications_completed", "Total applications completed", prometheus.CounterValue, nil),
		"appsPending":           newMetric("cluster", "applications_pending", "Applications pending", prometheus.GaugeValue, nil),
		"appsRunning":           newMetric("cluster", "applications_running", "Applications running", prometheus.GaugeValue, nil),
		"appsFailed":            newMetric("cluster", "applications_failed", "Total application failed", prometheus.CounterValue, nil),
		"appsKilled":            newMetric("cluster", "applications_killed", "Total application killed", prometheus.CounterValue, nil),
		"reservedMB":            newMetric("cluster", "memory_reserved", "Memory reserved", prometheus.GaugeValue, nil),
		"availableMB":           newMetric("cluster", "memory_available", "Memory available", prometheus.GaugeValue, nil),
		"allocatedMB":           newMetric("cluster", "memory_allocated", "Memory allocated", prometheus.GaugeValue, nil),
		"totalMB":               newMetric("cluster", "memory_total", "Total memory", prometheus.GaugeValue, nil),
		"reservedVirtualCores":  newMetric("cluster", "virtual_cores_reserved", "Virtual cores reserved", prometheus.GaugeValue, nil),
		"availableVirtualCores": newMetric("cluster", "virtual_cores_available", "Virtual cores available", prometheus.GaugeValue, nil),
		"allocatedVirtualCores": newMetric("cluster", "virtual_cores_allocated", "Virtual cores allocated", prometheus.GaugeValue, nil),
		"totalVirtualCores":     newMetric("cluster", "virtual_cores_total", "Total virtual cores", prometheus.GaugeValue, nil),
		"containersAllocated":   newMetric("cluster", "containers_allocated", "Containers allocated", prometheus.GaugeValue, nil),
		"containersReserved":    newMetric("cluster", "containers_reserved", "Containers reserved", prometheus.GaugeValue, nil),
		"containersPending":     newMetric("cluster", "containers_pending", "Containers pending", prometheus.GaugeValue, nil),
		"totalNodes":            newMetric("cluster", "nodes_total", "Nodes total", prometheus.GaugeValue, nil),
		"lostNodes":             newMetric("cluster", "nodes_lost", "Nodes lost", prometheus.GaugeValue, nil),
		"unhealthyNodes":        newMetric("cluster", "nodes_unhealthy", "Nodes unhealthy", prometheus.GaugeValue, nil),
		"decommissionedNodes":   newMetric("cluster", "nodes_decommissioned", "Nodes decommissioned", prometheus.GaugeValue, nil),
		"decommissioningNodes":  newMetric("cluster", "nodes_decommissioning", "Nodes decommissioning", prometheus.GaugeValue, nil),
		"rebootedNodes":         newMetric("cluster", "nodes_rebooted", "Nodes rebooted", prometheus.GaugeValue, nil),
		"activeNodes":           newMetric("cluster", "nodes_active", "Nodes active", prometheus.GaugeValue, nil),
	}

	schedulerQueueLabels  = []string{"queue"} // queue = queueName
	schedulerQueueMetrics = map[string]metricInfo{
		"capacity":             newMetric("queue", "capacity", "Configured queue capacity in percentage relative to its parent queue", prometheus.GaugeValue, schedulerQueueLabels),
		"usedCapacity":         newMetric("queue", "capacity_used", "Used queue capacity in percentage", prometheus.GaugeValue, schedulerQueueLabels),
		"maxCapacity":          newMetric("queue", "capacity_max", "Configured maximum queue capacity in percentage relative to its parent queue", prometheus.GaugeValue, schedulerQueueLabels),
		"absoluteCapacity":     newMetric("queue", "capacity_absolute", "Absolute capacity percentage this queue can use of entire cluster", prometheus.GaugeValue, schedulerQueueLabels),
		"absoluteUsedCapacity": newMetric("queue", "capacity_absolute_used", "Absolute used capacity percentage this queue is using of the entire cluster", prometheus.GaugeValue, schedulerQueueLabels),
		"absoluteMaxCapacity":  newMetric("queue", "capacity_absolute_max", "Absolute maximum capacity percentage this queue can use of the entire cluster", prometheus.GaugeValue, schedulerQueueLabels),
		"numApplications":      newMetric("queue", "applications", "The number of applications currently in the queue", prometheus.GaugeValue, schedulerQueueLabels),
	}
	schedulerQueueMetricsResources = map[string]metricInfo{
		"resourcesUsed_memory": newMetric("queue", "resources_memory_used", "The total amount of memory used by this queue", prometheus.GaugeValue, schedulerQueueLabels),
		"resourcesUsed_vCores": newMetric("queue", "resources_vcores_used", "The total amount of vCores used by this queue", prometheus.GaugeValue, schedulerQueueLabels),
	}
	schedulerQueueMetricsLeaf = map[string]metricInfo{
		// for type capacitySchedulerLeafQueueInfo
		"numActiveApplications":  newMetric("queue", "applications_active", "The number of active applications in this queue", prometheus.GaugeValue, schedulerQueueLabels),
		"numPendingApplications": newMetric("queue", "applications_pending", "The number of pending applications in this queue", prometheus.GaugeValue, schedulerQueueLabels),
		"numContainers":          newMetric("queue", "containers", "The number of containers being used", prometheus.GaugeValue, schedulerQueueLabels),
		"maxApplications":        newMetric("queue", "applications_max", "The maximum number of applications this queue can have", prometheus.GaugeValue, schedulerQueueLabels),
		"maxApplicationsPerUser": newMetric("queue", "applications_peruser_max", "The maximum number of applications per user this queue can have", prometheus.GaugeValue, schedulerQueueLabels),
		"userLimit":              newMetric("queue", "user_limit", "The minimum user limit percent set in the configuration", prometheus.GaugeValue, schedulerQueueLabels),
		"userLimitFactor":        newMetric("queue", "user_limitfactor", "The user limit factor set in the configuration", prometheus.GaugeValue, schedulerQueueLabels),
	}
	schedulerQueueMetricsUsers = map[string]metricInfo{
		// users
		"user_resourcesUsed_memory":   newMetric("queue", "user_memory_used", "The amount of memory used by the user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username")),
		"user_resourcesUsed_vCores":   newMetric("queue", "user_vcores_used", "The amount of vCores used by the user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username")),
		"user_numActiveApplications":  newMetric("queue", "user_applications_active", "The number of active applications for this user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username")),
		"user_numPendingApplications": newMetric("queue", "user_applications_pending", "The number of pending applications for this user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username")),
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
	ch <- e.totalFetches.Desc()
	ch <- e.totalFetchesFailures.Desc()
}

// Collect fetches the stats from configured YARN API handlers and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.totalScrapes.Inc()
	up := 0.0
	ok := true

	if e.fetchClusterMetricsData != nil {
		ok = ok && e.scrapeClusterMetrics(ch)
	}
	if e.fetchSchedulerMetricsData != nil {
		ok = ok && e.scrapeSchedulerMetrics(ch)
	}

	if ok {
		up = 1.0
	}

	ch <- prometheus.MustNewConstMetric(e.up.Desc(), prometheus.GaugeValue, up)
	ch <- e.totalScrapes
	ch <- e.totalFetches
	ch <- e.totalFetchesFailures
}

func (e *Exporter) scrapeClusterMetrics(ch chan<- prometheus.Metric) (up bool) {
	e.totalFetches.Inc()

	var data map[string]map[string]float64
	body, err := e.fetchClusterMetricsData()
	if err != nil {
		e.totalFetchesFailures.Inc()
		level.Error(e.logger).Log("msg", "Can't scrape YARN", "err", err)
		return false
	}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e.totalFetchesFailures.Inc()
		level.Error(e.logger).Log("msg", "Can't unmarshal cluster metrics", "err", err)
		return false
	}

	for name, metric := range clusterMetrics {
		ch <- prometheus.MustNewConstMetric(metric.Desc, metric.Type, data["clusterMetrics"][name])
	}

	return true
}

func (e *Exporter) scrapeSchedulerMetrics(ch chan<- prometheus.Metric) (up bool) {
	e.totalFetches.Inc()

	var data map[string]map[string]map[string]interface{}
	body, err := e.fetchSchedulerMetricsData()
	if err != nil {
		e.totalFetchesFailures.Inc()
		level.Error(e.logger).Log("msg", "Can't scrape YARN", "err", err)
		return false
	}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e.totalFetchesFailures.Inc()
		level.Error(e.logger).Log("msg", "Can't unmarshal scheduler", "err", err)
		return false
	}

	ch <- prometheus.MustNewConstMetric(
		schedulerQueueMetrics["usedCapacity"].Desc,
		schedulerQueueMetrics["usedCapacity"].Type,
		data["scheduler"]["schedulerInfo"]["usedCapacity"].(float64),
		data["scheduler"]["schedulerInfo"]["queueName"].(string),
	)

	if queues, ok := data["scheduler"]["schedulerInfo"]["queues"].(map[string]interface{}); ok {
		if queues == nil {
			e.totalFetchesFailures.Inc()
			level.Error(e.logger).Log("msg", "Empty scheduler queues", "err", err)
			return false
		}
		if queuesList, ok := queues["queue"].([]interface{}); ok {
			if len(queuesList) == 0 {
				e.totalFetchesFailures.Inc()
				level.Error(e.logger).Log("msg", "Empty scheduler queues", "err", err)
				return false
			}
			for _, q := range queuesList {
				if _, ok := q.(map[string]interface{}); ok {
					if err := e.parseQueue(q.(map[string]interface{}), ch); err != nil {
						e.totalFetchesFailures.Inc()
						level.Error(e.logger).Log("msg", "Can't unmarshal queue", "err", err)
						return false
					}
				}
			}
			return true
		}
	}
	e.totalFetchesFailures.Inc()
	level.Error(e.logger).Log("msg", "Empty scheduler queues", "err", err)
	return false
}

func (e *Exporter) parseQueue(queue map[string]interface{}, ch chan<- prometheus.Metric) (err error) {
	for name, metric := range schedulerQueueMetrics {
		ch <- prometheus.MustNewConstMetric(metric.Desc, metric.Type, queue[name].(float64), queue["queueName"].(string))
	}
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetricsResources["resourcesUsed_memory"].Desc, schedulerQueueMetricsResources["resourcesUsed_memory"].Type, queue["resourcesUsed"].(map[string]interface{})["memory"].(float64), queue["queueName"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetricsResources["resourcesUsed_vCores"].Desc, schedulerQueueMetricsResources["resourcesUsed_vCores"].Type, queue["resourcesUsed"].(map[string]interface{})["vCores"].(float64), queue["queueName"].(string))

	if _, ok := queue["type"]; ok {
		for name, metric := range schedulerQueueMetricsLeaf {
			ch <- prometheus.MustNewConstMetric(metric.Desc, metric.Type, queue[name].(float64), queue["queueName"].(string))
		}

		if users, ok := queue["users"].(map[string]interface{})["user"].([]interface{}); ok && len(users) > 0 {
			for _, u := range users {
				if user, ok := u.(map[string]interface{}); ok {
					ch <- prometheus.MustNewConstMetric(schedulerQueueMetricsUsers["user_resourcesUsed_memory"].Desc, schedulerQueueMetricsUsers["user_resourcesUsed_memory"].Type,
						user["resourcesUsed"].(map[string]interface{})["memory"].(float64), queue["queueName"].(string), user["username"].(string))
					ch <- prometheus.MustNewConstMetric(schedulerQueueMetricsUsers["user_resourcesUsed_vCores"].Desc, schedulerQueueMetricsUsers["user_resourcesUsed_vCores"].Type,
						user["resourcesUsed"].(map[string]interface{})["vCores"].(float64), queue["queueName"].(string), user["username"].(string))
					ch <- prometheus.MustNewConstMetric(schedulerQueueMetricsUsers["user_numActiveApplications"].Desc, schedulerQueueMetricsUsers["user_numActiveApplications"].Type,
						user["numActiveApplications"].(float64), queue["queueName"].(string), user["username"].(string))
					ch <- prometheus.MustNewConstMetric(schedulerQueueMetricsUsers["user_numPendingApplications"].Desc, schedulerQueueMetricsUsers["user_numPendingApplications"].Type,
						user["numPendingApplications"].(float64), queue["queueName"].(string), user["username"].(string))
				}
			}
		}
	}

	if queues, ok := queue["queues"].(map[string]interface{}); ok {
		if queues == nil {
			return errors.New("queues in " + queue["queueName"].(string) + " is empty")
		}
		if queuesList, ok := queues["queue"].([]interface{}); ok {
			if len(queuesList) == 0 {
				return errors.New("queues in " + queue["queueName"].(string) + " is empty")
			}
			for _, q := range queuesList {
				if _, ok := q.(map[string]interface{}); ok {
					if err := e.parseQueue(q.(map[string]interface{}), ch); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func fetchURLFunc(target string) func() ([]byte, error) {
	return func() ([]byte, error) {
		targetURL, err := url.Parse(target)
		if err != nil {
			return nil, err
		}
		tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: !*yarnSSLVerify}}
		client := http.Client{
			Timeout:   *yarnTimeout,
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
}
