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
	namespace          = "yarn"
	yarn_api_cluster   = "/ws/v1/cluster/metrics"
	yarn_api_scheduler = "/ws/v1/cluster/scheduler"
)

// Exporter collects YARN stats from the given API URL and exports them using
// the prometheus metrics package.
type Exporter struct {
	URL                                *url.URL
	scrapeScheduler, scrapeAppsRunning bool
	timeout                            time.Duration

	up                   prometheus.Gauge
	totalScrapes         prometheus.Counter
	totalFetches         prometheus.Counter
	totalFetchesFailures prometheus.Counter

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

func newMetric(subsystem string, metricName string, docString string, t prometheus.ValueType, variableLabels []string, constLabels prometheus.Labels) metricInfo {
	return metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, metricName),
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
		"appsSubmitted":         newMetric("cluster", "applications_submitted", "Total applications submitted", prometheus.CounterValue, nil, nil),
		"appsCompleted":         newMetric("cluster", "applications_completed", "Total applications completed", prometheus.CounterValue, nil, nil),
		"appsPending":           newMetric("cluster", "applications_pending", "Applications pending", prometheus.GaugeValue, nil, nil),
		"appsRunning":           newMetric("cluster", "applications_running", "Applications running", prometheus.GaugeValue, nil, nil),
		"appsFailed":            newMetric("cluster", "applications_failed", "Total application failed", prometheus.CounterValue, nil, nil),
		"appsKilled":            newMetric("cluster", "applications_killed", "Total application killed", prometheus.CounterValue, nil, nil),
		"reservedMB":            newMetric("cluster", "memory_reserved", "Memory reserved", prometheus.GaugeValue, nil, nil),
		"availableMB":           newMetric("cluster", "memory_available", "Memory available", prometheus.GaugeValue, nil, nil),
		"allocatedMB":           newMetric("cluster", "memory_allocated", "Memory allocated", prometheus.GaugeValue, nil, nil),
		"totalMB":               newMetric("cluster", "memory_total", "Total memory", prometheus.GaugeValue, nil, nil),
		"reservedVirtualCores":  newMetric("cluster", "virtual_cores_reserved", "Virtual cores reserved", prometheus.GaugeValue, nil, nil),
		"availableVirtualCores": newMetric("cluster", "virtual_cores_available", "Virtual cores available", prometheus.GaugeValue, nil, nil),
		"allocatedVirtualCores": newMetric("cluster", "virtual_cores_allocated", "Virtual cores allocated", prometheus.GaugeValue, nil, nil),
		"totalVirtualCores":     newMetric("cluster", "virtual_cores_total", "Total virtual cores", prometheus.GaugeValue, nil, nil),
		"containersAllocated":   newMetric("cluster", "containers_allocated", "Containers allocated", prometheus.GaugeValue, nil, nil),
		"containersReserved":    newMetric("cluster", "containers_reserved", "Containers reserved", prometheus.GaugeValue, nil, nil),
		"containersPending":     newMetric("cluster", "containers_pending", "Containers pending", prometheus.GaugeValue, nil, nil),
		"totalNodes":            newMetric("cluster", "nodes_total", "Nodes total", prometheus.GaugeValue, nil, nil),
		"lostNodes":             newMetric("cluster", "nodes_lost", "Nodes lost", prometheus.GaugeValue, nil, nil),
		"unhealthyNodes":        newMetric("cluster", "nodes_unhealthy", "Nodes unhealthy", prometheus.GaugeValue, nil, nil),
		"decommissionedNodes":   newMetric("cluster", "nodes_decommissioned", "Nodes decommissioned", prometheus.GaugeValue, nil, nil),
		"decommissioningNodes":  newMetric("cluster", "nodes_decommissioning", "Nodes decommissioning", prometheus.GaugeValue, nil, nil),
		"rebootedNodes":         newMetric("cluster", "nodes_rebooted", "Nodes rebooted", prometheus.GaugeValue, nil, nil),
		"activeNodes":           newMetric("cluster", "nodes_active", "Nodes active", prometheus.GaugeValue, nil, nil),
	}

	schedulerQueueLabels  = []string{"queue", "state"} // queue = queueName
	schedulerQueueMetrics = map[string]metricInfo{
		"capacity":             newMetric("scheduler", "capacity", "Configured queue capacity in percentage relative to its parent queue", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"usedCapacity":         newMetric("scheduler", "capacity_used", "Used queue capacity in percentage", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"maxCapacity":          newMetric("scheduler", "capacity_max", "Configured maximum queue capacity in percentage relative to its parent queue", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"absoluteCapacity":     newMetric("scheduler", "capacity_absolute", "Absolute capacity percentage this queue can use of entire cluster", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"absoluteUsedCapacity": newMetric("scheduler", "capacity_absolute_used", "Absolute used capacity percentage this queue is using of the entire cluster", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"absoluteMaxCapacity":  newMetric("scheduler", "capacity_absolute_max", "Absolute maximum capacity percentage this queue can use of the entire cluster", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"numApplications":      newMetric("scheduler", "applications", "The number of applications currently in the queue", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"resourcesUsed_memory": newMetric("scheduler", "resources_memory_used", "The total amount of memory used by this queue", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"resourcesUsed_vCores": newMetric("scheduler", "resources_vcores_used", "The total amount of vCores used by this queue", prometheus.GaugeValue, schedulerQueueLabels, nil),
		// for type capacitySchedulerLeafQueueInfo
		"numActiveApplications":  newMetric("scheduler", "applications_active", "The number of active applications in this queue", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"numPendingApplications": newMetric("scheduler", "applications_pending", "The number of pending applications in this queue", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"numContainers":          newMetric("scheduler", "containers", "The number of containers being used", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"maxApplications":        newMetric("scheduler", "applications_max", "The maximum number of applications this queue can have", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"maxApplicationsPerUser": newMetric("scheduler", "applications_peruser_max", "The maximum number of applications per user this queue can have", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"userLimit":              newMetric("scheduler", "user_limit", "The minimum user limit percent set in the configuration", prometheus.GaugeValue, schedulerQueueLabels, nil),
		"userLimitFactor":        newMetric("scheduler", "user_limitfactor", "The user limit factor set in the configuration", prometheus.GaugeValue, schedulerQueueLabels, nil),
		// users
		"user_resourcesUsed_memory":   newMetric("scheduler", "user_memory_used", "The amount of memory used by the user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username"), nil),
		"user_resourcesUsed_vCores":   newMetric("scheduler", "user_vcores_used", "The amount of vCores used by the user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username"), nil),
		"user_numActiveApplications":  newMetric("scheduler", "user_applications_active", "The number of active applications for this user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username"), nil),
		"user_numPendingApplications": newMetric("scheduler", "user_applications_pending", "The number of pending applications for this user in this queue", prometheus.GaugeValue, append(schedulerQueueLabels, "username"), nil),
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
	if e.scrapeClusterMetrics(ch) && e.scrapeSchedulerMetrics(ch) {
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
	body, err := e.fetchPath(yarn_api_cluster)
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
	body, err := e.fetchPath(yarn_api_scheduler)
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
		"RUNNING",
	)

	queues := data["scheduler"]["schedulerInfo"]["queues"].(map[string]interface{})["queue"].([]interface{})
	for _, q := range queues {
		parseQueue(q.(map[string]interface{}), ch)
	}

	return true
}

func parseQueue(queue map[string]interface{}, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["capacity"].Desc, schedulerQueueMetrics["capacity"].Type, queue["capacity"].(float64), queue["queueName"].(string), queue["state"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["usedCapacity"].Desc, schedulerQueueMetrics["usedCapacity"].Type, queue["usedCapacity"].(float64), queue["queueName"].(string), queue["state"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["maxCapacity"].Desc, schedulerQueueMetrics["maxCapacity"].Type, queue["maxCapacity"].(float64), queue["queueName"].(string), queue["state"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["absoluteCapacity"].Desc, schedulerQueueMetrics["absoluteCapacity"].Type, queue["absoluteCapacity"].(float64), queue["queueName"].(string), queue["state"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["absoluteMaxCapacity"].Desc, schedulerQueueMetrics["absoluteMaxCapacity"].Type, queue["absoluteMaxCapacity"].(float64), queue["queueName"].(string), queue["state"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["absoluteUsedCapacity"].Desc, schedulerQueueMetrics["absoluteUsedCapacity"].Type, queue["absoluteUsedCapacity"].(float64), queue["queueName"].(string), queue["state"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["numApplications"].Desc, schedulerQueueMetrics["numApplications"].Type, queue["numApplications"].(float64), queue["queueName"].(string), queue["state"].(string))

	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["resourcesUsed_memory"].Desc, schedulerQueueMetrics["resourcesUsed_memory"].Type, queue["resourcesUsed"].(map[string]interface{})["memory"].(float64), queue["queueName"].(string), queue["state"].(string))
	ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["resourcesUsed_vCores"].Desc, schedulerQueueMetrics["resourcesUsed_vCores"].Type, queue["resourcesUsed"].(map[string]interface{})["vCores"].(float64), queue["queueName"].(string), queue["state"].(string))

	if _, ok := queue["type"]; ok {
		ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["numActiveApplications"].Desc, schedulerQueueMetrics["numActiveApplications"].Type, queue["numActiveApplications"].(float64), queue["queueName"].(string), queue["state"].(string))
		ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["numPendingApplications"].Desc, schedulerQueueMetrics["numPendingApplications"].Type, queue["numPendingApplications"].(float64), queue["queueName"].(string), queue["state"].(string))
		ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["numContainers"].Desc, schedulerQueueMetrics["numContainers"].Type, queue["numContainers"].(float64), queue["queueName"].(string), queue["state"].(string))
		ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["maxApplications"].Desc, schedulerQueueMetrics["maxApplications"].Type, queue["maxApplications"].(float64), queue["queueName"].(string), queue["state"].(string))
		ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["maxApplicationsPerUser"].Desc, schedulerQueueMetrics["maxApplicationsPerUser"].Type, queue["maxApplicationsPerUser"].(float64), queue["queueName"].(string), queue["state"].(string))
		ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["userLimit"].Desc, schedulerQueueMetrics["userLimit"].Type, queue["userLimit"].(float64), queue["queueName"].(string), queue["state"].(string))
		ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["userLimitFactor"].Desc, schedulerQueueMetrics["userLimitFactor"].Type, queue["userLimitFactor"].(float64), queue["queueName"].(string), queue["state"].(string))

		users := queue["users"].(map[string]interface{})["user"].([]interface{})
		for _, u := range users {
			user := u.(map[string]interface{})
			ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["user_resourcesUsed_memory"].Desc, schedulerQueueMetrics["user_resourcesUsed_memory"].Type,
				user["resourcesUsed"].(map[string]interface{})["memory"].(float64), queue["queueName"].(string), queue["state"].(string), user["username"].(string))
			ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["user_resourcesUsed_vCores"].Desc, schedulerQueueMetrics["user_resourcesUsed_vCores"].Type,
				user["resourcesUsed"].(map[string]interface{})["vCores"].(float64), queue["queueName"].(string), queue["state"].(string), user["username"].(string))
			ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["user_numActiveApplications"].Desc, schedulerQueueMetrics["user_numActiveApplications"].Type,
				user["numActiveApplications"].(float64), queue["queueName"].(string), queue["state"].(string), user["username"].(string))
			ch <- prometheus.MustNewConstMetric(schedulerQueueMetrics["user_numPendingApplications"].Desc, schedulerQueueMetrics["user_numPendingApplications"].Type,
				user["numPendingApplications"].(float64), queue["queueName"].(string), queue["state"].(string), user["username"].(string))
		}
	}

	if _, ok := queue["queues"]; ok {
		queues := queue["queues"].(map[string]interface{})["queue"].([]interface{})
		for _, q := range queues {
			parseQueue(q.(map[string]interface{}), ch)
		}
	}

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