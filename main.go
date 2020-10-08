package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/laoleesch/yarn-exporter/pkg/collector"
	"github.com/laoleesch/yarn-exporter/pkg/yarn"
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
	namespace        = "yarn"
	yarnAPICluster   = "/ws/v1/cluster/metrics"
	yarnAPIScheduler = "/ws/v1/cluster/scheduler"
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

	var clusterMetrics yarn.ClusterMetricsAPI
	ce, err := collector.NewExporter(namespace, &clusterMetrics, fetchURLFunc(*yarnURL+yarnAPICluster), logger)
	if err != nil {
		level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(ce)

	if *yarnScrapeScheduler {
		var schedulerMetrics yarn.SchedulerMetricsAPI
		se, err := collector.NewExporter(namespace, &schedulerMetrics, fetchURLFunc(*yarnURL+yarnAPIScheduler), logger)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
			os.Exit(1)
		}
		prometheus.MustRegister(se)
	}

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
