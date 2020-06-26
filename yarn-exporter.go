package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// args
	listenAddress         = kingpin.Flag("web.listen-address", "Address to listen on.").Default(":9911").String()
	metricsURI            = kingpin.Flag("web.metrics-uri", "Path under which to expose metrics.").Default("/metrics").String()
	yarnURL               = kingpin.Flag("yarn.url", "URL on which to scrape yarn.").Default("http://localhost:8088").String()
	yarnSSLVerify         = kingpin.Flag("yarn.ssl-verify", "Flag that enables SSL certificate verification for the scrape URI").Default("false").Bool()
	yarnScrapeScheduler   = kingpin.Flag("yarn.scrape-scheduler", "Flag that enables scheduler metrics /ws/v1/cluster/scheduler").Default("false").Bool()
	yarnScrapeAppsRunning = kingpin.Flag("yarn.scrape-apps-running", "Flag that enables running apps metrics /ws/v1/cluster/apps?state=RUNNING").Default("false").Bool()
	yarnTimeout           = kingpin.Flag("yarn.timeout", "Timeout for trying to get stats from yarn.").Default("3s").Duration()
)

func main() {
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)
	level.Info(logger).Log("msg", "Running yarn-exporter", "version", version.Info())
	level.Info(logger).Log("msg", "Build context", "context", version.BuildContext())
	level.Info(logger).Log("msg", "Listening on ", "address", *listenAddress)

	// exporter, err := NewExporter(*yarnURL, *yarnSSLVerify, *yarnScrapeScheduler, *yarnScrapeAppsRunning, *yarnTimeout, logger)
	// if err != nil {
	// 	level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
	// 	os.Exit(1)
	// }
	// prometheus.MustRegister(exporter)
	// prometheus.MustRegister(version.NewCollector("yarn-exporter"))

	r := http.NewServeMux()
	r.Handle(*metricsURI, promhttp.Handler())
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>YARN Exporter</title></head>
             <body>
             <h1>YARN Exporter</h1>
             <p><a href='` + *metricsURI + `'>Metrics</a></p>
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
			level.Error(logger).Log("msg", "main http listener error", "err", err)
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
