package yarn

import (
	"encoding/json"

	"github.com/laoleesch/yarn-exporter/pkg/collector"
	"github.com/prometheus/client_golang/prometheus"
)

// ClusterMetricsAPI JSON
type ClusterMetricsAPI struct {
	MetricsInfo map[string]collector.MetricInfo

	ClusterMetrics ClusterMetricsDTO `json:"clusterMetrics"`
}

type ClusterMetricsDTO struct {
	AppsSubmitted         float64 `json:"appsSubmitted"`
	AppsCompleted         float64 `json:"appsCompleted"`
	AppsPending           float64 `json:"appsPending"`
	AppsRunning           float64 `json:"appsRunning"`
	AppsFailed            float64 `json:"appsFailed"`
	AppsKilled            float64 `json:"appsKilled"`
	ReservedMB            float64 `json:"reservedMB"`
	AvailableMB           float64 `json:"availableMB"`
	AllocatedMB           float64 `json:"allocatedMB"`
	TotalMB               float64 `json:"totalMB"`
	ReservedVirtualCores  float64 `json:"reservedVirtualCores"`
	AvailableVirtualCores float64 `json:"availableVirtualCores"`
	AllocatedVirtualCores float64 `json:"allocatedVirtualCores"`
	TotalVirtualCores     float64 `json:"totalVirtualCores"`
	ContainersAllocated   float64 `json:"containersAllocated"`
	ContainersReserved    float64 `json:"containersReserved"`
	ContainersPending     float64 `json:"containersPending"`
	TotalNodes            float64 `json:"totalNodes"`
	LostNodes             float64 `json:"lostNodes"`
	UnhealthyNodes        float64 `json:"unhealthyNodes"`
	DecommissionedNodes   float64 `json:"decommissionedNodes"`
	DecommissioningNodes  float64 `json:"decommissioningNodes"`
	RebootedNodes         float64 `json:"rebootedNodes"`
	ActiveNodes           float64 `json:"activeNodes"`
}

// Subsystem impl APIMeter.Subsystem
func (a *ClusterMetricsAPI) Subsystem() string {
	return "cluster"
}

// DefMetricsInfo impl APIMeter.DefMetricsInfo
func (a *ClusterMetricsAPI) DefMetricsInfo(n string) map[string]collector.MetricInfo {
	s := a.Subsystem()
	variableLabels := []string{}
	a.MetricsInfo = map[string]collector.MetricInfo{
		"appsSubmitted":         collector.NewMetric(n, s, "applications_submitted", "Total applications submitted", prometheus.CounterValue, variableLabels),
		"appsCompleted":         collector.NewMetric(n, s, "applications_completed", "Total applications completed", prometheus.CounterValue, variableLabels),
		"appsPending":           collector.NewMetric(n, s, "applications_pending", "Applications pending", prometheus.GaugeValue, variableLabels),
		"appsRunning":           collector.NewMetric(n, s, "applications_running", "Applications running", prometheus.GaugeValue, variableLabels),
		"appsFailed":            collector.NewMetric(n, s, "applications_failed", "Total application failed", prometheus.CounterValue, variableLabels),
		"appsKilled":            collector.NewMetric(n, s, "applications_killed", "Total application killed", prometheus.CounterValue, variableLabels),
		"reservedMB":            collector.NewMetric(n, s, "memory_reserved", "Memory reserved", prometheus.GaugeValue, variableLabels),
		"availableMB":           collector.NewMetric(n, s, "memory_available", "Memory available", prometheus.GaugeValue, variableLabels),
		"allocatedMB":           collector.NewMetric(n, s, "memory_allocated", "Memory allocated", prometheus.GaugeValue, variableLabels),
		"totalMB":               collector.NewMetric(n, s, "memory_total", "Total memory", prometheus.GaugeValue, variableLabels),
		"reservedVirtualCores":  collector.NewMetric(n, s, "virtual_cores_reserved", "Virtual cores reserved", prometheus.GaugeValue, variableLabels),
		"availableVirtualCores": collector.NewMetric(n, s, "virtual_cores_available", "Virtual cores available", prometheus.GaugeValue, variableLabels),
		"allocatedVirtualCores": collector.NewMetric(n, s, "virtual_cores_allocated", "Virtual cores allocated", prometheus.GaugeValue, variableLabels),
		"totalVirtualCores":     collector.NewMetric(n, s, "virtual_cores_total", "Total virtual cores", prometheus.GaugeValue, variableLabels),
		"containersAllocated":   collector.NewMetric(n, s, "containers_allocated", "Containers allocated", prometheus.GaugeValue, variableLabels),
		"containersReserved":    collector.NewMetric(n, s, "containers_reserved", "Containers reserved", prometheus.GaugeValue, variableLabels),
		"containersPending":     collector.NewMetric(n, s, "containers_pending", "Containers pending", prometheus.GaugeValue, variableLabels),
		"totalNodes":            collector.NewMetric(n, s, "nodes_total", "Nodes total", prometheus.GaugeValue, variableLabels),
		"lostNodes":             collector.NewMetric(n, s, "nodes_lost", "Nodes lost", prometheus.GaugeValue, variableLabels),
		"unhealthyNodes":        collector.NewMetric(n, s, "nodes_unhealthy", "Nodes unhealthy", prometheus.GaugeValue, variableLabels),
		"decommissionedNodes":   collector.NewMetric(n, s, "nodes_decommissioned", "Nodes decommissioned", prometheus.GaugeValue, variableLabels),
		"decommissioningNodes":  collector.NewMetric(n, s, "nodes_decommissioning", "Nodes decommissioning", prometheus.GaugeValue, variableLabels),
		"rebootedNodes":         collector.NewMetric(n, s, "nodes_rebooted", "Nodes rebooted", prometheus.GaugeValue, variableLabels),
		"activeNodes":           collector.NewMetric(n, s, "nodes_active", "Nodes active", prometheus.GaugeValue, variableLabels),
	}
	return a.MetricsInfo
}

// Scrape impl APIMeter.Scrape
func (a *ClusterMetricsAPI) Scrape(ch chan<- prometheus.Metric, getDataFunc func() ([]byte, error)) error {

	body, err := getDataFunc()
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &a)
	if err != nil {
		return err
	}

	// metrics

	ch <- a.metric("appsSubmitted", a.ClusterMetrics.AppsSubmitted)
	ch <- a.metric("appsCompleted", a.ClusterMetrics.AppsCompleted)
	ch <- a.metric("appsPending", a.ClusterMetrics.AppsPending)
	ch <- a.metric("appsRunning", a.ClusterMetrics.AppsRunning)
	ch <- a.metric("appsFailed", a.ClusterMetrics.AppsFailed)
	ch <- a.metric("appsKilled", a.ClusterMetrics.AppsKilled)
	ch <- a.metric("reservedMB", a.ClusterMetrics.ReservedMB)
	ch <- a.metric("availableMB", a.ClusterMetrics.AvailableMB)
	ch <- a.metric("allocatedMB", a.ClusterMetrics.AllocatedMB)
	ch <- a.metric("totalMB", a.ClusterMetrics.TotalMB)
	ch <- a.metric("reservedVirtualCores", a.ClusterMetrics.ReservedVirtualCores)
	ch <- a.metric("availableVirtualCores", a.ClusterMetrics.AvailableVirtualCores)
	ch <- a.metric("allocatedVirtualCores", a.ClusterMetrics.AllocatedVirtualCores)
	ch <- a.metric("totalVirtualCores", a.ClusterMetrics.TotalVirtualCores)
	ch <- a.metric("containersAllocated", a.ClusterMetrics.ContainersAllocated)
	ch <- a.metric("containersReserved", a.ClusterMetrics.ContainersReserved)
	ch <- a.metric("containersPending", a.ClusterMetrics.ContainersPending)
	ch <- a.metric("totalNodes", a.ClusterMetrics.TotalNodes)
	ch <- a.metric("lostNodes", a.ClusterMetrics.LostNodes)
	ch <- a.metric("unhealthyNodes", a.ClusterMetrics.UnhealthyNodes)
	ch <- a.metric("decommissionedNodes", a.ClusterMetrics.DecommissionedNodes)
	ch <- a.metric("decommissioningNodes", a.ClusterMetrics.DecommissioningNodes)
	ch <- a.metric("rebootedNodes", a.ClusterMetrics.RebootedNodes)
	ch <- a.metric("activeNodes", a.ClusterMetrics.ActiveNodes)

	return nil
}

// helper
func (a *ClusterMetricsAPI) metric(name string, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(a.MetricsInfo[name].Desc, a.MetricsInfo[name].Type, value, labels...)
}
