package yarn

import (
	"encoding/json"

	"github.com/laoleesch/yarn-exporter/pkg/collector"
	"github.com/prometheus/client_golang/prometheus"
)

// SchedulerMetricsAPI JSON
type SchedulerMetricsAPI struct {
	MetricsInfo map[string]collector.MetricInfo

	Scheduler SchedulerDTO `json:"scheduler"`
}

type SchedulerDTO struct {
	SchedulerInfo SchedulerInfoDTO `json:"schedulerInfo"`
}

type SchedulerInfoDTO struct {
	Type         string                     `json:"type"`
	Capacity     float64                    `json:"capacity"`
	UsedCapacity float64                    `json:"usedCapacity"`
	MaxCapacity  float64                    `json:"maxCapacity"`
	QueueName    string                     `json:"queueName"`
	Queues       *SchedulerMetricsQueuesDTO `json:"queues,omitempty"`
}

type SchedulerMetricsQueuesDTO struct {
	Queue []SchedulerMetricsQueueDTO `json:"queue"`
}

type SchedulerMetricsQueueDTO struct {
	Capacity             float64                          `json:"capacity"`
	UsedCapacity         float64                          `json:"usedCapacity"`
	MaxCapacity          float64                          `json:"maxCapacity"`
	AbsoluteCapacity     float64                          `json:"absoluteCapacity"`
	AbsoluteUsedCapacity float64                          `json:"absoluteUsedCapacity"`
	AbsoluteMaxCapacity  float64                          `json:"absoluteMaxCapacity"`
	NumApplications      float64                          `json:"numApplications"`
	UsedResources        string                           `json:"usedResources"`
	QueueName            string                           `json:"queueName"`
	State                string                           `json:"state"`
	Queues               *SchedulerMetricsQueuesDTO       `json:"queues,omitempty"`
	ResourcesUsed        SchedulerMetricsResourcesUsedDTO `json:"resourcesUsed"`
	// Elements of the queues object for a Leaf queue - contains all elements in parent plus the following
	Type                         string                    `json:"type,omitempty"`
	NumActiveApplications        float64                   `json:"numActiveApplications,omitempty"`
	NumPendingApplications       float64                   `json:"numPendingApplications,omitempty"`
	NumContainers                float64                   `json:"numContainers,omitempty"`
	MaxApplications              float64                   `json:"maxApplications,omitempty"`
	MaxApplicationsPerUser       float64                   `json:"maxApplicationsPerUser,omitempty"`
	MaxActiveApplications        float64                   `json:"maxActiveApplications,omitempty"`
	MaxActiveApplicationsPerUser float64                   `json:"maxActiveApplicationsPerUser,omitempty"`
	UserLimit                    float64                   `json:"userLimit,omitempty"`
	UserLimitFactor              float64                   `json:"userLimitFactor,omitempty"`
	Users                        *SchedulerMetricsUsersDTO `json:"users,omitempty"`
}

type SchedulerMetricsResourcesUsedDTO struct {
	Memory float64 `json:"memory"`
	VCores float64 `json:"vCores"`
}

type SchedulerMetricsUsersDTO struct {
	User []SchedulerMetricsUserDTO `json:"user"`
}

type SchedulerMetricsUserDTO struct {
	Username               string                           `json:"username"`
	ResourcesUsed          SchedulerMetricsResourcesUsedDTO `json:"resourcesUsed"`
	NumActiveApplications  float64                          `json:"numActiveApplications"`
	NumPendingApplications float64                          `json:"numPendingApplications"`
}

// Subsystem impl APIMeter.Subsystem
func (a *SchedulerMetricsAPI) Subsystem() string {
	return "queue"
}

// DefMetricsInfo impl APIMeter.DefMetricsInfo
func (a *SchedulerMetricsAPI) DefMetricsInfo(n string) map[string]collector.MetricInfo {
	s := a.Subsystem()
	labels := []string{"queue"}
	a.MetricsInfo = map[string]collector.MetricInfo{
		"capacity":             collector.NewMetric(n, s, "capacity", "Configured queue capacity in percentage relative to its parent queue", prometheus.GaugeValue, labels),
		"usedCapacity":         collector.NewMetric(n, s, "capacity_used", "Used queue capacity in percentage", prometheus.GaugeValue, labels),
		"maxCapacity":          collector.NewMetric(n, s, "capacity_max", "Configured maximum queue capacity in percentage relative to its parent queue", prometheus.GaugeValue, labels),
		"absoluteCapacity":     collector.NewMetric(n, s, "capacity_absolute", "Absolute capacity percentage this queue can use of entire cluster", prometheus.GaugeValue, labels),
		"absoluteUsedCapacity": collector.NewMetric(n, s, "capacity_absolute_used", "Absolute used capacity percentage this queue is using of the entire cluster", prometheus.GaugeValue, labels),
		"absoluteMaxCapacity":  collector.NewMetric(n, s, "capacity_absolute_max", "Absolute maximum capacity percentage this queue can use of the entire cluster", prometheus.GaugeValue, labels),
		"numApplications":      collector.NewMetric(n, s, "applications", "The number of applications currently in the queue", prometheus.GaugeValue, labels),
		"resourcesUsed_memory": collector.NewMetric(n, s, "resources_memory_used", "The total amount of memory used by this queue", prometheus.GaugeValue, labels),
		"resourcesUsed_vCores": collector.NewMetric(n, s, "resources_vcores_used", "The total amount of vCores used by this queue", prometheus.GaugeValue, labels),
		// for type capacitySchedulerLeafQueueInfo
		"numActiveApplications":  collector.NewMetric(n, s, "applications_active", "The number of active applications in this queue", prometheus.GaugeValue, labels),
		"numPendingApplications": collector.NewMetric(n, s, "applications_pending", "The number of pending applications in this queue", prometheus.GaugeValue, labels),
		"numContainers":          collector.NewMetric(n, s, "containers", "The number of containers being used", prometheus.GaugeValue, labels),
		"maxApplications":        collector.NewMetric(n, s, "applications_max", "The maximum number of applications this queue can have", prometheus.GaugeValue, labels),
		"maxApplicationsPerUser": collector.NewMetric(n, s, "applications_peruser_max", "The maximum number of applications per user this queue can have", prometheus.GaugeValue, labels),
		"userLimit":              collector.NewMetric(n, s, "user_limit", "The minimum user limit percent set in the configuration", prometheus.GaugeValue, labels),
		"userLimitFactor":        collector.NewMetric(n, s, "user_limitfactor", "The user limit factor set in the configuration", prometheus.GaugeValue, labels),
		// users
		"user_resourcesUsed_memory":   collector.NewMetric(n, s, "user_memory_used", "The amount of memory used by the user in this queue", prometheus.GaugeValue, append(labels, "username")),
		"user_resourcesUsed_vCores":   collector.NewMetric(n, s, "user_vcores_used", "The amount of vCores used by the user in this queue", prometheus.GaugeValue, append(labels, "username")),
		"user_numActiveApplications":  collector.NewMetric(n, s, "user_applications_active", "The number of active applications for this user in this queue", prometheus.GaugeValue, append(labels, "username")),
		"user_numPendingApplications": collector.NewMetric(n, s, "user_applications_pending", "The number of pending applications for this user in this queue", prometheus.GaugeValue, append(labels, "username")),
	}
	return a.MetricsInfo
}

// Scrape impl APIMeter.Scrape
func (a *SchedulerMetricsAPI) Scrape(ch chan<- prometheus.Metric, getDataFunc func() ([]byte, error)) error {

	body, err := getDataFunc()
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &a)
	if err != nil {
		return err
	}

	// metrics

	ch <- a.metric("usedCapacity", a.Scheduler.SchedulerInfo.UsedCapacity, a.Scheduler.SchedulerInfo.QueueName)

	if a.Scheduler.SchedulerInfo.Queues != nil {
		for _, q := range a.Scheduler.SchedulerInfo.Queues.Queue {
			if err = a.parseQueue(q, ch); err != nil {
				return err
			}
		}
	}

	return nil
}

// helpers
func (a *SchedulerMetricsAPI) metric(name string, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(a.MetricsInfo[name].Desc, a.MetricsInfo[name].Type, value, labels...)
}

func (a *SchedulerMetricsAPI) parseQueue(q SchedulerMetricsQueueDTO, ch chan<- prometheus.Metric) error {

	ch <- a.metric("capacity", q.Capacity, q.QueueName)
	ch <- a.metric("usedCapacity", q.UsedCapacity, q.QueueName)
	ch <- a.metric("maxCapacity", q.MaxCapacity, q.QueueName)
	ch <- a.metric("absoluteCapacity", q.AbsoluteCapacity, q.QueueName)
	ch <- a.metric("absoluteUsedCapacity", q.AbsoluteUsedCapacity, q.QueueName)
	ch <- a.metric("absoluteMaxCapacity", q.AbsoluteMaxCapacity, q.QueueName)
	ch <- a.metric("numApplications", q.NumApplications, q.QueueName)

	ch <- a.metric("resourcesUsed_memory", q.ResourcesUsed.Memory, q.QueueName)
	ch <- a.metric("resourcesUsed_vCores", q.ResourcesUsed.VCores, q.QueueName)

	if q.Type != "" {
		ch <- a.metric("numActiveApplications", q.NumActiveApplications, q.QueueName)
		ch <- a.metric("numPendingApplications", q.NumPendingApplications, q.QueueName)
		ch <- a.metric("numContainers", q.NumContainers, q.QueueName)
		ch <- a.metric("maxApplications", q.MaxApplications, q.QueueName)
		ch <- a.metric("maxApplicationsPerUser", q.MaxActiveApplicationsPerUser, q.QueueName)
		ch <- a.metric("userLimit", q.UserLimit, q.QueueName)
		ch <- a.metric("userLimitFactor", q.UserLimitFactor, q.QueueName)
	}

	if q.Users != nil {
		for _, u := range q.Users.User {
			ch <- a.metric("user_resourcesUsed_memory", u.ResourcesUsed.Memory, q.QueueName, u.Username)
			ch <- a.metric("user_resourcesUsed_vCores", u.ResourcesUsed.VCores, q.QueueName, u.Username)
			ch <- a.metric("user_numActiveApplications", u.NumActiveApplications, q.QueueName, u.Username)
			ch <- a.metric("user_numPendingApplications", u.NumPendingApplications, q.QueueName, u.Username)
		}
	}

	if q.Queues != nil {
		for _, sq := range q.Queues.Queue {
			if err := a.parseQueue(sq, ch); err != nil {
				return err
			}
		}
	}

	return nil
}
