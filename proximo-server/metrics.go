package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Counters struct holds various counters needed for app metrics.
type counters struct {
	ErrorCounter           prometheus.Counter
	SinkMessagesCounter    *prometheus.CounterVec
	SourcedMessagesCounter *prometheus.CounterVec
}

// NewCounters returns metric counters needed for the app.
func NewCounters() counters {
	c := counters{
		ErrorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "errors_total",
			Help: "A counter of the number of errors",
		}),
		SinkMessagesCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "messages_sinked_total",
				Help: "How many messages got sinked",
			},
			[]string{"topic"},
		),
		SourcedMessagesCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "messages_consumed_total",
				Help: "How many messages got consumed",
			},
			[]string{"topic"},
		),
	}
	prometheus.DefaultRegisterer.MustRegister(
		c.ErrorCounter,
		c.SinkMessagesCounter,
		c.SourcedMessagesCounter,
	)
	return c
}
