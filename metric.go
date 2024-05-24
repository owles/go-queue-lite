package go_queue_lite

import (
	"sync"
	"time"
)

type Metrics struct {
	processed       int64
	errors          int64
	totalProcessing time.Duration
	startTime       time.Time
	mu              sync.Mutex
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

func (m *Metrics) IncProcessed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed++
}

func (m *Metrics) IncErrors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors++
}

func (m *Metrics) RecordProcessingTime(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalProcessing += duration
}

func (m *Metrics) GetProcessed() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processed
}

func (m *Metrics) GetErrors() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.errors
}

func (m *Metrics) GetAverageProcessingTime() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processed == 0 {
		return 0
	}
	return m.totalProcessing / time.Duration(m.processed)
}

func (m *Metrics) GetProcessingRate(interval time.Duration) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if interval == 0 {
		return 0
	}
	return float64(m.processed) / interval.Seconds()
}

func (m *Metrics) GetTasksPerSecond() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.processed) / elapsed
}

func (m *Metrics) GetTasksPerMinute() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	elapsed := time.Since(m.startTime).Minutes()
	if elapsed == 0 {
		return 0
	}
	return float64(m.processed) / elapsed
}

func (m *Metrics) GetTasksPerDay() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	elapsed := time.Since(m.startTime).Hours() / 24
	if elapsed == 0 {
		return 0
	}
	return float64(m.processed) / elapsed
}
