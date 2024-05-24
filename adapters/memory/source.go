package memory

import (
	"go-queue-lite/core"
	"sort"
	"sync"
	"time"
)

type Source struct {
	mu    sync.Mutex
	jobs  map[string]*core.Model
	queue map[string][]*core.Model
}

func NewMemorySource() *Source {
	return &Source{
		jobs:  make(map[string]*core.Model),
		queue: make(map[string][]*core.Model),
	}
}

func (m *Source) ResetPending() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, job := range m.jobs {
		if job.Status == core.JobPending {
			job.Status = core.JobQueued
		}
	}

	return nil
}

func (m *Source) Enqueue(job core.Model) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs[job.ID] = &job
	m.queue[job.Queue] = append(m.queue[job.Queue], &job)
	m.sortQueue(job.Queue)

	return nil
}

func (m *Source) Dequeue(queue string, limit int) ([]core.Model, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var dequeuedJobs []core.Model
	var remainingJobs []*core.Model

	for i := 0; i < limit && len(m.queue[queue]) > 0; {
		job := m.queue[queue][0]
		m.queue[queue] = m.queue[queue][1:]

		if job.AvailableAt.After(time.Now()) {
			remainingJobs = append(remainingJobs, job)
		} else {
			job.Status = core.JobPending
			dequeuedJobs = append(dequeuedJobs, *job)
			m.jobs[job.ID] = job
			i++
		}
	}

	m.queue[queue] = append(remainingJobs, m.queue[queue]...)

	return dequeuedJobs, nil
}

func (m *Source) UpdateJob(job core.Model) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.jobs[job.ID]; !exists {
		return core.ErrNoJobFound
	}

	m.jobs[job.ID] = &job
	m.sortQueue(job.Queue)

	return nil
}

func (m *Source) DeleteJob(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, exists := m.jobs[jobID]
	if !exists {
		return core.ErrNoJobFound
	}

	delete(m.jobs, jobID)
	m.removeJobFromQueue(job.Queue, jobID)

	return nil
}

func (m *Source) Length(queue string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.queue[queue]), nil
}

func (m *Source) Count(queue string, status core.Status) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for _, job := range m.queue[queue] {
		if job.Status == status {
			count++
		}
	}

	return count, nil
}

func (m *Source) Clear(queue string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, job := range m.queue[queue] {
		delete(m.jobs, job.ID)
	}

	m.queue[queue] = []*core.Model{}

	return nil
}

func (m *Source) sortQueue(queue string) {
	sort.Slice(m.queue[queue], func(i, j int) bool {
		return m.queue[queue][i].Score > m.queue[queue][j].Score
	})
}

func (m *Source) removeJobFromQueue(queue, jobID string) {
	for i, job := range m.queue[queue] {
		if job.ID == jobID {
			m.queue[queue] = append(m.queue[queue][:i], m.queue[queue][i+1:]...)
			break
		}
	}
}
