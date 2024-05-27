package go_queue_lite

import (
	"github.com/owles/go-queue-lite/core"
	"time"
)

type Job struct {
	model *core.Model
	task  core.Task
}

func NewJob(task core.Task) *Job {
	return &Job{
		model: core.NewModel(),
		task:  task,
	}
}

func (j *Job) getModel() (core.Model, error) {
	err := j.model.Encode(j.task)
	if err != nil {
		return core.Model{}, err
	}

	return *j.model, nil
}

func (j *Job) onQueue(name string) *Job {
	j.model.SetQueue(name)
	return j
}

func (j *Job) Priority(priority core.Priority) *Job {
	j.model.SetPriority(priority)
	return j
}

func (j *Job) High() *Job {
	return j.Priority(core.PriorityHigh)
}

func (j *Job) Normal() *Job {
	return j.Priority(core.PriorityNormal)
}

func (j *Job) Low() *Job {
	return j.Priority(core.PriorityLow)
}

func (j *Job) Delay(at time.Time) *Job {
	j.model.SetAvailableAt(at)
	return j
}

func (j *Job) DelaySeconds(s int) *Job {
	return j.Delay(time.Now().Add(time.Duration(s) * time.Second))
}

func (j *Job) DelayMinutes(m int) *Job {
	return j.Delay(time.Now().Add(time.Duration(m) * time.Minute))
}

func (j *Job) DelayHours(h int) *Job {
	return j.Delay(time.Now().Add(time.Duration(h) * time.Hour))
}

func (j *Job) DelayDays(d int) *Job {
	return j.Delay(time.Now().Add(time.Duration(d) * (time.Hour * 24)))
}
