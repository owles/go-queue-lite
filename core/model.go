package core

import (
	"encoding/json"
	"github.com/google/uuid"
	"reflect"
	"time"
)

type Payload struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type Model struct {
	ID          string
	Queue       string
	Priority    Priority
	Status      Status
	Attempts    int
	Score       int64
	Payload     json.RawMessage
	AvailableAt time.Time
	CreatedAt   time.Time
}

func NewModel() *Model {
	job := &Model{
		ID:          uuid.New().String(),
		Queue:       DefaultQueue,
		Status:      JobQueued,
		Priority:    PriorityNormal,
		Score:       0,
		Attempts:    0,
		AvailableAt: time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
		Payload:     json.RawMessage("{}"),
	}
	job.setupScore()
	return job
}

func (j *Model) setupScore() {
	priorityShift := int64(j.Priority) << 32
	createdAtSeconds := j.CreatedAt.UnixMilli()
	j.Score = priorityShift + createdAtSeconds
}

func (j *Model) SetQueue(name string) *Model {
	j.Queue = name
	return j
}

func (j *Model) SetPayload(pl json.RawMessage) *Model {
	j.Payload = pl
	return j
}

func (j *Model) SetAvailableAt(at time.Time) *Model {
	j.AvailableAt = at.UTC()
	return j
}

func (j *Model) SetPriority(priority Priority) *Model {
	j.Priority = priority
	j.setupScore()
	return j
}

func (j *Model) SetStatus(status Status) *Model {
	j.Status = status
	return j
}

func (j *Model) Attempt() *Model {
	j.Attempts++
	return j
}

func (j *Model) Decode() (Payload, error) {
	pl := Payload{}

	err := json.Unmarshal(j.Payload, &pl)
	if err != nil {
		return Payload{}, err
	}

	return pl, nil
}

func (j *Model) Encode(task Task) error {
	taskType := reflect.TypeOf(task)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}

	pl := Payload{
		Type: taskType.String(),
		Data: taskData,
	}

	j.Payload, err = json.Marshal(pl)
	if err != nil {
		return err
	}

	return nil
}
