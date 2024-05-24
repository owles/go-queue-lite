package core

type SourceInterface interface {
	ResetPending(queue string) error

	Enqueue(job Model) error
	Dequeue(queue string, limit int) ([]Model, error)

	UpdateJob(job Model) error
	DeleteJob(queue, jobID string) error

	Length(queue string) (int, error)
	Count(queue string, status Status) (int, error)
	Clear(queue string) error
}
