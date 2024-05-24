package core

const DefaultQueue = "default"

type Status int

const (
	JobQueued Status = iota
	JobPending
	JobDone
	JobError
)

type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
)
