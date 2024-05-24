package core

import "errors"

var ErrAlreadyRunning = errors.New("queue already running")
var ErrTypeNotRegistered = errors.New("data type not registered")
var ErrTypeExtract = errors.New("type cannot extract")
var ErrNoJobsFound = errors.New("no jobs found")
var ErrNoJobFound = errors.New("job not found")
