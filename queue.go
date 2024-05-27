package go_queue_lite

import (
	"context"
	"errors"
	"go-queue-lite/core"
	"log/slog"
	"reflect"
	"sync"
	"time"
)

const defaultPrefetchFactor = 5

type Config struct {
	Name           string
	Workers        int
	TryAttempts    int
	DelayAttempts  time.Duration
	PrefetchFactor int
	RemoveDoneJobs bool
	logger         *slog.Logger
}

type Queue struct {
	sync.Mutex

	config Config

	ctx    context.Context
	cancel context.CancelFunc

	src       core.SourceInterface
	isRunning bool
	wg        sync.WaitGroup
	jobCh     chan *core.Model

	typeManager core.TaskTypeManagerInterface

	workers []*Worker

	logger *slog.Logger
}

func New(ctx context.Context, src core.SourceInterface, config Config) *Queue {
	newCtx, cancel := context.WithCancel(context.WithValue(ctx, "queue", config.Name))

	var lg *slog.Logger
	if config.logger == nil {
		lg = slog.Default()
		newCtx = WithEnabled(newCtx, false)
	} else {
		lg = config.logger
		newCtx = WithEnabled(newCtx, true)
	}

	return &Queue{
		ctx:       WithLogQueue(newCtx, config.Name),
		cancel:    cancel,
		config:    config,
		isRunning: false,
		src:       src,
		workers:   make([]*Worker, config.Workers),
		jobCh:     make(chan *core.Model, config.Workers),

		typeManager: NewTaskTypeManager(),

		logger: slog.New(NewLogHandlerMiddleware(lg.Handler())),
	}
}

func (q *Queue) RegisterTaskType(typ core.Task) *Queue {
	slog.DebugContext(WithLogJobType(q.ctx, reflect.TypeOf(typ).String()), "register type")

	q.typeManager.RegisterType(typ)
	return q
}

func (q *Queue) Enqueue(job *Job) error {
	q.Lock()
	defer q.Unlock()

	dataType := reflect.TypeOf(job.task)
	ctx := WithLogJobType(q.ctx, dataType.String())

	slog.DebugContext(ctx, "enqueue")

	if !q.typeManager.ExistType(dataType.String()) {
		slog.ErrorContext(ctx, "typeManager.ExistType", "error", core.ErrTypeNotRegistered)
		return core.ErrTypeNotRegistered
	}

	job.onQueue(q.config.Name)

	model, err := job.getModel()
	if err != nil {
		slog.ErrorContext(ctx, "job.getModel", "error", err)
		return err
	}

	if err = q.src.Enqueue(model); err != nil {
		slog.ErrorContext(ctx, "source.Enqueue", "error", err)
		return err
	}

	return nil
}

func (q *Queue) Stop() {
	q.Lock()
	defer q.Unlock()

	q.logger.InfoContext(q.ctx, "stop queue")

	q.cancel()
}

func (q *Queue) pool() {
	prefetchFactor := q.config.PrefetchFactor
	if prefetchFactor <= 0 {
		prefetchFactor = defaultPrefetchFactor
	}

	for {
		select {
		case <-q.ctx.Done():
			break
		default:
			jobs, err := q.src.Dequeue(q.config.Name, q.config.Workers*prefetchFactor)
			if err != nil || len(jobs) == 0 {
				if errors.Is(err, core.ErrNoJobsFound) || len(jobs) == 0 {
					q.logger.DebugContext(q.ctx, "no jobs found, wait a second")
					time.Sleep(time.Second)
				}
				continue
			} else {
				q.logger.DebugContext(q.ctx, "found new jobs", "count", len(jobs))
			}

			for _, job := range jobs {
				q.jobCh <- &job
			}
			time.Sleep(time.Nanosecond)
		}
	}
}

func (q *Queue) Run() error {
	if q.isRunning {
		return core.ErrAlreadyRunning
	}

	if err := q.src.ResetPending(q.config.Name); err != nil {
		return err
	}

	q.isRunning = true

	q.logger.InfoContext(q.ctx, "start queue")

	for i := 0; i < q.config.Workers; i++ {
		q.workers[i] = NewWorker(i+1, &WorkerContext{
			ctx:         q.ctx,
			wg:          &q.wg,
			jobCh:       q.jobCh,
			src:         q.src,
			typeManager: q.typeManager,
			// Config
			tryAttempts:    q.config.TryAttempts,
			delayAttempts:  q.config.DelayAttempts,
			removeDoneJobs: q.config.RemoveDoneJobs,

			logger: q.logger,
		})
		go q.workers[i].start()
	}

	go q.pool()

	<-q.ctx.Done()
	q.wg.Wait()

	q.isRunning = false

	return nil
}

func (q *Queue) GetMetrics() []*Metrics {
	q.Lock()
	defer q.Unlock()

	m := make([]*Metrics, q.config.Workers)
	for i, w := range q.workers {
		m[i] = w.GetMetric()
	}
	return m
}
