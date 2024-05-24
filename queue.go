package go_queue_lite

import (
	"context"
	"errors"
	"go-queue-lite/core"
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

	tasks []core.Model

	workers []*Worker
}

func New(ctx context.Context, src core.SourceInterface, config Config) *Queue {
	newCtx, cancel := context.WithCancel(context.WithValue(ctx, "queue", config.Name))

	return &Queue{
		ctx:       newCtx,
		cancel:    cancel,
		config:    config,
		isRunning: false,
		src:       src,
		tasks:     []core.Model{},
		workers:   make([]*Worker, config.Workers),
		jobCh:     make(chan *core.Model, config.Workers),

		typeManager: NewTaskTypeManager(),
	}
}

func (q *Queue) RegisterTaskType(typ core.Task) *Queue {
	q.typeManager.RegisterType(typ)
	return q
}

func (q *Queue) addTasks(jobs []core.Model) {
	q.Lock()
	defer q.Unlock()
	q.tasks = append(q.tasks, jobs...)
}

func (q *Queue) getTask() (core.Model, error) {
	q.Lock()
	defer q.Unlock()
	if len(q.tasks) == 0 {
		return core.Model{}, core.ErrNoJobsFound
	}
	job := q.tasks[0]
	q.tasks = q.tasks[1:]
	return job, nil
}

func (q *Queue) Enqueue(job *Job) error {
	q.Lock()
	defer q.Unlock()

	dataType := reflect.TypeOf(job.task)
	if !q.typeManager.ExistType(dataType.String()) {
		return core.ErrTypeNotRegistered
	}

	job.onQueue(q.config.Name)

	model, err := job.getModel()
	if err != nil {
		return err
	}

	if err = q.src.Enqueue(model); err != nil {
		return err
	}

	return nil
}

func (q *Queue) Stop() {
	q.Lock()
	defer q.Unlock()

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
					time.Sleep(time.Second)
				}
				continue
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

	for i := 0; i < q.config.Workers; i++ {
		q.workers[i] = NewWorker(i, &WorkerContext{
			ctx:         q.ctx,
			wg:          &q.wg,
			jobCh:       q.jobCh,
			src:         q.src,
			typeManager: q.typeManager,
			// Config
			tryAttempts:    q.config.TryAttempts,
			delayAttempts:  q.config.DelayAttempts,
			removeDoneJobs: q.config.RemoveDoneJobs,
		})
		go q.workers[i].start()
	}

	go q.pool()

	<-q.ctx.Done()
	q.wg.Wait()

	q.isRunning = false

	return nil
}
