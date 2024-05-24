package go_queue_lite

import (
	"context"
	"encoding/json"
	"fmt"
	"go-queue-lite/core"
	"sync"
	"time"
)

type WorkerContext struct {
	ctx   context.Context
	wg    *sync.WaitGroup
	jobCh chan *core.Model

	src         core.SourceInterface
	typeManager core.TaskTypeManagerInterface

	tryAttempts    int
	delayAttempts  int
	removeDoneJobs bool
}

type Worker struct {
	id  int
	ctx *WorkerContext
}

func NewWorker(id int, ctx *WorkerContext) *Worker {
	return &Worker{
		id:  id,
		ctx: ctx,
	}
}

func (w *Worker) start() {
	w.ctx.wg.Add(1)
	defer w.ctx.wg.Done()

	fmt.Printf("worker %d started\n", w.id)

	for {
		select {
		case job := <-w.ctx.jobCh:
			fmt.Printf("worker: %d got a Job: %s with priority: %d \n", w.id, job.ID, job.Priority)
			//
			job.Attempt()

			pl, err := job.Decode()
			if err != nil {
				w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
			} else {
				if w.ctx.typeManager.ExistType(pl.Type) {
					var task core.Task
					task, err = w.ctx.typeManager.ExtractType(pl.Type)
					if err != nil {
						w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
					} else {
						err = json.Unmarshal(pl.Data, task)

						if err != nil {
							w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
						} else {
							if err = task.Handle(); err != nil {
								if w.ctx.tryAttempts >= job.Attempts {
									w.ctx.src.UpdateJob(*job.SetStatus(core.JobQueued).
										SetAvailableAt(time.Now().Add(time.Second * time.Duration(w.ctx.delayAttempts)).UTC()))
								} else {
									w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
								}
							} else {
								if w.ctx.removeDoneJobs {
									w.ctx.src.DeleteJob(job.ID)
								} else {
									w.ctx.src.UpdateJob(*job.SetStatus(core.JobDone))
								}
							}
						}
					}
				}
			}
		case <-w.ctx.ctx.Done():
			return
		}
	}
}
