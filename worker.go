package go_queue_lite

import (
	"context"
	"encoding/json"
	"go-queue-lite/core"
	"log/slog"
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
	delayAttempts  time.Duration
	removeDoneJobs bool

	logger *slog.Logger
}

type Worker struct {
	id     int
	ctx    *WorkerContext
	metric *Metrics
}

func NewWorker(id int, ctx *WorkerContext) *Worker {
	ctx.ctx = WithLogWorkerID(ctx.ctx, id)

	return &Worker{
		id:     id,
		ctx:    ctx,
		metric: NewMetrics(),
	}
}

func (w *Worker) GetMetric() *Metrics {
	return w.metric
}

func (w *Worker) start() {
	w.ctx.wg.Add(1)
	defer w.ctx.wg.Done()

	w.ctx.logger.InfoContext(w.ctx.ctx, "start worker")

	for {
		select {
		case job := <-w.ctx.jobCh:
			job.Attempt()
			ctx := WithLogJobID(w.ctx.ctx, job.ID)
			ts := time.Now()

			pl, err := job.Decode()
			if err != nil {
				w.ctx.logger.ErrorContext(ctx, "can't decode job", "error", err)
				w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
			} else {
				ctx = WithLogJobType(ctx, pl.Type)
				w.ctx.logger.InfoContext(ctx, "processing job")

				if w.ctx.typeManager.ExistType(pl.Type) {
					var task core.Task
					task, err = w.ctx.typeManager.ExtractType(pl.Type)
					if err != nil {
						w.metric.IncErrors()
						w.ctx.logger.ErrorContext(ctx, "can't extract type", "error", err)

						w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
					} else {
						err = json.Unmarshal(pl.Data, task)

						if err != nil {
							w.metric.IncErrors()
							w.ctx.logger.ErrorContext(ctx, "can't unmarshall payload", "error", err)

							w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
						} else {
							if err = task.Handle(); err != nil {
								w.metric.RecordProcessingTime(time.Since(ts))
								w.metric.IncErrors()
								w.ctx.logger.ErrorContext(ctx, "can't handle job task", "error", err)

								job.SetError(err.Error())
								if job.Attempts < w.ctx.tryAttempts {
									delay := time.Now().Add(w.ctx.delayAttempts).UTC()
									w.ctx.logger.DebugContext(ctx, "delay", "available_at", delay)
									w.ctx.src.UpdateJob(*job.SetStatus(core.JobQueued).SetAvailableAt(delay))
								} else {
									w.ctx.logger.DebugContext(ctx, "mark as error")
									w.ctx.src.UpdateJob(*job.SetStatus(core.JobError))
								}
							} else {
								w.metric.RecordProcessingTime(time.Since(ts))
								w.metric.IncProcessed()

								if w.ctx.removeDoneJobs {
									w.ctx.logger.InfoContext(ctx, "processed job", "action", "delete")
									w.ctx.src.DeleteJob(job.Queue, job.ID)
								} else {
									w.ctx.logger.InfoContext(ctx, "processed job", "action", "keep")
									w.ctx.src.UpdateJob(*job.SetStatus(core.JobDone))
								}
							}
						}
					}
				}
			}
		case <-w.ctx.ctx.Done():
			w.ctx.logger.InfoContext(w.ctx.ctx, "stop worker")
			return
		}
	}
}
