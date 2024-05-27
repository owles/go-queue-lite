package go_queue_lite

import (
	"context"
	"log/slog"
)

type queueLoggerContext struct {
	Queue    string
	WorkerID int
	JobID    string
	JobType  string
	Enabled  bool
}

type keyType int

const key = keyType(0)

type LogHandlerMiddleware struct {
	next slog.Handler
}

func NewLogHandlerMiddleware(next slog.Handler) *LogHandlerMiddleware {
	return &LogHandlerMiddleware{next: next}
}

func (h *LogHandlerMiddleware) Enabled(ctx context.Context, rec slog.Level) bool {
	if c, ok := ctx.Value(key).(queueLoggerContext); ok {
		return c.Enabled && h.next.Enabled(ctx, rec)
	}
	return h.next.Enabled(ctx, rec)
}

func (h *LogHandlerMiddleware) Handle(ctx context.Context, rec slog.Record) error {
	if c, ok := ctx.Value(key).(queueLoggerContext); ok {
		if c.Queue != "" {
			rec.Add("queue", c.Queue)
		}
		if c.WorkerID != 0 {
			rec.Add("worker_id", c.WorkerID)
		}
		if c.JobID != "" {
			rec.Add("job_id", c.JobID)
		}
		if c.JobType != "" {
			rec.Add("job_type", c.JobType)
		}
	}
	return h.next.Handle(ctx, rec)
}

func (h *LogHandlerMiddleware) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LogHandlerMiddleware{next: h.next.WithAttrs(attrs)} // не забыть обернуть, но осторожно
}

func (h *LogHandlerMiddleware) WithGroup(name string) slog.Handler {
	return &LogHandlerMiddleware{next: h.next.WithGroup(name)} // не забыть обернуть, но осторожно
}

func WithLogQueue(ctx context.Context, queue string) context.Context {
	if c, ok := ctx.Value(key).(queueLoggerContext); ok {
		c.Queue = queue
		return context.WithValue(ctx, key, c)
	}
	return context.WithValue(ctx, key, queueLoggerContext{Queue: queue})
}

func WithLogWorkerID(ctx context.Context, workerID int) context.Context {
	if c, ok := ctx.Value(key).(queueLoggerContext); ok {
		c.WorkerID = workerID
		return context.WithValue(ctx, key, c)
	}
	return context.WithValue(ctx, key, queueLoggerContext{WorkerID: workerID})
}

func WithLogJobType(ctx context.Context, jobType string) context.Context {
	if c, ok := ctx.Value(key).(queueLoggerContext); ok {
		c.JobType = jobType
		return context.WithValue(ctx, key, c)
	}
	return context.WithValue(ctx, key, queueLoggerContext{JobType: jobType})
}

func WithLogJobID(ctx context.Context, jobID string) context.Context {
	if c, ok := ctx.Value(key).(queueLoggerContext); ok {
		c.JobID = jobID
		return context.WithValue(ctx, key, c)
	}
	return context.WithValue(ctx, key, queueLoggerContext{JobID: jobID})
}

func WithEnabled(ctx context.Context, e bool) context.Context {
	if c, ok := ctx.Value(key).(queueLoggerContext); ok {
		c.Enabled = e
		return context.WithValue(ctx, key, c)
	}
	return context.WithValue(ctx, key, queueLoggerContext{Enabled: e})
}
