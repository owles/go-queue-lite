package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"

	"go-queue-lite/core"
)

type Source struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisSource(addr string, password string, db int) (*Source, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()
	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &Source{
		client: client,
		ctx:    ctx,
	}, nil
}

func (s *Source) Enqueue(job core.Model) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	err = s.client.ZAdd(s.ctx, job.Queue, &redis.Z{
		Score:  float64(job.Score),
		Member: data,
	}).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) Dequeue(queue string, limit int) ([]core.Model, error) {
	var jobs []core.Model
	var delayed []core.Model

	for i := 0; i < limit; i++ {
		results, err := s.client.ZPopMax(s.ctx, queue, 1).Result()
		if errors.Is(err, redis.Nil) || len(results) == 0 {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to dequeue job: %v", err)
		}

		data := results[0].Member.(string)

		var job core.Model
		err = json.Unmarshal([]byte(data), &job)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal job: %v", err)
		}

		if job.AvailableAt.After(time.Now().UTC()) {
			delayed = append(delayed, job)
			continue
		}

		job.Status = core.JobPending
		jobs = append(jobs, job)
	}

	for _, job := range delayed {
		s.Enqueue(job)
	}

	return jobs, nil
}

func (s *Source) UpdateJob(job core.Model) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	err = s.client.Set(s.ctx, fmt.Sprintf("job:%s", job.ID), data, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) DeleteJob(jobID string) error {
	err := s.client.Del(s.ctx, fmt.Sprintf("job:%s", jobID)).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) Length(queue string) (int, error) {
	ln, err := s.client.LLen(s.ctx, queue).Result()
	if err != nil {
		return 0, err
	}

	return int(ln), nil
}

func (s *Source) Count(queue string, status core.Status) (int, error) {
	return 0, nil
}

func (s *Source) Clear(queue string) error {
	err := s.client.Del(s.ctx, queue).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) ResetPending(queue string) error {
	keys, err := s.client.ZRange(s.ctx, queue, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to get jobs from queue: %v", err)
	}

	for _, data := range keys {
		var job core.Model
		err := json.Unmarshal([]byte(data), &job)
		if err != nil {
			return fmt.Errorf("failed to unmarshal job: %v", err)
		}

		if job.Status == core.JobPending {
			// Сбрасываем статус на Queued
			job.Status = core.JobQueued
			updatedData, err := json.Marshal(job)
			if err != nil {
				return fmt.Errorf("failed to marshal job: %v", err)
			}

			// Обновляем задание в Redis
			err = s.client.ZAdd(s.ctx, queue, &redis.Z{
				Score:  float64(job.Score),
				Member: updatedData,
			}).Err()
			if err != nil {
				return fmt.Errorf("failed to update job: %v", err)
			}
		}
	}

	return nil
}

func (s *Source) Close() error {
	return s.client.Close()
}
