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
	offset := 0

	for len(jobs) < limit {
		results, err := s.client.ZRevRangeWithScores(s.ctx, queue, int64(offset), int64(offset)).Result()
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
			offset++
			if err := s.Enqueue(job); err != nil {
				return nil, fmt.Errorf("failed to re-enqueue job: %v", err)
			}
			continue
		}

		_, err = s.client.ZRem(s.ctx, queue, data).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to remove job from queue: %v", err)
		}

		job.Status = core.JobPending
		jobs = append(jobs, job)

		// s.UpdateJob(job)

		offset++
	}

	return jobs, nil
}

func (s *Source) UpdateJob(job core.Model) error {
	if job.Status == core.JobQueued {
		return s.Enqueue(job)
	}

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	err = s.client.Set(s.ctx, fmt.Sprintf(job.Queue+":%s", job.ID), data, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) DeleteJob(queue, jobID string) error {
	err := s.client.Del(s.ctx, fmt.Sprintf(queue+":%s", jobID)).Err()
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
	iter := s.client.Scan(s.ctx, 0, queue+":*", 0).Iterator()
	for iter.Next(s.ctx) {
		key := iter.Val()

		data, err := s.client.Get(s.ctx, key).Result()
		if err != nil {
			return fmt.Errorf("failed to get job: %v", err)
		}

		var job core.Model
		err = json.Unmarshal([]byte(data), &job)
		if err != nil {
			return fmt.Errorf("failed to unmarshal job: %v", err)
		}

		if job.Status == core.JobPending {
			job.Status = core.JobQueued
			if err := s.Enqueue(job); err != nil {
				return fmt.Errorf("failed to re-enqueue job: %v", err)
			}
			s.client.Del(s.ctx, key)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("error iterating over job keys: %v", err)
	}

	return nil
}

func (s *Source) Close() error {
	return s.client.Close()
}
