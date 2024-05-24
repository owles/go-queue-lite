package go_queue_lite

import (
	"context"
	"fmt"
	"go-queue-lite/adapters/mysql"
	"go-queue-lite/adapters/pg"
	"go-queue-lite/adapters/redis"
	"go-queue-lite/adapters/sqlite"
	"sync"
	"testing"
	"time"
)

var wg sync.WaitGroup
var doneJobs sync.Map
var jobsCount = 100_000

type mockTask struct {
	ID int
}

func (t *mockTask) Handle() error {
	if t.ID == 6 {
		// panic(fmt.Errorf("Test panic!"))
		// return fmt.Errorf("mock task had id 100")
	}
	defer func() {
		time.AfterFunc(time.Second, func() {
			wg.Done()
		})
	}()

	if _, ok := doneJobs.Load(t.ID); ok {
		return fmt.Errorf("task %d is already done", t.ID)
	}
	doneJobs.Store(t.ID, true)
	// time.Sleep(500 * time.Millisecond)
	// time.Sleep(time.Duration(rand.Intn(901)+100) * time.Millisecond)

	return nil
}

func TestMySQLQueue(t *testing.T) {
	dsn := "root:root@123@tcp(127.0.0.1:3306)/queue_test?charset=utf8mb4&parseTime=true&multiStatements=true"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src, err := mysql.NewMySQLSource(dsn)
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}

	err = src.Up()
	if err != nil {
		t.Fatalf("Failed to UP migration: %v", err)
	}

	src.Clear("default")

	queue := New(ctx, src, Config{
		Workers:        1,
		Name:           "default",
		RemoveDoneJobs: true,
		TryAttempts:    3,
		DelayAttempts:  time.Second * 15,
		PrefetchFactor: 1,
	})

	defer src.Close()

	queue.RegisterTaskType(&mockTask{})

	for i := 0; i < jobsCount; i++ {
		task := &mockTask{ID: i}
		job := NewJob(task)
		if i < 5 {
			job.High()
		}

		if i >= 2 && i <= 5 {
			job.DelaySeconds(5)
		}

		err := queue.Enqueue(job)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
	}

	ts := time.Now()

	go queue.Run()

	wg.Wait()
	fmt.Println("Done! in ", time.Since(ts), "seconds")
}

func TestPGQueue(t *testing.T) {
	dsn := "postgres://postgres@localhost:5432/queue_test?sslmode=disable"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src, err := pg.NewPostgresSource(dsn)
	if err != nil {
		t.Fatalf("Failed to connect to PG: %v", err)
	}

	err = src.Up()
	if err != nil {
		t.Fatalf("Failed to UP migration: %v", err)
	}

	queue := New(ctx, src, Config{
		Workers:        10,
		Name:           "default",
		RemoveDoneJobs: true,
		TryAttempts:    3,
		DelayAttempts:  time.Second * 15,
	})

	defer src.Close()

	queue.RegisterTaskType(&mockTask{})

	for i := 0; i < jobsCount; i++ {
		task := &mockTask{ID: i}
		job := NewJob(task)
		if i < 5 {
			job.High()
		}

		if i == 2 {
			job.DelaySeconds(5)
		}

		err := queue.Enqueue(job)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
	}

	ts := time.Now()

	go queue.Run()

	wg.Wait()
	fmt.Println("Done! in ", time.Since(ts), "seconds")
}

func TestSQLite(t *testing.T) {
	dsn := "file:test_data/queue.db?cache=shared&mode=rwc"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src, err := sqlite.NewSQLiteSource(dsn)
	if err != nil {
		t.Fatalf("Failed to connect to PG: %v", err)
	}

	err = src.Up()
	if err != nil {
		t.Fatalf("Failed to UP migration: %v", err)
	}

	queue := New(ctx, src, Config{
		Workers:        10,
		Name:           "default",
		RemoveDoneJobs: true,
	})

	defer src.Close()

	queue.RegisterTaskType(&mockTask{})

	for i := 0; i < jobsCount; i++ {
		task := &mockTask{ID: i}
		job := NewJob(task)
		if i < 5 {
			job.High()
		}

		if i == 2 {
			job.DelaySeconds(5)
		}

		err := queue.Enqueue(job)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
	}

	ts := time.Now()

	go queue.Run()

	wg.Wait()
	fmt.Println("Done! in ", time.Since(ts), "seconds")
}

func TestRedis(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src, err := redis.NewRedisSource("127.0.0.1:6379", "", 0)
	if err != nil {
		t.Fatalf("Failed to connect to PG: %v", err)
	}

	queue := New(ctx, src, Config{
		Workers:        1,
		Name:           "default",
		RemoveDoneJobs: true,
		TryAttempts:    3,
		PrefetchFactor: 1,
		DelayAttempts:  time.Second * 15,
	})

	src.Clear("default")

	defer src.Close()

	queue.RegisterTaskType(&mockTask{})
	ts := time.Now()

	for i := 0; i < jobsCount; i++ {
		task := &mockTask{ID: i}
		job := NewJob(task)
		if i < 5 {
			job.High()
		}

		if i >= 2 && i <= 5 {
			job.DelaySeconds(5 + i)
		}

		err := queue.Enqueue(job)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
	}

	fmt.Printf("Created %d jobs in %s \n", jobsCount, time.Since(ts))

	ts = time.Now()

	go queue.Run()

	wg.Wait()
	fmt.Println("Done! in ", time.Since(ts), "seconds")
}
