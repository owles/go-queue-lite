# Go Queue Lite

Go Queue Lite is a lightweight queue implementation in Go designed to handle background tasks efficiently. This package provides a simple and scalable way to manage jobs, workers, and task execution.

See an example [here](https://github.com/owles/go-queue-lite-example).

## Features

- Lightweight and efficient job queue.
- Simple API for job and worker management.
- Customizable logger and metrics.
- Easy to integrate with any Go application.

## Installation

To install the package, run:

```bash
go get github.com/owles/go-queue-lite
```

## Usage

### Initialization

First, import the package:

```go
import (
    "github.com/owles/go-queue-lite"
)
```

### Creating a Queue

Create a new queue instance:

```go
src, err := mysql.NewMySQLSource(dsn)
if err != nil {
    panic(err)
}

// Make migration (for MySQL, Postgres or SQLite)
err = src.Up()
if err != nil {
    panic(err)
}

queue := New(ctx, src, Config{
    Workers:        5,
    Name:           "default",
    RemoveDoneJobs: true,
    TryAttempts:    3,
    DelayAttempts:  time.Second * 15,
    PrefetchFactor: 1,
})

err = queue.Run()
if err != nil {
    panic(err)
}
```

### Adding Jobs

Add jobs to the queue using the `Enqueue` method:

```go
// Create struct with Task interface implementation:
//
// type Task interface {
//   Handle() error
// }

type MySendEmailTask struct {
    UserID int `json:"user_id"`
}

func (t *MySendEmailTask) Handle() error {
	// Some task work here
	u := userRepository.find(t.UserID)
	return emailSender.SendVerificationEmailToUserID(u.UserID)
}

queue.RegisterTaskType(&MySendEmailTask{})

// And enqueue your Job

job := NewJob(&mockTask{
    ID: 1000,
}).High().DelayMinutes(5)

queue.Enqueue(job)
```

## Project Structure

- `core/`: Core functionality of the queue.
- `adapters/`: Adapter implementations for different storages.
- `logger.go`: Logger interface and default logger implementation.
- `metric.go`: Metric collection and reporting.
- `queue.go`: Main queue implementation.
- `worker.go`: Worker implementation.
- `job.go`: Job definition and handling.
- `task_manager.go`: Task manager interface and implementations.
- `test_data/`: Test data for unit tests.
- `queue_test.go`: Unit tests for the queue package.

## Contributing

Contributions are welcome! Please fork the repository and create a pull request with your changes.

## License

This project is licensed under the MIT License.

## Contact

For any questions or issues, please open an issue on GitHub.
