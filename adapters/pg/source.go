package pg

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"io/fs"
	"time"

	"github.com/jmoiron/sqlx"
	pg "github.com/lib/pq"
	"go-queue-lite/core"
)

//go:embed migrations/*
var Migrations embed.FS

type PostgresSource struct {
	db *sqlx.DB
}

func NewPostgresSource(dsn string) (*PostgresSource, error) {
	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &PostgresSource{db: db}, nil
}

func (s *PostgresSource) Up() error {
	migrationFS, err := fs.Sub(Migrations, "migrations")
	if err != nil {
		return err
	}

	driver, err := postgres.WithInstance(s.db.DB, &postgres.Config{})
	if err != nil {
		return err
	}

	migrationSrc, err := iofs.New(migrationFS, ".")
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance(
		"iofs",
		migrationSrc,
		"postgres",
		driver,
	)
	if err != nil {
		return err
	}

	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

func (s *PostgresSource) ResetPending(queue string) error {
	query := `
		UPDATE jobs
		SET status = $1
		WHERE status = $2 AND queue = $3;
	`
	_, err := s.db.Exec(query, core.JobQueued, core.JobPending, queue)
	return err
}

func (s *PostgresSource) Enqueue(job core.Model) error {
	query := `
		INSERT INTO jobs (
			id, queue, priority, status, attempts, error, payload, score, available_at, created_at
		) VALUES (:id, :queue, :priority, :status, :attempts, :error, :payload, :score, :available_at, :created_at);
	`
	_, err := s.db.NamedExec(query, map[string]interface{}{
		"id":           job.ID,
		"queue":        job.Queue,
		"priority":     job.Priority,
		"status":       job.Status,
		"attempts":     job.Attempts,
		"error":        job.Error,
		"payload":      job.Payload,
		"score":        job.Score,
		"available_at": job.AvailableAt,
		"created_at":   job.CreatedAt,
	})

	return err
}

func (s *PostgresSource) Dequeue(queue string, limit int) ([]core.Model, error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	query := `
		SELECT id, queue, priority, status, attempts, error, payload, score, available_at as availableat, created_at as createdat
		FROM jobs
		WHERE queue = $1 AND status = $2 AND available_at <= $3
		ORDER BY score DESC
		LIMIT $4;
	`

	var jobs []core.Model
	err = tx.Select(&jobs, query, queue, core.JobQueued, time.Now().UTC(), limit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrNoJobsFound
		}
		return nil, fmt.Errorf("failed to get jobs: %v", err)
	}

	if len(jobs) == 0 {
		return nil, core.ErrNoJobsFound
	}

	jobIDs := make([]interface{}, len(jobs))
	for i, job := range jobs {
		jobIDs[i] = job.ID
	}

	updateQuery := `UPDATE jobs SET status = $1 WHERE id = ANY($2);`
	updateQuery, args, err := sqlx.In(updateQuery, pg.Array(jobIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to build update query: %v", err)
	}
	updateQuery = s.db.Rebind(updateQuery)

	_, err = tx.Exec(updateQuery, append([]interface{}{core.JobPending}, args...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to update job statuses: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	for i := range jobs {
		jobs[i].Status = core.JobPending
	}

	return jobs, nil
}

func (s *PostgresSource) UpdateJob(job core.Model) error {
	query := `
		UPDATE jobs
		SET status = :status, attempts = :attempts, error = :error, available_at = :available_at
		WHERE id = :id;
	`
	_, err := s.db.NamedExec(query, map[string]interface{}{
		"id":           job.ID,
		"status":       job.Status,
		"attempts":     job.Attempts,
		"error":        job.Error,
		"available_at": job.AvailableAt,
	})

	return err
}

func (s *PostgresSource) DeleteJob(queue, jobID string) error {
	query := `
		DELETE FROM jobs
		WHERE id = $1 AND queue = $2;
	`
	_, err := s.db.Exec(query, jobID, queue)
	return err
}

func (s *PostgresSource) Length(queue string) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM jobs
		WHERE queue = $1;
	`
	var count int
	err := s.db.Get(&count, query, queue)
	return count, err
}

func (s *PostgresSource) Count(queue string, status core.Status) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM jobs
		WHERE queue = $1 AND status = $2;
	`
	var count int
	err := s.db.Get(&count, query, queue, status)
	return count, err
}

func (s *PostgresSource) Clear(queue string) error {
	query := `
		DELETE FROM jobs
		WHERE queue = $1;
	`
	_, err := s.db.Exec(query, queue)
	return err
}

func (s *PostgresSource) Close() error {
	return s.db.Close()
}
