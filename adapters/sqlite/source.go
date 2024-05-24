package sqlite

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"io/fs"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"go-queue-lite/core"
)

//go:embed migrations/*
var Migrations embed.FS

type Source struct {
	db *sqlx.DB
}

func NewSQLiteSource(dsn string) (*Source, error) {
	db, err := sqlx.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	return &Source{db: db}, nil
}

func (s *Source) Up() error {
	migrationFS, err := fs.Sub(Migrations, "migrations")
	if err != nil {
		return err
	}

	driver, err := sqlite3.WithInstance(s.db.DB, &sqlite3.Config{})
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
		"sqlite3",
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

func (s *Source) ResetPending(queue string) error {
	query := `
		UPDATE jobs
		SET status = ?
		WHERE status = ? AND queue = ?;
	`
	_, err := s.db.Exec(query, core.JobQueued, core.JobPending, queue)
	return err
}

func (s *Source) Enqueue(job core.Model) error {
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

func (s *Source) Dequeue(queue string, limit int) ([]core.Model, error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	query := `
		SELECT id, queue, priority, status, attempts, payload, score, available_at, created_at
		FROM jobs
		WHERE queue = ? AND status = ? AND available_at <= ?
		ORDER BY score DESC
		LIMIT ?;
	`

	var rawJobs []struct {
		ID          string        `db:"id"`
		Queue       string        `db:"queue"`
		Priority    core.Priority `db:"priority"`
		Status      core.Status   `db:"status"`
		Attempts    int           `db:"attempts"`
		Error       string        `db:"error"`
		Payload     []byte        `db:"payload"`
		Score       int64         `db:"score"`
		AvailableAt time.Time     `db:"available_at"`
		CreatedAt   time.Time     `db:"created_at"`
	}

	err = tx.Select(&rawJobs, query, queue, core.JobQueued, time.Now().UTC(), limit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrNoJobsFound
		}
		return nil, fmt.Errorf("failed to get jobs: %v", err)
	}

	if len(rawJobs) == 0 {
		return nil, core.ErrNoJobsFound
	}

	jobs := make([]core.Model, len(rawJobs))
	jobIDs := make([]interface{}, len(rawJobs))
	for i, rawJob := range rawJobs {
		jobs[i] = core.Model{
			ID:          rawJob.ID,
			Queue:       rawJob.Queue,
			Priority:    rawJob.Priority,
			Status:      rawJob.Status,
			Attempts:    rawJob.Attempts,
			Error:       rawJob.Error,
			Payload:     rawJob.Payload,
			Score:       rawJob.Score,
			AvailableAt: rawJob.AvailableAt,
			CreatedAt:   rawJob.CreatedAt,
		}
		jobIDs[i] = rawJob.ID
	}

	updateQuery := `
		UPDATE jobs
		SET status = ?
		WHERE id IN (?` + strings.Repeat(",?", len(jobIDs)-1) + `);
	`

	updateQuery, params, err := sqlx.In(updateQuery, append([]interface{}{core.JobPending}, jobIDs...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to build update query: %v", err)
	}
	updateQuery = s.db.Rebind(updateQuery)

	_, err = tx.Exec(updateQuery, params...)
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

func (s *Source) UpdateJob(job core.Model) error {
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

func (s *Source) DeleteJob(jobID string) error {
	query := `
		DELETE FROM jobs
		WHERE id = ?;
	`
	_, err := s.db.Exec(query, jobID)
	return err
}

func (s *Source) Length(queue string) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM jobs
		WHERE queue = ?;
	`
	var count int
	err := s.db.Get(&count, query, queue)
	return count, err
}

func (s *Source) Count(queue string, status core.Status) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM jobs
		WHERE queue = ? AND status = ?;
	`
	var count int
	err := s.db.Get(&count, query, queue, status)
	return count, err
}

func (s *Source) Clear(queue string) error {
	query := `
		DELETE FROM jobs
		WHERE queue = ?;
	`
	_, err := s.db.Exec(query, queue)
	return err
}

func (s *Source) Close() error {
	return s.db.Close()
}
