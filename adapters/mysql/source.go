package mysql

import (
	"embed"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4"
	mysql2 "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jmoiron/sqlx"
	"go-queue-lite/core"
	"io/fs"
	"strings"
	"time"
)

//go:embed migrations/*
var Migrations embed.FS

type Source struct {
	db *sqlx.DB
}

func NewMySQLSource(dsn string) (*Source, error) {
	db, err := sqlx.Open("mysql", dsn)
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

	driver, err := mysql2.WithInstance(s.db.DB, &mysql2.Config{})
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
		"",
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

func (s *Source) ResetPending() error {
	query := `
		UPDATE jobs
		SET status = ?
		WHERE status = ?;
	`
	_, err := s.db.Exec(query, core.JobQueued, core.JobPending)
	return err
}

func (s *Source) Enqueue(job core.Model) error {
	query := `
		INSERT INTO jobs (
			id, queue, priority, status, attempts, payload, score, available_at, created_at
		) VALUES (:id, :queue, :priority, :status, :attempts, :payload, :score, :available_at, :created_at);
	`
	_, err := s.db.NamedExec(query, map[string]interface{}{
		"id":           job.ID,
		"queue":        job.Queue,
		"priority":     job.Priority,
		"status":       job.Status,
		"attempts":     job.Attempts,
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
		SELECT id, queue, priority, status, attempts, payload, score, available_at AS availableat, created_at AS createdat
		FROM jobs
		WHERE queue = ? AND status = ? AND available_at <= ?
		ORDER BY score DESC
		LIMIT ?;
	`

	var jobs []core.Model
	err = tx.Select(&jobs, query, queue, core.JobQueued, time.Now().UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs: %v", err)
	}

	if len(jobs) == 0 {
		return nil, core.ErrNoJobsFound
	}

	jobIDs := make([]interface{}, len(jobs))
	for i, job := range jobs {
		jobIDs[i] = job.ID
	}

	updateQuery := `
		UPDATE jobs
		SET status = ?
		WHERE id IN (?` + strings.Repeat(",?", len(jobIDs)-1) + `);
	`

	updateQuery, _, err = sqlx.In(updateQuery, append([]interface{}{core.JobPending}, jobIDs...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to build update query: %v", err)
	}
	updateQuery = s.db.Rebind(updateQuery)

	_, err = tx.Exec(updateQuery, append([]interface{}{core.JobPending}, jobIDs...)...)
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
	if job.ID == "" {
		return core.ErrNoJobFound
	}

	query := `
		UPDATE jobs
		SET status = :status, attempts = :attempts, available_at = :available_at
		WHERE id = :id;
	`
	_, err := s.db.NamedExec(query, map[string]interface{}{
		"id":           job.ID,
		"status":       job.Status,
		"attempts":     job.Attempts,
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
