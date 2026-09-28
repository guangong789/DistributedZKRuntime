package main

import (
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

type SQLiteJobStore struct {
	db *sql.DB
}

func NewSQLiteJobStore(path string) (*SQLiteJobStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	store := &SQLiteJobStore{
		db: db,
	}

	if err := store.initSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

func (s *SQLiteJobStore) initSchema() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			job_id INTEGER PRIMARY KEY,
			state INTEGER NOT NULL,
			attempt_id INTEGER NOT NULL,
			worker_id TEXT NOT NULL,
			task_type TEXT NOT NULL DEFAULT '',
			payload TEXT NOT NULL DEFAULT '',
			timeout_ms INTEGER NOT NULL DEFAULT 0
		);
	`)
	if err != nil {
		return err
	}

	if err := s.ensureJobColumn(
		"task_type",
		"TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return err
	}

	if err := s.ensureJobColumn(
		"payload",
		"TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return err
	}

	if err := s.ensureJobColumn(
		"timeout_ms",
		"INTEGER NOT NULL DEFAULT 0",
	); err != nil {
		return err
	}

	return nil
}

func (s *SQLiteJobStore) Save(record JobRecord) error {
	_, err := s.db.Exec(`
		INSERT INTO jobs(
			job_id,
			state,
			attempt_id,
			worker_id,
			task_type,
			payload,
			timeout_ms
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			state = excluded.state,
			attempt_id = excluded.attempt_id,
			worker_id = excluded.worker_id,
			task_type = excluded.task_type,
			payload = excluded.payload,
			timeout_ms = excluded.timeout_ms
	`,
		record.JobID,
		record.State,
		record.AttemptID,
		record.WorkerID,
		record.TaskType,
		record.Payload,
		record.TimeoutMs,
	)

	return err
}

func (s *SQLiteJobStore) Load(
	jobID int64,
) (JobRecord, bool, error) {
	var record JobRecord
	var state int

	err := s.db.QueryRow(`
		SELECT
			job_id,
			state,
			attempt_id,
			worker_id,
			task_type,
			payload,
			timeout_ms
		FROM jobs
		WHERE job_id = ?
	`, jobID).Scan(
		&record.JobID,
		&state,
		&record.AttemptID,
		&record.WorkerID,
		&record.TaskType,
		&record.Payload,
		&record.TimeoutMs,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return JobRecord{}, false, nil
	}

	if err != nil {
		return JobRecord{}, false, err
	}

	record.State = JobState(state)

	return record, true, nil
}

func (s *SQLiteJobStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteJobStore) ListByState(
	state JobState,
) ([]JobRecord, error) {
	rows, err := s.db.Query(`
		SELECT
			job_id,
			state,
			attempt_id,
			worker_id,
			task_type,
			payload,
			timeout_ms
		FROM jobs
		WHERE state = ?
	`, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []JobRecord

	for rows.Next() {
		var record JobRecord
		var storedState int

		if err := rows.Scan(
			&record.JobID,
			&storedState,
			&record.AttemptID,
			&record.WorkerID,
			&record.TaskType,
			&record.Payload,
			&record.TimeoutMs,
		); err != nil {
			return nil, err
		}

		record.State = JobState(storedState)

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func (s *SQLiteJobStore) ensureJobColumn(
	name string,
	definition string,
) error {
	rows, err := s.db.Query(`PRAGMA table_info(jobs)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	found := false

	for rows.Next() {
		var (
			cid        int
			columnName string
			columnType string
			notNull    int
			defaultVal any
			primaryKey int
		)

		if err := rows.Scan(
			&cid,
			&columnName,
			&columnType,
			&notNull,
			&defaultVal,
			&primaryKey,
		); err != nil {
			return err
		}

		if columnName == name {
			found = true
			break
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if found {
		return nil
	}

	query := fmt.Sprintf(
		"ALTER TABLE jobs ADD COLUMN %s %s",
		name,
		definition,
	)

	_, err = s.db.Exec(query)
	return err
}
