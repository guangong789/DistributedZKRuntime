package main

import (
	"database/sql"
	"errors"

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
			worker_id TEXT NOT NULL
		);
	`)
	return err
}

func (s *SQLiteJobStore) Save(record JobRecord) error {
	_, err := s.db.Exec(`
		INSERT INTO jobs (
			job_id,
			state,
			attempt_id,
			worker_id
		)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			state = excluded.state,
			attempt_id = excluded.attempt_id,
			worker_id = excluded.worker_id
	`,
		record.JobID,
		record.State,
		record.AttemptID,
		record.WorkerID,
	)

	return err
}

func (s *SQLiteJobStore) Load(jobID int64) (JobRecord, bool, error) {
	var record JobRecord
	var state int

	err := s.db.QueryRow(`
		SELECT job_id, state, attempt_id, worker_id
		FROM jobs
		WHERE job_id = ?
	`, jobID).Scan(
		&record.JobID,
		&state,
		&record.AttemptID,
		&record.WorkerID,
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
