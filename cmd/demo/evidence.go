package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	_ "modernc.org/sqlite"
)

func expectedHash(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func validateResponse(req *runtimepb.SubmitJobRequest, resp *runtimepb.SubmitJobResponse) error {
	if resp == nil || resp.JobId != req.JobId || resp.AttemptId != 1 || resp.Status != "Succeeded" || resp.Error != "" || resp.Output != expectedHash(req.Payload) {
		return errors.New("hash response has invalid identity, attempt, status, output or task error")
	}
	return nil
}

type persistedJob struct {
	JobID, State, AttemptID     int64
	WorkerID, TaskType, Payload string
	TimeoutMs                   int64
}

func validateRecord(req *runtimepb.SubmitJobRequest, record persistedJob) error {
	// JobSucceeded's stable SQLite numeric value is 2 (cmd/coordinator/jobs.go).
	// package main is not importable; no runtime state or schema is redefined here.
	want := persistedJob{req.JobId, 2, 1, "worker-1", req.TaskType, req.Payload, req.TimeoutMs}
	if record != want {
		return errors.New("SQLite record does not match the accepted hash job")
	}
	return nil
}

func verifySQLite(ctx context.Context, path string, req *runtimepb.SubmitJobRequest) error {
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	var r persistedJob
	err = db.QueryRowContext(ctx, `SELECT job_id, state, attempt_id, worker_id, task_type, payload, timeout_ms FROM jobs WHERE job_id = ?`, req.JobId).
		Scan(&r.JobID, &r.State, &r.AttemptID, &r.WorkerID, &r.TaskType, &r.Payload, &r.TimeoutMs)
	if err != nil {
		return fmt.Errorf("read persisted job: %w", err)
	}
	return validateRecord(req, r)
}
