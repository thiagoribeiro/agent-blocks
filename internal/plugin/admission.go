package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
)

// Admission hands committed operations to Trama for publication.
//
// The operations table is the queue: an operation is waiting while it is not
// done and admitted_at is NULL. Workers claim one row at a time (FOR UPDATE SKIP
// LOCKED), so several workers never admit the same operation. A click only
// wakes a worker; it never waits for reconciliation or for Trama status polls.
//
// Trama runs publish+complete with its own retries and calls
// /operations/{id}/failed (onFailureCallback) when they are exhausted. That
// requeues the operation with backoff. Sweep is the last-resort net for lost
// callbacks and restarts.

const (
	admissionTimeout = 10 * time.Second
	// staleAdmission is how long an admitted operation may stay unfinished before
	// Sweep asks Trama about it (lost callback, plugin restart).
	staleAdmission   = 2 * time.Minute
	maxAdmissionIdle = 30 * time.Second
)

func (s *Store) migrateAdmission(ctx context.Context) error {
	q := fmt.Sprintf(`ALTER TABLE %[1]s
 ADD COLUMN IF NOT EXISTS admitted_at timestamptz,
 ADD COLUMN IF NOT EXISTS admission_failures integer NOT NULL DEFAULT 0,
 ADD COLUMN IF NOT EXISTS admit_after timestamptz;
 UPDATE %[1]s SET admitted_at=created_at WHERE admitted_at IS NULL AND admission_failures=0 AND executions<>'[]'::jsonb;
 CREATE INDEX IF NOT EXISTS operations_unadmitted ON %[1]s (created_at, id) WHERE NOT done AND admitted_at IS NULL;`, s.table("operations"))
	_, err := s.Pool.Exec(ctx, q)
	return err
}

// ClaimAdmission marks the oldest waiting operation as admitted and returns it.
func (s *Store) ClaimAdmission(ctx context.Context) (string, bool, error) {
	var id string
	err := s.Pool.QueryRow(ctx, fmt.Sprintf(`UPDATE %[1]s SET admitted_at=clock_timestamp() WHERE id=(
 SELECT id FROM %[1]s WHERE NOT done AND admitted_at IS NULL AND (admit_after IS NULL OR admit_after<=clock_timestamp())
 ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id`, s.table("operations"))).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return id, err == nil, err
}

// AdmissionFailed puts the operation back in the queue with exponential backoff
// (1 s doubling up to 5 min) and returns how many times it has failed.
func (s *Store) AdmissionFailed(ctx context.Context, id string) (int, error) {
	var failures int
	err := s.Pool.QueryRow(ctx, fmt.Sprintf(`UPDATE %s SET admitted_at=NULL, admission_failures=admission_failures+1,
 admit_after=clock_timestamp() + interval '1 second' * least(300, power(2, admission_failures))
 WHERE id=$1 AND NOT done RETURNING admission_failures`, s.table("operations")), id).Scan(&failures)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return failures, err
}

// NextAdmission is how long workers may sleep before a backoff expires.
func (s *Store) NextAdmission(ctx context.Context) (time.Duration, error) {
	var next *time.Time
	err := s.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT min(coalesce(admit_after, created_at)) FROM %s WHERE NOT done AND admitted_at IS NULL`, s.table("operations"))).Scan(&next)
	if err != nil || next == nil {
		return maxAdmissionIdle, err
	}
	return min(max(time.Until(*next), 0), maxAdmissionIdle), nil
}

// StaleAdmissions lists admitted operations still unfinished after age.
func (s *Store) StaleAdmissions(ctx context.Context, age time.Duration) ([]string, error) {
	rows, err := s.Pool.Query(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE NOT done AND admitted_at IS NOT NULL
 AND admitted_at < clock_timestamp() - $1::interval ORDER BY admitted_at LIMIT 50`, s.table("operations")), age.String())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// PublicationTimes returns when the operation was committed and last admitted.
func (s *Store) PublicationTimes(ctx context.Context, id string) (created time.Time, admitted *time.Time, err error) {
	err = s.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT created_at, admitted_at FROM %s WHERE id=$1`, s.table("operations")), id).Scan(&created, &admitted)
	return
}

// NotifyAdmissions wakes one admission worker without blocking.
func (r *Runtime) NotifyAdmissions() {
	if r.Admissions == nil {
		return
	}
	select {
	case r.Admissions <- struct{}{}:
	default:
	}
}

// RunAdmissions serves the admission queue with the given number of workers.
func (r *Runtime) RunAdmissions(ctx context.Context, workers int) {
	for range max(workers, 1) {
		go r.admissionWorker(ctx)
	}
	r.NotifyAdmissions()
}

func (r *Runtime) admissionWorker(ctx context.Context) {
	for {
		for ctx.Err() == nil {
			id, ok, err := r.Store.ClaimAdmission(ctx)
			if err != nil {
				slog.Warn("admission claim failed", "plugin", r.Store.Name, "error", err)
				break
			}
			if !ok {
				break
			}
			// More may be waiting: let an idle worker join while this one admits.
			r.NotifyAdmissions()
			r.Admit(ctx, id)
		}
		wait, err := r.Store.NextAdmission(ctx)
		if err != nil {
			slog.Warn("admission schedule failed", "plugin", r.Store.Name, "error", err)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-r.Admissions:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// Admit starts the publication workflow of one claimed operation. On failure the
// operation goes back to the queue with backoff.
func (r *Runtime) Admit(ctx context.Context, id string) {
	call, cancel := context.WithTimeout(ctx, admissionTimeout)
	defer cancel()
	var resp struct {
		ID string `json:"id"`
	}
	_, err := r.call(call, "POST", r.TramaURL+"/workflows/run", "", map[string]any{"definition": r.ResultDefinition(id), "payload": map[string]any{}}, &resp)
	if err == nil && resp.ID == "" {
		err = fmt.Errorf("missing Trama execution id")
	}
	if err == nil {
		err = r.Store.Track(call, id, resp.ID)
	}
	if err != nil {
		r.requeue(context.WithoutCancel(ctx), id, "admission failed", err)
	}
}

// PublicationFailed handles Trama's onFailureCallback: retries were exhausted.
func (r *Runtime) PublicationFailed(ctx context.Context, id string) error {
	r.requeue(ctx, id, "publication failed in Trama", nil)
	return nil
}

func (r *Runtime) requeue(ctx context.Context, id, reason string, cause error) {
	failures, err := r.Store.AdmissionFailed(ctx, id)
	if err != nil {
		slog.Error("admission requeue failed", "plugin", r.Store.Name, "operation", id, "error", err)
		return
	}
	slog.Warn(reason, "plugin", r.Store.Name, "operation", id, "attempt", failures, "error", cause)
	r.NotifyAdmissions()
}

// Sweep is the safety net: it asks Trama about operations admitted long ago
// that never completed, and requeues those whose execution failed or vanished.
func (r *Runtime) Sweep(ctx context.Context) {
	ids, err := r.Store.StaleAdmissions(ctx, staleAdmission)
	if err != nil {
		slog.Warn("sweep failed", "plugin", r.Store.Name, "error", err)
		return
	}
	for _, id := range ids {
		if err := r.sweepOne(ctx, id); err != nil {
			slog.Warn("sweep operation failed", "plugin", r.Store.Name, "operation", id, "error", err)
		}
	}
}

func (r *Runtime) sweepOne(ctx context.Context, id string) error {
	op, err := r.Store.Operation(ctx, id)
	if err != nil {
		return err
	}
	if len(op.Executions) == 0 {
		r.requeue(ctx, id, "admitted operation has no execution", nil)
		return nil
	}
	var wf map[string]any
	code, err := r.call(ctx, "GET", r.TramaURL+"/workflows/"+url.PathEscape(op.Executions[len(op.Executions)-1]), "", nil, &wf)
	if code == 404 || code == 204 {
		r.requeue(ctx, id, "execution missing in Trama", nil)
		return nil
	}
	if err != nil {
		return err
	}
	switch str(wf, "status") {
	case "SUCCEEDED":
		return r.Store.Complete(ctx, id)
	case "FAILED":
		r.requeue(ctx, id, "execution failed in Trama", nil)
	}
	return nil
}
