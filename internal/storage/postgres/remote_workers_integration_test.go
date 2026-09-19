package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtwo56/mercury/internal/job"
)

// These cases share the existing disposable schema and migration setup. Separate
// connections exercise actual row locking rather than simulating concurrency.
func testRemoteWorkers(t *testing.T, conn *pgx.Conn, repository *JobRepository, databaseURL, qualifiedSchema string, now time.Time) {
	ctx := context.Background()
	second, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	if _, err := second.Exec(ctx, "SET search_path TO "+qualifiedSchema); err != nil {
		t.Fatal(err)
	}
	other := NewJobRepository(second)
	reset := func() {
		t.Helper()
		if _, err := conn.Exec(ctx, "TRUNCATE jobs"); err != nil {
			t.Fatal(err)
		}
	}
	create := func(id string, typ job.TaskType) {
		t.Helper()
		j := integrationJob(id, now.Add(-time.Minute))
		j.TaskType = typ
		if err := repository.Create(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	claim := func(token job.LeaseToken, at time.Time) job.Job {
		t.Helper()
		j, err := repository.ClaimNext(ctx, "worker", token, at, at.Add(time.Minute), "render")
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	t.Run("filter before locking and reject empty types", func(t *testing.T) {
		reset()
		create("a-unsupported", "other")
		create("b-supported", "render")
		create("c-supported", "render")
		for _, types := range [][]job.TaskType{nil, {}, {" "}} {
			if _, err := repository.ClaimNext(ctx, "worker", "token", now, now.Add(time.Minute), types...); err == nil {
				t.Fatal("empty/blank filter accepted")
			}
		}
		locked, err := second.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer locked.Rollback(ctx)
		if _, err := locked.Exec(ctx, "SELECT id FROM jobs WHERE id='b-supported' FOR UPDATE"); err != nil {
			t.Fatal(err)
		}
		got := claim("first", now)
		if got.ID != "c-supported" {
			t.Fatalf("claim skipped filter/lock: %s", got.ID)
		}
		if err := locked.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if got := claim("second", now); got.ID != "b-supported" {
			t.Fatalf("wrong ordering: %s", got.ID)
		}
		unsupported, err := repository.GetByID(ctx, "a-unsupported")
		if err != nil || unsupported.State != job.StateQueued || unsupported.Lease != nil {
			t.Fatal("unsupported job was claimed")
		}
	})
	t.Run("concurrent start consumes one attempt", func(t *testing.T) {
		reset()
		create("start", "render")
		j := claim("token", now)
		results := make(chan error, 2)
		gate := make(chan struct{})
		for _, repo := range []*JobRepository{repository, other} {
			go func(r *JobRepository) {
				<-gate
				_, err := r.StartExecution(ctx, j.ID, "worker", "token", now.Add(time.Second))
				results <- err
			}(repo)
		}
		close(gate)
		for range 2 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		first, err := repository.GetByID(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		replay, err := repository.StartExecution(ctx, j.ID, "worker", "token", now.Add(2*time.Second))
		if err != nil || first.AttemptsStarted != 1 || !reflect.DeepEqual(first, replay) {
			t.Fatal("start replay changed durable execution")
		}
		for _, token := range []job.LeaseToken{"stale", "token"} {
			at := now.Add(3 * time.Second)
			if token == "token" {
				at = now.Add(time.Minute)
			}
			if _, err := repository.StartExecution(ctx, j.ID, "worker", token, at); !errors.Is(err, ErrLifecycleConflict) {
				t.Fatalf("stale/expired start: %v", err)
			}
		}
	})
	for _, policy := range []string{"success", "retryable", "permanent"} {
		t.Run(policy+" lost response and delayed reports", func(t *testing.T) {
			reset()
			create("outcome", "render")
			j := claim("first", now)
			if _, err := repository.StartExecution(ctx, j.ID, "worker", "first", now); err != nil {
				t.Fatal(err)
			}
			retryAt := now.Add(2 * time.Minute)
			report := func(r *JobRepository) (job.Job, error) {
				switch policy {
				case "success":
					return r.CompleteExecution(ctx, j.ID, "worker", "first", json.RawMessage(`{"ok":true}`), now.Add(time.Second))
				case "permanent":
					return r.FailExecutionPermanently(ctx, j.ID, "worker", "first", now.Add(time.Second), "permanent")
				default:
					return r.FailExecution(ctx, j.ID, "worker", "first", now.Add(time.Second), "retryable", &retryAt)
				}
			}
			// Competing terminal reports serialize: exactly one may clear the lease.
			gate := make(chan struct{})
			results := make(chan error, 2)
			for _, r := range []*JobRepository{repository, other} {
				go func(r *JobRepository) { <-gate; _, err := report(r); results <- err }(r)
			}
			close(gate)
			successes, conflicts := 0, 0
			for range 2 {
				err := <-results
				if err == nil {
					successes++
				} else if errors.Is(err, ErrLifecycleConflict) {
					conflicts++
				} else {
					t.Fatal(err)
				}
			}
			if successes != 1 || conflicts != 1 {
				t.Fatalf("terminal outcomes=%d/%d", successes, conflicts)
			}
			stored, err := repository.GetByID(ctx, j.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := job.StateSucceeded
			if policy == "retryable" {
				want = job.StateRetryScheduled
			}
			if policy == "permanent" {
				want = job.StateFailed
			}
			if stored.State != want || stored.Lease != nil || stored.AttemptsStarted != 1 {
				t.Fatalf("reconciled state=%#v", stored)
			}
			if _, err := report(repository); !errors.Is(err, ErrLifecycleConflict) {
				t.Fatal("repeated outcome accepted")
			}
			after, err := repository.GetByID(ctx, j.ID)
			if err != nil || !reflect.DeepEqual(stored, after) {
				t.Fatal("repeat changed outcome or retry time")
			}
			if policy == "retryable" {
				newer := claim("newer", retryAt)
				if _, err := report(repository); !errors.Is(err, ErrLifecycleConflict) {
					t.Fatal("delayed report affected newer owner")
				}
				after, err := repository.GetByID(ctx, j.ID)
				if err != nil || !reflect.DeepEqual(newer, after) {
					t.Fatal("delayed report changed newer execution")
				}
			}
		})
	}
	t.Run("expired execution recovery fences every report", func(t *testing.T) {
		reset()
		create("recovered", "render")
		j := claim("expired", now)
		if _, err := repository.StartExecution(ctx, j.ID, "worker", "expired", now); err != nil {
			t.Fatal(err)
		}
		expired := now.Add(time.Minute)
		retry := expired.Add(time.Minute)
		if _, err := repository.CompleteExecution(ctx, j.ID, "worker", "expired", json.RawMessage(`null`), expired); !errors.Is(err, ErrLifecycleConflict) {
			t.Fatal("expired completion accepted")
		}
		if _, err := repository.RecoverExpiredLeases(ctx, expired, retry, 10); err != nil {
			t.Fatal(err)
		}
		newer := claim("fresh", retry)
		reports := []func() error{
			func() error { _, e := repository.StartExecution(ctx, j.ID, "worker", "expired", retry); return e },
			func() error {
				_, e := repository.RenewLeaseBounded(ctx, j.ID, "worker", "expired", retry, retry.Add(time.Minute), time.Hour)
				return e
			},
			func() error {
				_, e := repository.CompleteExecution(ctx, j.ID, "worker", "expired", json.RawMessage(`null`), retry)
				return e
			},
			func() error {
				_, e := repository.FailExecution(ctx, j.ID, "worker", "expired", retry, "failed", &retry)
				return e
			},
			func() error {
				_, e := repository.FailExecutionPermanently(ctx, j.ID, "worker", "expired", retry, "failed")
				return e
			},
		}
		for _, report := range reports {
			if err := report(); !errors.Is(err, ErrLifecycleConflict) {
				t.Fatalf("stale report error=%v", err)
			}
		}
		after, err := repository.GetByID(ctx, j.ID)
		if err != nil || !reflect.DeepEqual(newer, after) {
			t.Fatal("stale reports changed current lease")
		}
	})
	t.Run("lifecycle time is refreshed after lock acquisition", func(t *testing.T) {
		reset()
		create("waited", "render")
		j := claim("token", now)
		if _, err := repository.StartExecution(ctx, j.ID, "worker", "token", now); err != nil {
			t.Fatal(err)
		}
		// The request arrives before expiry, but its transaction sees a later
		// authoritative clock. All report paths must reject the expired lease.
		live := repository.WithLifecycleClock(func() time.Time { return now.Add(time.Minute) })
		retry := now.Add(2 * time.Minute)
		reports := []func() error{
			func() error { _, e := live.StartExecution(ctx, j.ID, "worker", "token", now); return e },
			func() error {
				_, e := live.RenewLeaseBounded(ctx, j.ID, "worker", "token", now, now.Add(time.Minute), time.Hour)
				return e
			},
			func() error {
				_, e := live.CompleteExecution(ctx, j.ID, "worker", "token", json.RawMessage(`null`), now)
				return e
			},
			func() error {
				_, e := live.FailExecution(ctx, j.ID, "worker", "token", now, "failed", &retry)
				return e
			},
			func() error {
				_, e := live.FailExecutionPermanently(ctx, j.ID, "worker", "token", now, "failed")
				return e
			},
		}
		before, err := repository.GetByID(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, report := range reports {
			if err := report(); !errors.Is(err, ErrLifecycleConflict) {
				t.Fatalf("expired report accepted: %v", err)
			}
		}
		after, err := repository.GetByID(ctx, j.ID)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("expired report changed persisted execution")
		}
	})
	t.Run("remote renewal cannot extend execution indefinitely", func(t *testing.T) {
		reset()
		j := integrationJob("bounded", now.Add(-time.Hour))
		started := now.Add(-time.Hour + 10*time.Second)
		j.State = job.StateRunning
		j.AttemptsStarted = 1
		j.StartedAt = &started
		j.Lease = &job.Lease{WorkerID: "worker", Token: "token", ExpiresAt: now.Add(time.Second)}
		if err := repository.Create(ctx, j); err != nil {
			t.Fatal(err)
		}
		renewed, err := repository.RenewLeaseBounded(ctx, j.ID, "worker", "token", now, now.Add(time.Minute), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		deadline := started.Add(time.Hour)
		if !renewed.Lease.ExpiresAt.Equal(deadline) {
			t.Fatal("execution limit not enforced")
		}
		if _, err := repository.RenewLeaseBounded(ctx, j.ID, "worker", "token", deadline, deadline.Add(time.Minute), time.Hour); !errors.Is(err, ErrLifecycleConflict) {
			t.Fatal("renewed beyond limit")
		}
	})
	reset()
}
