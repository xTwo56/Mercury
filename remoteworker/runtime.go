// Package remoteworker runs application handlers against Mercury over HTTP.
//
// In simple terms, Runtime reserves a local execution slot, claims compatible
// work, confirms the start, keeps the lease alive, invokes the handler, and
// reports one outcome. More precisely, it treats lease tokens as fencing
// credentials, reconciles ambiguous HTTP responses through inspection, and
// never reruns a handler or blindly repeats a terminal report.
package remoteworker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

var (
	// ErrAlreadyRunning means Run was called more than once on one Runtime.
	ErrAlreadyRunning = errors.New("remote worker runtime already started")
	// ErrShutdownTimeout means one or more handlers ignored cancellation past
	// the configured graceful-shutdown bound.
	ErrShutdownTimeout = errors.New("remote worker graceful shutdown timed out")
	errHandlerPanic    = errors.New("task handler panicked")
)

// V1LeaseDuration is the server-controlled lease duration in Mercury's current
// remote-worker protocol. It defines the safe upper bound for heartbeat cadence
// and the minimum capacity hold after an uncertain claim.
const V1LeaseDuration = time.Minute

// LifecycleClient is the HTTP lifecycle surface required by Runtime. The
// concrete implementation is client.Client; this interface also permits
// deterministic application tests without a database or HTTP server.
type LifecycleClient interface {
	Claim(context.Context, workerclient.WorkerID, []workerclient.TaskType) (workerclient.Job, error)
	Start(context.Context, workerclient.JobID, workerclient.WorkerID, workerclient.LeaseToken) (workerclient.Job, error)
	Heartbeat(context.Context, workerclient.JobID, workerclient.WorkerID, workerclient.LeaseToken) (workerclient.Job, error)
	Complete(context.Context, workerclient.JobID, workerclient.WorkerID, workerclient.LeaseToken, json.RawMessage) (workerclient.Job, error)
	Fail(context.Context, workerclient.JobID, workerclient.WorkerID, workerclient.LeaseToken, workerclient.FailureClassification, string) (workerclient.Job, error)
	Inspect(context.Context, workerclient.JobID) (workerclient.Job, error)
}

// Config controls polling, lease renewal, local concurrency, and shutdown.
// UncertainClaimHold should be at least Mercury's claim lease duration because
// a lost claim response provides no job ID that can be inspected.
type Config struct {
	WorkerID           workerclient.WorkerID
	Concurrency        int
	PollInterval       time.Duration
	HeartbeatInterval  time.Duration
	UncertainClaimHold time.Duration
	ShutdownTimeout    time.Duration
}

// Runtime coordinates bounded remote execution. It acquires capacity before
// claiming and retains that slot until the handler exits, even after ownership
// loss. Context cancellation asks a handler to stop but cannot forcibly stop Go
// code that ignores its context.
type Runtime struct {
	client   LifecycleClient
	registry *Registry
	logger   *slog.Logger
	config   Config
	clock    runtimeClock

	slots  chan struct{}
	active sync.WaitGroup
	mu     sync.Mutex
	ran    bool
}

// New validates dependencies and creates a stopped Runtime. Register all task
// handlers before calling Run; Run seals the registry and rejects an empty one.
func New(lifecycle LifecycleClient, registry *Registry, config Config, logger *slog.Logger) (*Runtime, error) {
	return newRuntime(lifecycle, registry, config, logger, systemClock{})
}

func newRuntime(lifecycle LifecycleClient, registry *Registry, config Config, logger *slog.Logger, clock runtimeClock) (*Runtime, error) {
	if lifecycle == nil || registry == nil || clock == nil {
		return nil, errors.New("remote worker runtime dependencies must not be nil")
	}
	if config.WorkerID == "" {
		return nil, errors.New("remote worker ID must not be empty")
	}
	if config.Concurrency <= 0 {
		return nil, errors.New("remote worker concurrency must be positive")
	}
	if config.PollInterval <= 0 || config.HeartbeatInterval <= 0 || config.UncertainClaimHold <= 0 || config.ShutdownTimeout <= 0 {
		return nil, errors.New("remote worker timing values must be positive")
	}
	if config.HeartbeatInterval >= V1LeaseDuration {
		return nil, errors.New("remote worker heartbeat interval must be shorter than the server lease duration")
	}
	if config.UncertainClaimHold < V1LeaseDuration {
		return nil, errors.New("remote worker uncertain-claim hold must cover the server lease duration")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Runtime{
		client: lifecycle, registry: registry, logger: logger, config: config,
		clock: clock, slots: make(chan struct{}, config.Concurrency),
	}, nil
}

// Run claims immediately, then polls until ctx is cancelled. Shutdown stops new
// claims, cancels active handler and heartbeat contexts, and waits up to
// ShutdownTimeout. A timed-out handler may continue running because Go provides
// no safe mechanism for forcibly terminating an uncooperative goroutine.
func (runtime *Runtime) Run(ctx context.Context) error {
	runtime.mu.Lock()
	if runtime.ran {
		runtime.mu.Unlock()
		return ErrAlreadyRunning
	}
	runtime.ran = true
	runtime.mu.Unlock()

	types := runtime.registry.sealAndTypes()
	if len(types) == 0 {
		return errors.New("remote worker requires at least one registered task handler")
	}
	executionCtx, cancelExecutions := context.WithCancel(ctx)
	defer cancelExecutions()
	runtime.fillSlots(executionCtx, types)
	ticker := runtime.clock.NewTicker(runtime.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancelExecutions()
			return runtime.waitForShutdown()
		case <-ticker.C():
			runtime.fillSlots(executionCtx, types)
		}
	}
}

func (runtime *Runtime) waitForShutdown() error {
	done := make(chan struct{})
	go func() {
		runtime.active.Wait()
		close(done)
	}()
	timer := runtime.clock.NewTimer(runtime.config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C():
		return ErrShutdownTimeout
	}
}

func (runtime *Runtime) fillSlots(ctx context.Context, types []workerclient.TaskType) {
	for ctx.Err() == nil {
		select {
		case runtime.slots <- struct{}{}:
		default:
			return
		}
		if !runtime.claimOne(ctx, types) {
			return
		}
	}
}

func (runtime *Runtime) claimOne(ctx context.Context, types []workerclient.TaskType) bool {
	release := func() { <-runtime.slots }
	claimed, err := runtime.client.Claim(ctx, runtime.config.WorkerID, types)
	if err != nil {
		if errors.Is(err, workerclient.ErrNoJobAvailable) {
			release()
			return false
		}
		if workerclient.OutcomeUncertain(err) {
			// A lost claim response has no ID to inspect. Keep this capacity occupied
			// until the possible server lease expires instead of claiming again.
			runtime.active.Add(1)
			go func() {
				defer runtime.active.Done()
				defer release()
				timer := runtime.clock.NewTimer(runtime.config.UncertainClaimHold)
				defer timer.Stop()
				select {
				case <-ctx.Done():
				case <-timer.C():
				}
			}()
			return true
		}
		release()
		if ctx.Err() == nil {
			runtime.logError(ctx, "claim remote job", "", "", err)
		}
		return false
	}
	runtime.active.Add(1)
	go func() {
		defer runtime.active.Done()
		defer release()
		runtime.execute(ctx, claimed)
	}()
	return true
}

func (runtime *Runtime) execute(parent context.Context, claimed workerclient.Job) {
	started, ok := runtime.confirmStart(parent, claimed)
	if !ok || started.Lease == nil {
		return
	}
	handler, err := runtime.registry.lookup(started.TaskType)
	if err != nil {
		// Claims are filtered by the sealed registry. A mismatched response is a
		// protocol fault; do not execute or invent a lifecycle transition.
		runtime.logError(parent, "resolve remote task handler", started.ID, started.TaskType, err)
		return
	}

	var handlerCtx context.Context
	var cancelHandler context.CancelFunc
	if started.ExecutionDeadline == nil {
		handlerCtx, cancelHandler = context.WithCancel(parent)
	} else {
		// Expose the authoritative server deadline through Context.Deadline while
		// the injected timer below makes cancellation deterministic in tests.
		handlerCtx, cancelHandler = context.WithDeadline(parent, *started.ExecutionDeadline)
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(parent)
	defer cancelHandler()
	defer cancelHeartbeat()

	var deadlineReached atomic.Bool
	deadlineDone := make(chan struct{})
	deadlineStopped := make(chan struct{})
	if started.ExecutionDeadline != nil {
		timer := runtime.clock.NewTimer(durationUntil(runtime.clock.Now(), *started.ExecutionDeadline))
		defer timer.Stop()
		go func() {
			defer close(deadlineStopped)
			select {
			case <-timer.C():
				deadlineReached.Store(true)
				cancelHandler()
				cancelHeartbeat()
			case <-deadlineDone:
			}
		}()
	}

	heartbeatDone := make(chan heartbeatResult, 1)
	go func() { heartbeatDone <- runtime.heartbeat(heartbeatCtx, cancelHandler, started) }()
	result, executionErr, panicked := invokeHandler(handlerCtx, handler, started.Payload)
	cancelHeartbeat()
	heartbeat := <-heartbeatDone
	close(deadlineDone)
	if started.ExecutionDeadline != nil {
		<-deadlineStopped
	}
	cancelHandler()

	// Joining the heartbeat before reporting prevents renewal from racing a
	// terminal write that clears the same lease.
	if heartbeat.ownershipLost || deadlineReached.Load() || executionDeadlineReached(started, runtime.clock.Now()) || parent.Err() != nil || !runtime.clock.Now().Before(heartbeat.confirmedExpiration) {
		return
	}
	if panicked {
		runtime.reportFailure(parent, started, workerclient.FailurePermanent, errHandlerPanic.Error())
		return
	}
	if executionErr != nil {
		classification, message := classifyFailure(executionErr)
		runtime.reportFailure(parent, started, classification, message)
		return
	}
	if !json.Valid(result) {
		runtime.reportFailure(parent, started, workerclient.FailurePermanent, "task handler returned invalid JSON")
		return
	}
	runtime.reportCompletion(parent, started, result)
}

func (runtime *Runtime) confirmStart(ctx context.Context, claimed workerclient.Job) (workerclient.Job, bool) {
	if claimed.ID == "" || claimed.TaskType == "" || claimed.State != workerclient.StateLeased || claimed.Lease == nil || claimed.Lease.Token == "" || !sameOwner(claimed.Lease, runtime.config.WorkerID, claimed.Lease.Token) || !runtime.clock.Now().Before(claimed.Lease.ExpiresAt) {
		runtime.logError(ctx, "validate claimed remote job", claimed.ID, claimed.TaskType, workerclient.ErrProtocol)
		return workerclient.Job{}, false
	}
	startCtx, cancelStart := context.WithDeadline(ctx, claimed.Lease.ExpiresAt)
	started, err := runtime.client.Start(startCtx, claimed.ID, runtime.config.WorkerID, claimed.Lease.Token)
	cancelStart()
	if err == nil {
		if validStartedExecution(started, runtime.config.WorkerID, claimed.Lease.Token, runtime.clock.Now()) {
			return started, true
		}
		runtime.logError(ctx, "validate remote start response", claimed.ID, claimed.TaskType, workerclient.ErrProtocol)
		return workerclient.Job{}, false
	}
	if !workerclient.OutcomeUncertain(err) {
		if ctx.Err() == nil {
			runtime.logError(ctx, "start remote job", claimed.ID, claimed.TaskType, err)
		}
		return workerclient.Job{}, false
	}
	// Start is replay-safe on the server, but inspection is sufficient and avoids
	// another state-changing request after an ambiguous response.
	inspected, inspectErr := runtime.client.Inspect(ctx, claimed.ID)
	if inspectErr == nil && validStartedExecution(inspected, runtime.config.WorkerID, claimed.Lease.Token, runtime.clock.Now()) {
		return inspected, true
	}
	if ctx.Err() == nil {
		runtime.logError(ctx, "reconcile uncertain remote start", claimed.ID, claimed.TaskType, firstError(inspectErr, err))
	}
	return workerclient.Job{}, false
}

type heartbeatResult struct {
	confirmedExpiration time.Time
	ownershipLost       bool
}

func (runtime *Runtime) heartbeat(ctx context.Context, cancelHandler context.CancelFunc, started workerclient.Job) heartbeatResult {
	confirmed := started.Lease.ExpiresAt
	ticker := runtime.clock.NewTicker(runtime.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return heartbeatResult{confirmedExpiration: confirmed}
		case <-ticker.C():
			now := runtime.clock.Now()
			if !now.Before(confirmed) {
				cancelHandler()
				return heartbeatResult{confirmedExpiration: confirmed, ownershipLost: true}
			}
			renewCtx, cancelRenew := context.WithDeadline(ctx, confirmed)
			renewed, err := runtime.client.Heartbeat(renewCtx, started.ID, runtime.config.WorkerID, started.Lease.Token)
			cancelRenew()
			if err != nil {
				if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					return heartbeatResult{confirmedExpiration: confirmed}
				}
				if errors.Is(err, workerclient.ErrOwnershipLost) || errors.Is(err, workerclient.ErrAuthentication) || (!workerclient.OutcomeUncertain(err) && errors.Is(err, workerclient.ErrProtocol)) {
					cancelHandler()
					return heartbeatResult{confirmedExpiration: confirmed, ownershipLost: true}
				}
				if workerclient.OutcomeUncertain(err) {
					inspected, inspectErr := runtime.client.Inspect(ctx, started.ID)
					if inspectErr == nil && validRunningOwnership(inspected, runtime.config.WorkerID, started.Lease.Token, runtime.clock.Now()) {
						if inspected.Lease.ExpiresAt.After(confirmed) {
							confirmed = inspected.Lease.ExpiresAt
						}
						continue
					}
					if errors.Is(inspectErr, workerclient.ErrOwnershipLost) || errors.Is(inspectErr, workerclient.ErrAuthentication) || errors.Is(inspectErr, workerclient.ErrJobNotFound) {
						cancelHandler()
						return heartbeatResult{confirmedExpiration: confirmed, ownershipLost: true}
					}
				}
				if !runtime.clock.Now().Before(confirmed) {
					cancelHandler()
					return heartbeatResult{confirmedExpiration: confirmed, ownershipLost: true}
				}
				runtime.logError(ctx, "renew remote job lease", started.ID, started.TaskType, err)
				continue
			}
			if !validRunningOwnership(renewed, runtime.config.WorkerID, started.Lease.Token, now) || !renewed.Lease.ExpiresAt.After(confirmed) {
				cancelHandler()
				return heartbeatResult{confirmedExpiration: confirmed, ownershipLost: true}
			}
			confirmed = renewed.Lease.ExpiresAt
		}
	}
}

func (runtime *Runtime) reportCompletion(ctx context.Context, started workerclient.Job, result json.RawMessage) {
	_, err := runtime.client.Complete(ctx, started.ID, runtime.config.WorkerID, started.Lease.Token, result)
	if err == nil {
		return
	}
	if workerclient.OutcomeUncertain(err) {
		// Inspection is read-only. It may show the accepted terminal result, but
		// absence of proof never causes this runtime to replay the completion.
		_, _ = runtime.client.Inspect(ctx, started.ID)
	}
	if ctx.Err() == nil {
		runtime.logError(ctx, "report remote job completion", started.ID, started.TaskType, err)
	}
}

func (runtime *Runtime) reportFailure(ctx context.Context, started workerclient.Job, classification workerclient.FailureClassification, message string) {
	_, err := runtime.client.Fail(ctx, started.ID, runtime.config.WorkerID, started.Lease.Token, classification, boundedMessage(message))
	if err == nil {
		return
	}
	if workerclient.OutcomeUncertain(err) {
		_, _ = runtime.client.Inspect(ctx, started.ID)
	}
	if ctx.Err() == nil {
		runtime.logError(ctx, "report remote job failure", started.ID, started.TaskType, err)
	}
}

// Failure marks a handler error as retryable or permanent. Unwrapped handler
// errors default to retryable so transient application failures do not
// accidentally exhaust a job immediately.
type Failure struct {
	Classification workerclient.FailureClassification
	Err            error
}

func (failure *Failure) Error() string {
	if failure.Err == nil {
		return string(failure.Classification) + " task failure"
	}
	return failure.Err.Error()
}

func (failure *Failure) Unwrap() error { return failure.Err }

// Retryable marks err for Mercury-managed retry scheduling.
func Retryable(err error) error {
	return &Failure{Classification: workerclient.FailureRetryable, Err: err}
}

// Permanent marks err as terminal regardless of remaining attempt budget.
func Permanent(err error) error {
	return &Failure{Classification: workerclient.FailurePermanent, Err: err}
}

func classifyFailure(err error) (workerclient.FailureClassification, string) {
	classification := workerclient.FailureRetryable
	var failure *Failure
	if errors.As(err, &failure) && failure.Classification == workerclient.FailurePermanent {
		classification = workerclient.FailurePermanent
	}
	return classification, boundedMessage(err.Error())
}

func boundedMessage(message string) string {
	const limit = 4000
	if len(message) <= limit {
		return message
	}
	return message[:limit]
}

func invokeHandler(ctx context.Context, handler Handler, payload json.RawMessage) (result json.RawMessage, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			result = nil
			err = errHandlerPanic
			panicked = true
		}
	}()
	result, err = handler.Execute(ctx, append(json.RawMessage(nil), payload...))
	return result, err, false
}

func validRunningOwnership(job workerclient.Job, workerID workerclient.WorkerID, token workerclient.LeaseToken, now time.Time) bool {
	return job.State == workerclient.StateRunning && job.Lease != nil && sameOwner(job.Lease, workerID, token) && now.Before(job.Lease.ExpiresAt)
}

func validStartedExecution(job workerclient.Job, workerID workerclient.WorkerID, token workerclient.LeaseToken, now time.Time) bool {
	return validRunningOwnership(job, workerID, token, now) && job.ExecutionDeadline != nil && now.Before(*job.ExecutionDeadline)
}

func executionDeadlineReached(job workerclient.Job, now time.Time) bool {
	return job.ExecutionDeadline == nil || !now.Before(*job.ExecutionDeadline)
}

func sameOwner(lease *workerclient.Lease, workerID workerclient.WorkerID, token workerclient.LeaseToken) bool {
	return lease != nil && lease.WorkerID == workerID && lease.Token == token
}

func durationUntil(now, deadline time.Time) time.Duration {
	if !deadline.After(now) {
		return 0
	}
	return deadline.Sub(now)
}

func firstError(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}

func (runtime *Runtime) logError(ctx context.Context, message string, id workerclient.JobID, taskType workerclient.TaskType, err error) {
	attributes := []any{"error", err}
	if id != "" {
		attributes = append(attributes, "job_id", id)
	}
	if taskType != "" {
		attributes = append(attributes, "task_type", taskType)
	}
	runtime.logger.ErrorContext(ctx, message, attributes...)
}

type runtimeClock interface {
	Now() time.Time
	NewTicker(time.Duration) runtimeTicker
	NewTimer(time.Duration) runtimeTimer
}

type runtimeTicker interface {
	C() <-chan time.Time
	Stop()
}

type runtimeTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTicker(interval time.Duration) runtimeTicker {
	return systemTicker{Ticker: time.NewTicker(interval)}
}
func (systemClock) NewTimer(interval time.Duration) runtimeTimer {
	return systemTimer{Timer: time.NewTimer(interval)}
}

type systemTicker struct{ *time.Ticker }

func (ticker systemTicker) C() <-chan time.Time { return ticker.Ticker.C }

type systemTimer struct{ *time.Timer }

func (timer systemTimer) C() <-chan time.Time { return timer.Timer.C }

// Compile-time checks keep the concrete timer wrappers aligned with test clocks.
var (
	_ runtimeTicker = systemTicker{}
	_ runtimeTimer  = systemTimer{}
)
