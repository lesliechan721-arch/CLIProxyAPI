package auth

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func (m *Manager) sameUpstreamRetryCount() int {
	if cfg := m.runtimeConfigSnapshot(); cfg != nil && cfg.SameUpstreamRetry > 0 {
		return cfg.SameUpstreamRetry
	}
	return 0
}

func (m *Manager) consumeSameUpstreamRetry(ctx context.Context, auth *Auth, err error, remaining *atomic.Int64) bool {
	if remaining.Load() <= 0 || !m.isSameUpstreamRetryable(ctx, auth, err) {
		return false
	}
	remaining.Add(-1)
	return true
}

func (m *Manager) isSameUpstreamRetryable(ctx context.Context, auth *Auth, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isRequestTerminatedError(err) || isRequestStopError(err) || isRequestInvalidError(err) || isCloudflareChallengeError(err) {
		return false
	}
	if isTOSViolationError(err) || isAntigravityValidationRequiredError(err) || cliproxyexecutor.IsUpstreamWebsocketTurnError(err) {
		return false
	}
	if hint := trustedRetryAfterHint(err); hint != nil && *hint > 0 {
		return false
	}
	if _, matched := matchRequestScopedErrorAction(auth, err, m.requestScopedErrorConfig(ctx)); matched {
		return false
	}
	status := statusCodeFromError(err)
	return isSameUpstreamRetryStatus(status) || status == 0 && (isTransientTransportError(err) || isConnectionLifecycleError(err))
}

func isSameUpstreamRetryStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, 520, 521, 522, 523, 524, 525, 526:
		return true
	default:
		return false
	}
}

// Some executors only parse Retry-After on 429s. Preserve a valid hint on a
// transient error from the current attempt so the usual cooldown can use it.
func (m *Manager) applySameUpstreamRetryAfter(ctx context.Context, err error, streamHeaders http.Header) error {
	if err == nil || m.sameUpstreamRetryCount() == 0 || !isSameUpstreamRetryStatus(statusCodeFromError(err)) || trustedRetryAfterHint(err) != nil {
		return err
	}
	raw := strings.TrimSpace(logging.GetResponseHeaders(ctx).Get("Retry-After"))
	if raw == "" {
		raw = strings.TrimSpace(streamHeaders.Get("Retry-After"))
	}
	if raw == "" {
		return err
	}
	var delay time.Duration
	if seconds, errParse := strconv.ParseUint(raw, 10, 64); errParse == nil {
		delay = time.Duration(min(seconds, uint64(math.MaxInt64/int64(time.Second)))) * time.Second
	} else if deadline, errParseTime := http.ParseTime(raw); errParseTime == nil {
		delay = time.Until(deadline)
	}
	return withTrustedRetryAfter(err, &delay)
}

type sameUpstreamRetryKey struct {
	authID string
	model  string
}

func (m *Manager) sameUpstreamRetryBudget(budgets map[sameUpstreamRetryKey]*atomic.Int64, authID, model string, enabled bool) *atomic.Int64 {
	key := sameUpstreamRetryKey{authID: authID, model: model}
	if remaining := budgets[key]; remaining != nil {
		return remaining
	}
	remaining := &atomic.Int64{}
	if enabled {
		remaining.Store(int64(m.sameUpstreamRetryCount()))
	}
	budgets[key] = remaining
	return remaining
}

func (m *Manager) deferUpstreamFailures(ctx context.Context, currentAuth func() *Auth, opts *cliproxyexecutor.Options, remaining *atomic.Int64) func() {
	if remaining.Load() <= 0 {
		return func() {}
	}
	handler := &sameUpstreamFailureHandler{active: true, retryable: func(err error) bool {
		return remaining.Load() > 0 && m.isSameUpstreamRetryable(ctx, currentAuth(), err)
	}}
	opts.UpstreamFailureHandler = handler
	return handler.Commit
}

type sameUpstreamFailureHandler struct {
	mu        sync.Mutex
	active    bool
	retryable func(error) bool
	pending   func()
}

func (h *sameUpstreamFailureHandler) Defer(err error, finish func()) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.active || !h.retryable(err) {
		return false
	}
	h.pending = finish
	return true
}

func (h *sameUpstreamFailureHandler) Recovered() {
	h.mu.Lock()
	h.pending = nil
	h.mu.Unlock()
}

func (h *sameUpstreamFailureHandler) Commit() {
	h.mu.Lock()
	h.active = false
	finish := h.pending
	h.pending = nil
	h.mu.Unlock()
	if finish != nil {
		finish()
	}
}

// executeWithSameUpstreamRetry defers result recording until the selected
// credential/model has recovered or exhausted its additional attempts.
func executeWithSameUpstreamRetry[T any](ctx context.Context, m *Manager, currentAuth func() *Auth, remaining *atomic.Int64, execute func(context.Context) (T, error)) (T, context.Context, error) {
	for {
		response, errExecute := execute(ctx)
		errExecute = markUpstreamExecutionAttemptFromContext(ctx, errExecute)
		errExecute = m.applySameUpstreamRetryAfter(ctx, errExecute, nil)
		if !m.consumeSameUpstreamRetry(ctx, currentAuth(), errExecute, remaining) {
			return response, ctx, errExecute
		}
		ctx = newUpstreamAttemptContext(ctx)
	}
}
