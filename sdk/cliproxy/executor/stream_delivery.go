package executor

import (
	"context"
	"sync"
)

type streamErrorDeliveryKey struct{}

type streamErrorDelivery struct {
	mu       sync.Mutex
	finished bool
	pending  []func()
}

// WithStreamErrorDelivery lets a stream owner delay disconnect notifications until
// its output has been handed to its caller, or the caller cancels the stream.
// The owner must call finish on every exit, including failure before streaming.
func WithStreamErrorDelivery(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	delivery := &streamErrorDelivery{}
	finish := func() {
		delivery.mu.Lock()
		delivery.finished = true
		pending := delivery.pending
		delivery.pending = nil
		delivery.mu.Unlock()
		for _, notify := range pending {
			notify()
		}
	}
	return context.WithValue(ctx, streamErrorDeliveryKey{}, delivery), finish
}

// DeferStreamErrorNotification queues notify at the enclosing stream owner's
// delivery boundary. It returns false when no such owner is present.
// Notifications registered after the owner finishes run immediately.
func DeferStreamErrorNotification(ctx context.Context, notify func()) bool {
	if ctx == nil || notify == nil {
		return false
	}
	delivery, ok := ctx.Value(streamErrorDeliveryKey{}).(*streamErrorDelivery)
	if !ok {
		return false
	}
	delivery.mu.Lock()
	if !delivery.finished {
		delivery.pending = append(delivery.pending, notify)
		delivery.mu.Unlock()
		return true
	}
	delivery.mu.Unlock()
	notify()
	return true
}
