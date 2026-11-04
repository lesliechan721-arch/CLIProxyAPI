package executor

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestStreamErrorNotificationsFinishConcurrently(t *testing.T) {
	ctx, finish := WithStreamErrorDelivery(t.Context())
	var notified atomic.Int32
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			if !DeferStreamErrorNotification(ctx, func() { notified.Add(1) }) {
				t.Error("stream owner was not found")
			}
		})
		workers.Go(finish)
	}
	workers.Wait()
	finish()
	if got := notified.Load(); got != 32 {
		t.Fatalf("notifications = %d, want 32", got)
	}
}
