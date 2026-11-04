package auth

import (
	"context"
	"errors"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestStreamErrorDeliverySurvivesDisconnectCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx, finish := cliproxyexecutor.WithStreamErrorDelivery(ctx)
	notified := make(chan struct{})
	cliproxyexecutor.DeferStreamErrorNotification(ctx, func() {
		cancel()
		close(notified)
	})
	want := errors.New("upstream terminal error")
	source := make(chan cliproxyexecutor.StreamChunk, 2)
	source <- cliproxyexecutor.StreamChunk{Payload: []byte("output")}
	source <- cliproxyexecutor.StreamChunk{Err: want}
	close(source)
	result := wrapStreamErrorDelivery(ctx, &cliproxyexecutor.StreamResult{Chunks: source}, finish)
	first := <-result.Chunks
	if string(first.Payload) != "output" {
		t.Fatalf("first chunk = %+v", first)
	}
	select {
	case <-notified:
		t.Fatal("disconnect was notified before final error delivery")
	default:
	}
	final, ok := <-result.Chunks
	if !ok || !errors.Is(final.Err, want) {
		t.Fatalf("final chunk = %+v, open=%t", final, ok)
	}
	for range result.Chunks {
	}
	select {
	case <-notified:
	default:
		t.Fatal("disconnect was not notified after final error delivery")
	}
}

func TestStreamErrorDeliveryCancellationFinishesLateNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ctx, finish := cliproxyexecutor.WithStreamErrorDelivery(ctx)
	source := make(chan cliproxyexecutor.StreamChunk)
	result := wrapStreamErrorDelivery(ctx, &cliproxyexecutor.StreamResult{Chunks: source}, finish)
	cancel()
	for range result.Chunks {
	}
	close(source)
	notified := false
	cliproxyexecutor.DeferStreamErrorNotification(ctx, func() { notified = true })
	if !notified {
		t.Fatal("canceled delivery retained a late disconnect notification")
	}
}
