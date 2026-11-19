package auth

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestExecuteStreamDeliversTerminalErrorBeforeDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager := NewManager(nil, &FillFirstSelector{}, nil)
		const provider, model = "stream-delivery", "stream-delivery-model"
		registerRetryRoundLocalAuths(t, manager, provider, model, map[string]int{"stream-delivery-auth": 0})
		ctx, cancel := context.WithCancel(cliproxyexecutor.WithDownstreamWebsocket(t.Context()))
		defer cancel()
		want := errors.New("upstream terminal error")
		manager.RegisterExecutor(&customStreamMockExecutor{
			identifier: provider,
			streamFn: func(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
				if !cliproxyexecutor.DeferStreamErrorNotification(ctx, cancel) {
					t.Fatal("disconnect notification was not deferred")
				}
				chunks := make(chan cliproxyexecutor.StreamChunk, 2)
				chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("output")}
				chunks <- cliproxyexecutor.StreamChunk{Err: want}
				close(chunks)
				return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
			},
		})

		stream, errStream := manager.ExecuteStream(ctx, []string{provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
		if errStream != nil {
			t.Fatal(errStream)
		}
		if first := <-stream.Chunks; string(first.Payload) != "output" {
			t.Fatalf("first chunk = %+v", first)
		}
		synctest.Wait()
		if ctx.Err() != nil {
			t.Fatal("disconnect canceled the stream before terminal error delivery")
		}
		if got := manager.InFlightSnapshot().InFlight; got != 1 {
			t.Fatalf("in-flight requests before terminal error delivery = %d, want 1", got)
		}
		if final, ok := <-stream.Chunks; !ok || !errors.Is(final.Err, want) {
			t.Fatalf("final chunk = %+v, open=%t", final, ok)
		}
		for range stream.Chunks {
		}
		if ctx.Err() != context.Canceled || manager.InFlightSnapshot().InFlight != 0 {
			t.Fatal("stream end did not notify the disconnect and release the in-flight count")
		}
	})
}

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
