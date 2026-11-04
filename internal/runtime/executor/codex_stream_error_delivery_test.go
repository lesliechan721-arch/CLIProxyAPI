package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestCodexManagerErrorSurvivesDisconnectCancellation(t *testing.T) {
	for _, buffering := range []bool{false, true} {
		for _, failure := range []struct {
			frame  string
			status int
		}{
			{`{"type":"error","status":503,"error":{"type":"server_error","message":"upstream overloaded"}}`, http.StatusServiceUnavailable},
			{`{"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"type":"server_error","status":500,"message":"upstream processing failed"}}}`, http.StatusInternalServerError},
		} {
			t.Run(fmt.Sprintf("buffering=%t/status=%d", buffering, failure.status), func(t *testing.T) {
				server := codexWebsocketServerHoldingConnection(t, codexCreatedEvent,
					`{"type":"response.output_text.delta","delta":"hello"}`, failure.frame)
				defer server.Close()
				cfg := codexBufferingConfig(buffering)
				exec := NewCodexWebsocketsExecutor(cfg)
				exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
				manager := cliproxyauth.NewManager(nil, nil, nil)
				manager.SetConfig(cfg)
				manager.SetRetryConfig(0, 0, 0)
				manager.RegisterExecutor(exec)
				defer manager.CloseExecutionSession(t.Name())
				auth := codexTestAuth(server.URL)
				auth.ID, auth.Provider = t.Name(), "codex"
				auth.Attributes["websockets"] = "true"
				req, opts := codexWebsocketRequest()
				models := registry.GetGlobalRegistry()
				models.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: req.Model}})
				defer models.UnregisterClient(auth.ID)
				if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
					t.Fatal(errRegister)
				}
				opts.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: t.Name()}
				waitCtx, stopWaiting := context.WithTimeout(t.Context(), 5*time.Second)
				defer stopWaiting()
				ctx, cancel := context.WithCancel(cliproxyexecutor.WithDownstreamWebsocket(waitCtx))
				defer cancel()
				notified := make(chan struct{})
				go func() {
					select {
					case <-exec.UpstreamDisconnectChan(t.Name()):
						cancel()
						close(notified)
					case <-waitCtx.Done():
					}
				}()
				result, errStream := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
				if errStream != nil {
					t.Fatal(errStream)
				}
				_, errStream = drainChunks(result)
				var status interface{ StatusCode() int }
				if !errors.As(errStream, &status) || status.StatusCode() != failure.status {
					t.Fatalf("final error = %v, want upstream status %d", errStream, failure.status)
				}
				select {
				case <-notified:
				case <-waitCtx.Done():
					t.Fatal("disconnect was not notified after error delivery")
				}
			})
		}
	}
}
