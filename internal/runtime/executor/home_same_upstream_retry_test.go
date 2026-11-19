package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/json"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executionregistry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type sameUpstreamHomeDispatcher struct {
	auth  *cliproxyauth.Auth
	calls atomic.Int32
}

func (*sameUpstreamHomeDispatcher) HeartbeatOK() bool       { return true }
func (*sameUpstreamHomeDispatcher) AbortAmbiguousDispatch() {}
func (d *sameUpstreamHomeDispatcher) RPopAuth(context.Context, string, string, http.Header, int) ([]byte, error) {
	d.calls.Add(1)
	return json.Marshal(map[string]any{"auth": d.auth})
}

func TestHomeCodexWebsocketSameUpstreamRetry(t *testing.T) {
	for _, failure := range []string{"overload", "disconnect"} {
		t.Run(failure, func(t *testing.T) {
			var attempts, connections atomic.Int32
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, errUpgrade := upgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					t.Errorf("upgrade: %v", errUpgrade)
					return
				}
				defer func() { _ = conn.Close() }()
				connections.Add(1)
				for {
					if _, _, errRead := conn.ReadMessage(); errRead != nil {
						return
					}
					attempt := attempts.Add(1)
					if attempt < 3 && failure == "disconnect" {
						return
					}
					frames := []string{codexCreatedEvent, codexInProgressEvent, codexCompletedEventBody}
					if attempt < 3 {
						frames[2] = codexOverloadEvent
					}
					for _, frame := range frames {
						if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(frame)); errWrite != nil {
							t.Errorf("frame write: %v", errWrite)
							return
						}
					}
				}
			}))
			defer server.Close()
			cfg := codexBufferingConfig(true)
			cfg.SameUpstreamRetry = 2
			cfg.Home = config.HomeConfig{Enabled: true}
			executor := NewCodexWebsocketsExecutor(cfg)
			executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
			auth := codexTestAuth(server.URL)
			auth.ID, auth.Provider = "same-upstream-home-codex", "codex"
			auth.Attributes["websockets"] = "true"
			dispatcher := &sameUpstreamHomeDispatcher{auth: auth}
			scopes := executionregistry.New()
			manager := cliproxyauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.SetRetryConfig(0, 0, 0)
			manager.PublishHomeDispatch(dispatcher, scopes, 1)
			manager.RegisterExecutor(executor)
			defer manager.CloseExecutionSession(t.Name())
			req, opts := codexWebsocketRequest()
			opts.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: t.Name()}
			ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
			result, errStream := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
			if failure == "disconnect" {
				if errStream == nil {
					_, errStream = drainChunks(result)
				}
				if !cliproxyexecutor.IsUpstreamWebsocketTurnError(errStream) {
					t.Fatalf("disconnect error = %v, want a lost turn without replay", errStream)
				}
				if attempts.Load() != 1 || dispatcher.calls.Load() != 1 {
					t.Fatalf("lost turn replayed: attempts=%d dispatches=%d", attempts.Load(), dispatcher.calls.Load())
				}
				manager.CloseExecutionSession(t.Name())
				if errDrain := scopes.Drain(context.Background()); errDrain != nil {
					t.Fatal(errDrain)
				}
				return
			}
			if errStream != nil {
				t.Fatal(errStream)
			}
			if _, errDrain := drainChunks(result); errDrain != nil {
				t.Fatal(errDrain)
			}
			if attempts.Load() != 3 || dispatcher.calls.Load() != 1 {
				t.Fatalf("attempts=%d dispatches=%d, want 3 attempts in one Home scope", attempts.Load(), dispatcher.calls.Load())
			}
			result, errStream = manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
			if errStream != nil {
				t.Fatal(errStream)
			}
			if _, errDrain := drainChunks(result); errDrain != nil {
				t.Fatal(errDrain)
			}
			if attempts.Load() != 4 || connections.Load() != 3 || dispatcher.calls.Load() != 1 {
				t.Fatalf("next request: attempts=%d connections=%d dispatches=%d, want to reuse the recovered connection and scope", attempts.Load(), connections.Load(), dispatcher.calls.Load())
			}
			manager.CloseExecutionSession(t.Name())
			if errDrain := scopes.Drain(context.Background()); errDrain != nil {
				t.Fatal(errDrain)
			}
		})
	}
}

func TestHomeCodexSameUpstreamRetryReleasesTerminalScope(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, err = conn.ReadMessage(); err != nil {
			return
		}
		frames := []string{codexCreatedEvent, codexInProgressEvent, `{"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"type":"server_error","status":500,"message":"upstream processing failed"}}}`}
		for _, frame := range frames {
			if err = conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	cfg := codexBufferingConfig(true)
	cfg.SameUpstreamRetry = 2
	cfg.Home = config.HomeConfig{Enabled: true}
	exec := NewCodexWebsocketsExecutor(cfg)
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	auth := codexTestAuth(server.URL)
	auth.ID, auth.Provider = "same-upstream-home-terminal", "codex"
	auth.Attributes["websockets"] = "true"
	dispatcher := &sameUpstreamHomeDispatcher{auth: auth}
	scopes := executionregistry.New()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.SetRetryConfig(0, 0, 0)
	manager.PublishHomeDispatch(dispatcher, scopes, 1)
	manager.RegisterExecutor(exec)
	defer manager.CloseExecutionSession(t.Name())
	req, opts := codexWebsocketRequest()
	opts.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: t.Name()}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	result, err := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
	if result != nil {
		_, errDrain := drainChunks(result)
		if err == nil {
			err = errDrain
		}
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusInternalServerError {
		t.Fatalf("terminal buffered error = %v, want original upstream status 500", err)
	}
	sess := exec.getOrCreateSession(t.Name())
	sess.connMu.Lock()
	conn := sess.conn
	sess.connMu.Unlock()
	active := scopes.FreezeInFlight(time.Time{}).Executions
	select {
	case <-exec.UpstreamDisconnectChan(t.Name()):
	default:
		t.Fatal("terminal failure did not notify the downstream session")
	}
	if conn != nil || len(active) != 0 {
		t.Fatalf("connection present=%t active Home scopes=%d, want the failed connection and scope released", conn != nil, len(active))
	}
}

func TestCodexBufferedFailureNotifiesAfterErrorHandoff(t *testing.T) {
	for _, output := range []struct{ name, frame string }{
		{"bootstrap", codexInProgressEvent},
		{"live", `{"type":"response.output_text.delta","delta":"hello"}`},
	} {
		t.Run(output.name, func(t *testing.T) {
			server := codexWebsocketServerHoldingConnection(t, codexCreatedEvent, output.frame,
				`{"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"type":"server_error","status":500,"message":"upstream processing failed"}}}`)
			defer server.Close()
			exec := NewCodexWebsocketsExecutor(codexBufferingConfig(true))
			exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
			defer exec.CloseExecutionSession(t.Name())
			req, opts := codexWebsocketRequest()
			opts.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: t.Name()}
			ctx, cancel := context.WithTimeout(cliproxyexecutor.WithDownstreamWebsocket(t.Context()), 5*time.Second)
			defer cancel()
			result, errStream := exec.ExecuteStream(ctx, codexTestAuth(server.URL), req, opts)
			if errStream != nil {
				t.Fatal(errStream)
			}
			disconnect := exec.UpstreamDisconnectChan(t.Name())
			for {
				select {
				case <-disconnect:
					t.Fatal("disconnect was notified before the buffered error handoff")
				default:
				}
				select {
				case chunk, ok := <-result.Chunks:
					if !ok {
						t.Fatal("buffered stream closed without its upstream error")
					}
					if chunk.Err == nil {
						continue
					}
					var status interface{ StatusCode() int }
					if !errors.As(chunk.Err, &status) || status.StatusCode() != http.StatusInternalServerError {
						t.Fatalf("buffered stream error = %v, want upstream status 500", chunk.Err)
					}
				case <-ctx.Done():
					t.Fatal("buffered stream did not deliver its error")
				}
				break
			}
			select {
			case <-disconnect:
			case <-ctx.Done():
				t.Fatal("buffered stream did not notify disconnect after error handoff")
			}
		})
	}
}

func TestHomeCodexStreamErrorSameUpstreamRetry(t *testing.T) {
	for _, buffering := range []bool{false, true} {
		for _, failure := range []struct {
			name, frame string
			status      int
		}{
			{"error", `{"type":"error","status":503,"error":{"type":"server_error","message":"upstream overloaded"}}`, http.StatusServiceUnavailable},
			{"response_failed", `{"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"type":"server_error","status":500,"message":"upstream processing failed"}}}`, http.StatusInternalServerError},
		} {
			for _, mode := range []string{"recovered", "exhausted", "after_payload"} {
				t.Run(fmt.Sprintf("buffering=%t/%s/%s", buffering, failure.name, mode), func(t *testing.T) {
					var attempts atomic.Int32
					upstreamClosed := make(chan struct{}, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, err = conn.ReadMessage(); err != nil {
							return
						}
						attempt := attempts.Add(1)
						frames := []string{failure.frame}
						if mode == "recovered" && attempt > 1 {
							frames = []string{codexCreatedEvent, codexCompletedEventBody}
						} else if mode == "after_payload" {
							frames = append([]string{codexCreatedEvent, `{"type":"response.output_text.delta","delta":"hello"}`}, frames...)
						}
						for _, frame := range frames {
							if err = conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
								return
							}
						}
						_, _, _ = conn.ReadMessage()
						select {
						case upstreamClosed <- struct{}{}:
						default:
						}
					}))
					defer server.Close()
					cfg := codexBufferingConfig(buffering)
					cfg.SameUpstreamRetry = 1
					cfg.Home = config.HomeConfig{Enabled: true}
					exec := NewCodexWebsocketsExecutor(cfg)
					exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
					auth := codexTestAuth(server.URL)
					auth.ID, auth.Provider = "same-upstream-unbuffered", "codex"
					auth.Attributes["websockets"] = "true"
					dispatcher := &sameUpstreamHomeDispatcher{auth: auth}
					scopes := executionregistry.New()
					manager := cliproxyauth.NewManager(nil, nil, nil)
					manager.SetConfig(cfg)
					manager.SetRetryConfig(0, 0, 0)
					manager.PublishHomeDispatch(dispatcher, scopes, 1)
					manager.RegisterExecutor(exec)
					defer manager.CloseExecutionSession(t.Name())
					req, opts := codexWebsocketRequest()
					opts.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: t.Name()}
					ctx, cancel := context.WithTimeout(cliproxyexecutor.WithDownstreamWebsocket(context.Background()), 5*time.Second)
					defer cancel()
					disconnect := exec.UpstreamDisconnectChan(t.Name())
					notified := make(chan struct{})
					if mode == "after_payload" {
						go func() {
							select {
							case <-disconnect:
								close(notified)
								cancel()
							case <-ctx.Done():
							}
						}()
					}
					result, errStream := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
					if mode == "after_payload" {
						// Let terminal invalidation close the upstream while the manager's
						// caller has not yet consumed any of the stream output.
						select {
						case <-upstreamClosed:
						case <-ctx.Done():
							// An early notification cancels the real downstream listener.
							// The exact error assertion below must still reject a lost error.
						}
					}
					if result != nil {
						_, errDrain := drainChunks(result)
						if errStream == nil {
							errStream = errDrain
						}
					}
					if mode == "recovered" {
						if errStream != nil {
							t.Fatal(errStream)
						}
						select {
						case errDisconnect := <-exec.UpstreamDisconnectChan(t.Name()):
							t.Fatalf("recovered retry notified downstream disconnect: %v", errDisconnect)
						default:
						}
					} else {
						var status interface{ StatusCode() int }
						if !errors.As(errStream, &status) || status.StatusCode() != failure.status {
							t.Fatalf("terminal stream error = %v, want original upstream status %d", errStream, failure.status)
						}
						if mode == "after_payload" {
							select {
							case <-notified:
							case <-t.Context().Done():
								t.Fatal("terminal stream error did not notify downstream disconnect")
							}
						} else {
							select {
							case <-exec.UpstreamDisconnectChan(t.Name()):
								t.Fatal("failure before output closed the downstream before failover")
							default:
							}
						}
						if active := scopes.FreezeInFlight(time.Time{}).Executions; len(active) != 0 {
							t.Fatalf("terminal stream retained %d Home scopes", len(active))
						}
					}
					if mode == "exhausted" {
						if attempts.Load() < 2 {
							t.Fatalf("attempts=%d, want retry budget exhausted", attempts.Load())
						}
					} else {
						wantAttempts := int32(2)
						if mode == "after_payload" {
							wantAttempts = 1
						}
						if attempts.Load() != wantAttempts || dispatcher.calls.Load() != 1 {
							t.Fatalf("attempts=%d dispatches=%d, want %d attempts in one Home scope", attempts.Load(), dispatcher.calls.Load(), wantAttempts)
						}
					}
				})
			}
		}
	}
}
