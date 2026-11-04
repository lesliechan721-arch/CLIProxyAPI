package openai

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// An older proxy can report a lost interrupt target as a generic 400. Both
// stream errors and duplex error events must allow the client to reconnect.
func TestForwardResponsesWebsocketInterruptConnectionError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, source := range []string{"stream", "payload", "duplex_payload"} {
		t.Run(source, func(t *testing.T) {
			serverDone := make(chan error, 1)
			handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, errUpgrade := responsesWebsocketUpgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					serverDone <- errUpgrade
					return
				}
				defer func() { _ = conn.Close() }()
				ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
				ctx.Request = r
				data := make(chan []byte)
				errs := make(chan *interfaces.ErrorMessage, 1)
				go func() {
					data <- []byte(`{"type":"response.created","response":{"id":"r1"}}`)
					payload := []byte(`{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"no active upstream websocket for response.interrupt"}}`)
					if source == "stream" {
						errs <- responsesWebsocketErrorMessageFromPayload(payload)
					} else {
						data <- payload
						close(data)
					}
				}()
				_, _, _, _, errForward := handler.forwardResponsesWebsocket(
					ctx, newResponsesWebsocketWriter(conn), func(...interface{}) {}, data, errs, nil, "interrupt-recovery",
					responsesWebsocketForwardOptions{duplexStream: func() bool { return source == "duplex_payload" }},
				)
				if !errors.Is(errForward, websocket.ErrCloseSent) {
					serverDone <- errors.New("expected the forwarding loop to close for reconnect")
					return
				}
				serverDone <- nil
			}))
			defer server.Close()
			client, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if errDial != nil {
				t.Fatal(errDial)
			}
			defer func() { _ = client.Close() }()
			if errDeadline := client.SetReadDeadline(time.Now().Add(5 * time.Second)); errDeadline != nil {
				t.Fatal(errDeadline)
			}
			_, created, errCreated := client.ReadMessage()
			if errCreated != nil || ojson.GetBytesNoCopy(created, "type").String() != "response.created" {
				t.Fatalf("expected response.created, got %s, %v", created, errCreated)
			}
			_, payload, errRead := client.ReadMessage()
			if !websocket.IsCloseError(errRead, websocket.CloseAbnormalClosure) {
				t.Fatalf("expected transport disconnect without a terminal error frame, got %s, %v", payload, errRead)
			}
			select {
			case errServer := <-serverDone:
				if errServer != nil {
					t.Fatal(errServer)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("forwarding did not stop")
			}
		})
	}
}
