package middleware

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cappucci-no/mino/rest"
	"github.com/cappucci-no/mino/rest/httpx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeoutResponse(t *testing.T) {
	ast := assert.New(t)
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var child context.Context
	h := Timeout(time.Second)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		child = r.Context()
		w.Header().Set("X-Result", "mino")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "created")
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", nil).WithContext(parent))
	ast.Equal(http.StatusCreated, w.Code)
	ast.Equal("created", w.Body.String())
	ast.Equal("mino", w.Header().Get("X-Result"))
	deadline, ok := child.Deadline()
	require.True(t, ok)
	parentDeadline, _ := parent.Deadline()
	ast.True(deadline.Before(parentDeadline))
	ast.ErrorIs(child.Err(), context.Canceled) // the child is cleaned up on return
	ast.NoError(parent.Err())
}

func TestTimeoutConfiguration(t *testing.T) {
	next := http.NewServeMux()
	assert.Same(t, next, Timeout(0)(next))
	assert.PanicsWithValue(t, "middleware: timeout must be non-negative", func() {
		Timeout(-time.Second)
	})
}

func TestTimeoutExpires(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
	}{
		{"ordinary request", nil},
		{"SSE accept does not bypass timeout", http.Header{"Accept": {"text/event-stream"}}},
		{"upgrade does not bypass timeout", http.Header{"Upgrade": {"websocket"}, "Connection": {"Upgrade"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := assert.New(t)
			capture := captureDefaultLogger(t)
			parent, cancel := context.WithCancel(context.Background())
			release := make(chan struct{})
			finished := make(chan struct{})
			lateWrite := make(chan error, 1)
			w := httptest.NewRecorder()
			t.Cleanup(func() {
				cancel()
				close(release)
				awaitTimeoutHandler(t, finished)
				ast.ErrorIs(<-lateWrite, http.ErrHandlerTimeout)
				ast.Equal(http.StatusServiceUnavailable, w.Code)
				ast.Equal("Request Timeout", w.Body.String())
				// A late httpx response must not add a second error log.
				records := capture.take()
				require.Len(t, records, 1)
				ast.Equal("request", records[0].message)
				ast.Equal(slog.LevelError, records[0].level)
				ast.EqualValues(http.StatusServiceUnavailable, records[0].attrs["status"])
				ast.EqualValues(len("Request Timeout"), records[0].attrs["bytes"])
				ast.Equal("GET /limited", records[0].attrs["pattern"])
			})
			mux := rest.NewMux()
			mux.Use(Logger(WithLogger(slog.New(capture))))
			mux.With(Timeout(10*time.Millisecond), Recovery()).Get("/limited", func(w http.ResponseWriter, r *http.Request) {
				defer close(finished)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Partial", "must not escape")
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, "partial")
				<-r.Context().Done()
				<-release
				_, err := io.WriteString(w, "late")
				lateWrite <- err
				httpx.OkCtx(r.Context(), w, map[string]string{"result": "late"})
			})
			r := httptest.NewRequest(http.MethodGet, "/limited", nil).WithContext(parent)
			for name, values := range tt.header {
				r.Header[name] = values
			}
			returned := make(chan struct{})
			go func() {
				mux.ServeHTTP(w, r)
				close(returned)
			}()
			awaitTimeoutHandler(t, returned)
			ast.Equal(http.StatusServiceUnavailable, w.Code)
			ast.Equal("Request Timeout", w.Body.String())
			ast.Empty(w.Header().Get("X-Partial"))
			ast.NotEqual("application/json", w.Header().Get("Content-Type"))
			select {
			case <-finished:
				t.Fatal("Timeout must return without waiting for the worker to stop")
			default:
			}
		})
	}
}

func TestTimeoutParentContext(t *testing.T) {
	tests := []struct {
		name string
		err  error
		body string
	}{
		{"canceled", context.Canceled, ""},
		{"deadline", context.DeadlineExceeded, "Request Timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := assert.New(t)
			parent, cancel := context.WithCancel(context.Background())
			if tt.err == context.DeadlineExceeded {
				cancel()
				parent, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			} else {
				cancel()
			}
			release := make(chan struct{})
			finished := make(chan struct{})
			seen := make(chan context.Context, 1)
			t.Cleanup(func() {
				cancel()
				close(release)
				awaitTimeoutHandler(t, finished)
				child := <-seen
				ast.ErrorIs(child.Err(), tt.err)
				if deadline, ok := parent.Deadline(); ok {
					got, _ := child.Deadline()
					ast.Equal(deadline, got)
				}
			})
			h := Timeout(time.Hour)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				defer close(finished)
				seen <- r.Context()
				<-release
			}))
			w := httptest.NewRecorder()
			returned := make(chan struct{})
			go func() {
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(parent))
				close(returned)
			}()
			awaitTimeoutHandler(t, returned)
			ast.Equal(http.StatusServiceUnavailable, w.Code)
			ast.Equal(tt.body, w.Body.String())
		})
	}
}

func TestTimeoutRecovery(t *testing.T) {
	tests := []struct {
		name   string
		late   bool
		status int
	}{
		{"panic before timeout", false, http.StatusInternalServerError},
		{"panic after timeout", true, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := assert.New(t)
			capture := captureDefaultLogger(t)
			parent, cancel := context.WithCancel(context.Background())
			release := make(chan struct{})
			finished := make(chan struct{})
			w := httptest.NewRecorder()
			t.Cleanup(func() {
				cancel()
				close(release)
				awaitTimeoutHandler(t, finished)
				records := capture.take()
				require.Len(t, records, 1)
				ast.Equal("middleware: recovered from panic", records[0].message)
				ast.Equal("timeout worker panic", records[0].attrs["panic"])
				ast.Equal(tt.status, w.Code)
			})
			recovered := Recovery()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				if tt.late {
					<-r.Context().Done()
					<-release
				}
				panic("timeout worker panic")
			}))
			duration := time.Minute
			if tt.late {
				duration = 10 * time.Millisecond
			}
			h := Timeout(duration)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(finished)
				recovered.ServeHTTP(w, r)
			}))
			returned := make(chan struct{})
			go func() {
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(parent))
				close(returned)
			}()
			awaitTimeoutHandler(t, returned)
			ast.Equal(tt.status, w.Code)
			if tt.late {
				ast.Equal("Request Timeout", w.Body.String())
			} else {
				ast.Empty(w.Body.String())
			}
		})
	}
}

func TestTimeoutRouteScope(t *testing.T) {
	captureDefaultLogger(t)
	mux := rest.NewMux()
	var flushErr error
	var flusher, hijacker bool
	mux.With(Timeout(time.Minute), Recovery()).Get("/api", func(w http.ResponseWriter, _ *http.Request) {
		_, flusher = w.(http.Flusher)
		_, hijacker = w.(http.Hijacker)
		flushErr = http.NewResponseController(w).Flush()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.With(Recovery()).Get("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		httpx.Stream(r.Context(), w, func(out io.Writer) bool {
			_, _ = io.WriteString(out, ": ready\n\n")
			return false
		})
	})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api", nil))
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.False(t, flusher)
	assert.False(t, hijacker)
	assert.ErrorIs(t, flushErr, http.ErrNotSupported)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/events", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, ": ready\n\n", w.Body.String())
	assert.True(t, w.Flushed)
}

// awaitTimeoutHandler waits for done, failing the test after five seconds. It
// bounds both the response delivery and the worker cleanup.
func awaitTimeoutHandler(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout handler did not finish")
	}
}
