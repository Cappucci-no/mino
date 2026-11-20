package middleware

import (
	"net/http"
	"time"

	"github.com/cappucci-no/mino/rest/chain"
)

// Timeout limits the time spent waiting for an inner handler. It wraps the
// handler in http.TimeoutHandler, which buffers the response and replies with
// status 503 and the text "Request Timeout" when the deadline expires; later
// writes then fail with http.ErrHandlerTimeout. It does not invoke the httpx
// error handler or stop the handler; blocking work must observe the request
// context.
//
// A zero duration returns the next handler unchanged; a negative duration
// panics at setup time. Existing context deadlines are never extended. Parent
// cancellation is not reclassified as a client error: it writes a bare 503,
// and later writes fail with the context error, not http.ErrHandlerTimeout.
// Zero disables only this layer, not an outer timeout or parent deadline.
//
// Apply Timeout to ordinary routes only: its writer supports neither Flush
// nor Hijack, so SSE, streaming, and WebSocket routes must stay outside it.
// Request headers never bypass the limit. Place Logger outside Timeout to log
// the final response, and Recovery inside it to recover panics in the worker
// goroutine, including one raised after the timeout response has been sent:
//
//	mux.Use(middleware.Logger())
//	api := mux.With(middleware.Timeout(3*time.Second), middleware.Recovery())
//	api.Get("/users/{id}", getUser)
//	mux.With(middleware.Recovery()).Get("/events", events)
func Timeout(d time.Duration) chain.Middleware {
	if d < 0 {
		panic("middleware: timeout must be non-negative")
	}
	return func(next http.Handler) http.Handler {
		if d == 0 {
			return next
		}
		return http.TimeoutHandler(next, d, "Request Timeout")
	}
}
