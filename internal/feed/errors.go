package feed

import (
	"context"
	"errors"
)

// FetchError categorizes a feed-fetch failure so callers can decide whether a
// retry is worthwhile without matching on error strings.
type FetchError struct {
	Op     string // "request" | "transport" | "parse" | "status"
	Status int    // HTTP status code when Op == "status"
	Err    error
}

func (e *FetchError) Error() string { return e.Err.Error() }
func (e *FetchError) Unwrap() error { return e.Err }

// IsRetryable reports whether err represents a transient fetch failure that is
// worth retrying: transport/network errors (except context cancellation) and
// HTTP 5xx / 429 responses. Parse errors, bad requests, and 4xx are permanent.
func IsRetryable(err error) bool {
	var fe *FetchError
	if !errors.As(err, &fe) {
		return false
	}
	switch fe.Op {
	case "transport":
		return !errors.Is(err, context.Canceled)
	case "status":
		return fe.Status >= 500 || fe.Status == 429
	default: // "parse", "request"
		return false
	}
}
