package feed

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"transport", &FetchError{Op: "transport", Err: errors.New("conn refused")}, true},
		{"transport-timeout", &FetchError{Op: "transport", Err: context.DeadlineExceeded}, true},
		{"transport-canceled", &FetchError{Op: "transport", Err: context.Canceled}, false},
		{"status-500", &FetchError{Op: "status", Status: 500, Err: errors.New("unexpected status 500")}, true},
		{"status-503", &FetchError{Op: "status", Status: 503, Err: errors.New("unexpected status 503")}, true},
		{"status-429", &FetchError{Op: "status", Status: 429, Err: errors.New("unexpected status 429")}, true},
		{"status-404", &FetchError{Op: "status", Status: 404, Err: errors.New("unexpected status 404")}, false},
		{"status-401", &FetchError{Op: "status", Status: 401, Err: errors.New("unexpected status 401")}, false},
		{"parse", &FetchError{Op: "parse", Err: errors.New("bad xml")}, false},
		{"request", &FetchError{Op: "request", Err: errors.New("bad url")}, false},
		{"untyped", errors.New("???"), false},
		{"wrapped-transport", fmt.Errorf("poll: %w", &FetchError{Op: "transport", Err: errors.New("x")}), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRetryable(c.err); got != c.want {
				t.Fatalf("IsRetryable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
