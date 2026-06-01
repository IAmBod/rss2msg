package main

import (
	"time"

	sentry "github.com/getsentry/sentry-go"
)

// panicFlushTimeout bounds how long we wait for the crash report to reach Sentry
// before letting the process die.
const panicFlushTimeout = 2 * time.Second

// reportPanic sends an in-flight panic value to Sentry and flushes so the crash
// is recorded before the process exits. It is a no-op (beyond a bounded flush)
// when Sentry is disabled. It does NOT swallow the panic — callers re-panic
// after invoking it so existing crash behaviour is unchanged.
func reportPanic(r any) {
	if r == nil {
		return
	}
	sentry.CurrentHub().Recover(r)
	sentry.Flush(panicFlushTimeout)
}
