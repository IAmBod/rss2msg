package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Build metadata. These are overridden at build time via -ldflags, matching the
// defaults GoReleaser sets (see .goreleaser.yaml):
//
//	-X main.version={{.Version}} -X main.commit={{.Commit}} -X main.date={{.Date}}
//
// For `go build`/`go install` without ldflags they keep these placeholder values.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, and build date",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(),
				"rss2msg %s\n  commit:  %s\n  built:   %s\n  go:      %s\n  os/arch: %s/%s\n",
				version, commit, date, runtime.Version(), runtime.GOOS, runtime.GOARCH)
			return err
		},
	}
}
