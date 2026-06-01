package config

import _ "embed"

// exampleYAML is the canonical, fully-annotated reference configuration. It is
// kept byte-identical to examples/config.example.yaml (enforced by a drift-guard
// test) so the `generate-config` command and the repo example never diverge.
//
//go:embed example.yaml
var exampleYAML []byte

// ExampleConfig returns the embedded annotated reference configuration. The
// bytes are a complete, runnable config that Loads and Validates cleanly; the
// `generate-config` subcommand writes them to stdout or a file.
func ExampleConfig() []byte {
	out := make([]byte, len(exampleYAML))
	copy(out, exampleYAML)
	return out
}
