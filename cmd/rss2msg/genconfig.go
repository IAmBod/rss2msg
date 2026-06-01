package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/config"
)

func newGenerateConfigCmd() *cobra.Command {
	var (
		output string
		force  bool
	)
	cmd := &cobra.Command{
		Use:     "generate-config",
		Aliases: []string{"gen-config"},
		Short:   "Print an annotated, runnable reference config",
		Long: "Print a complete, fully-annotated reference configuration that is ready to run.\n" +
			"By default it is written to stdout so it can be redirected:\n\n" +
			"\trss2msg generate-config > config.yaml\n\n" +
			"Use --output to write to a file instead.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			data := config.ExampleConfig()
			if output == "" {
				_, err := cmd.OutOrStdout().Write(data)
				return err
			}
			flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
			if !force {
				flags |= os.O_EXCL
			}
			f, err := os.OpenFile(output, flags, 0o644)
			if err != nil {
				if errors.Is(err, fs.ErrExist) {
					return fmt.Errorf("%s already exists; pass --force to overwrite", output)
				}
				return err
			}
			if _, err := f.Write(data); err != nil {
				_ = f.Close()
				return err
			}
			return f.Close()
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "Write to this file instead of stdout")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Overwrite the output file if it already exists")
	return cmd
}
