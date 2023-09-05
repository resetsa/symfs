// hide completion commands

package cmd

import (
	"github.com/spf13/cobra"
)

var (
	// Used for flags.

	completionCmd = &cobra.Command{
		Use:    "completion",
		Short:  "Generate the autocompletion script for the specified shell",
		Hidden: true,
	}
)

func init() {
	rootCmd.AddCommand(completionCmd)
}
