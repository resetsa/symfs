package cmd

import (
	"github.com/spf13/cobra"
)

// checkCmd represents the check command
var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "Testing connection",
	Long:  "Testing connection module.",
}

func init() {
	rootCmd.AddCommand(checkCmd)
}
