package cmd

import (
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Config operations",
	Long:  "Config operations command",
}

func init() {
	rootCmd.AddCommand(configCmd)
}
