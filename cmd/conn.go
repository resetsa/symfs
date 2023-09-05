/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"resetsa/symfs/utils"

	"github.com/spf13/cobra"
)

// connCmd represents the conn command
var connCmd = &cobra.Command{
	Use:   "conn",
	Short: "check connect",
	Long:  "check connect to Cassandra cluster",
	RunE:  runChecker,
}

func init() {
	checkCmd.AddCommand(connCmd)
}

func runChecker(cmd *cobra.Command, args []string) error {
	// disable help and errors output
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "start checking connection")
	defer Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "end check connection")
	if _, err := utils.InitSession(&Conf); err != nil {
		return err
	}
	Logger.LeveledFunc(utils.LogInfo, Logger.Println, "check connection OK")
	return nil
}
