/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"resetsa/symfs/utils"

	"github.com/spf13/cobra"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "Show config",
	Long:  "Show config file for app",
	Run: func(cmd *cobra.Command, args []string) {
		Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "start show config")
		defer Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "end show config")
		fmt.Println(Conf)
	},
}

func init() {
	configCmd.AddCommand(showCmd)
}
