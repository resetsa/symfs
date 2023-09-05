/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"path/filepath"

	"resetsa/symfs/utils"

	"github.com/spf13/cobra"
)

// generateCmd represents the generate command
var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate data for load",
	Long:  "Generate data for load in column family",
	RunE: func(cmd *cobra.Command, args []string) error {
		// disable help and errors output
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "start generate phase")
		Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "filename - %s", GenerateFilename)
		Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "counter - %d", GenerateCount)
		defer Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "stop generate phase")
		// checks args
		Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "check file exist %v", GenerateFilename)
		fileExist, err := utils.PathExists(GenerateFilename)
		if err != nil {
			return err
		}
		if fileExist && (!Force) {
			return fmt.Errorf("file %s already exists, use force for overwrite or change filename", GenerateFilename)
		}
		Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "check dir exist %v", filepath.Dir(GenerateFilename))
		if dirExist, err := utils.PathExists(filepath.Dir(GenerateFilename)); (err != nil) || (!dirExist) {
			return fmt.Errorf("dir not exists or other error access to %s", filepath.Dir(GenerateFilename))
		}
		// run GenerateData
		Logger.LeveledFunc(utils.LogVerbose, Logger.Print, "generate data")
		if err := utils.GenerateData(GenerateFilename, GenerateCount, PrefixVid, PrefixUrl); err != nil {
			return err
		}
		Logger.LeveledFuncF(utils.LogInfo, Logger.Printf, "data was generated to file %s", GenerateFilename)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)
	generateCmd.PersistentFlags().Uint32Var(&GenerateCount, "counter", 100, "counter for record number")
	generateCmd.PersistentFlags().StringVar(&GenerateFilename, "filename", "./generated.csv", "filename for generate records")
	generateCmd.PersistentFlags().StringVar(&PrefixVid, "prefixvid", "vg-000001", "prefix for vid")
	generateCmd.PersistentFlags().StringVar(&PrefixUrl, "prefixurl", "https://localhost/", "prefix for url")
}
