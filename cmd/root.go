/*
Copyright © 2023 Sergey Stepanenko powersa@mail.ru
*/
package cmd

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"resetsa/symfs/conf"
	"resetsa/symfs/utils"

	"github.com/spf13/cobra"
)

var cfgFile, InFile string
var Verbose, Force, Select bool
var Conf conf.Config
var GenerateFilename, PrefixVid, PrefixUrl, Delim string
var GenerateCount uint32
var BatchStringSize, Parallel int
var Logger utils.AppLogger
var timeStart, timeEnd time.Time
var logMap = utils.MapLevelPrefix{
	utils.LogError:   "ERROR: ",
	utils.LogInfo:    "INFO: ",
	utils.LogWarning: "WARNING: ",
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "symfs",
	Short: "Simulate filestorage activity to Cassandra",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	timeStart = time.Now()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		Logger.PrintError("Interrupt from Control-C")
		os.Exit(255)
	}()
	err := rootCmd.Execute()
	if err != nil {
		// output error
		Logger.PrintError(err)
		os.Exit(1)
	}
	timeEnd = time.Now()
	Logger.LeveledFuncF(utils.LogInfo, Logger.Printf, "running time %v\n", timeEnd.Sub(timeStart))
}

func init() {

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/symfs.yaml)")
	rootCmd.PersistentFlags().BoolVar(&Verbose, "verbose", false, "verbose logging")
	rootCmd.PersistentFlags().BoolVar(&Force, "force", false, "force run")
	cobra.OnInitialize(initLogger, initConfig)
}
