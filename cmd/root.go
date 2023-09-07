/*
Copyright © 2023 Sergey Stepanenko powersa@mail.ru
*/
package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"resetsa/symfs/conf"
	"resetsa/symfs/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile, InFile string
var Verbose, Force, Select bool
var Conf conf.Config
var GenerateFilename, PrefixVid, PrefixUrl, Delim string
var GenerateCount uint32
var BatchStringSize int
var Logger utils.AppLogger

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

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".symfs" (without extension).
		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.SetConfigName("symfs.yaml")
	}

	viper.AutomaticEnv() // read in environment variables that match
	if err := ParseConfig(&Conf); err != nil {
		rootCmd.SilenceErrors = true
		rootCmd.SilenceUsage = true
		Logger.LeveledFuncF(utils.LogError, Logger.Printf, "fail on parse config %s", viper.ConfigFileUsed())
		Logger.LeveledFunc(utils.LogError, Logger.Fatal, err)
	}
}

func initLogger() {
	if Verbose {
		logMap[utils.LogVerbose] = "VERBOSE: "
	}
	Logger = utils.NewAppLogger(logMap)
}

func ParseConfig(c *conf.Config) error {
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	if err := viper.Unmarshal(&c); err != nil {
		return err
	}
	return nil
}
