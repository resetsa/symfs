package cmd

import (
	"os"
	"resetsa/symfs/conf"
	"resetsa/symfs/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

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

// init logger
func initLogger() {
	if Verbose {
		logMap[utils.LogVerbose] = "VERBOSE: "
	}
	Logger = utils.NewAppLogger(logMap)
}

// parse config - this example from guide
func ParseConfig(c *conf.Config) error {
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	if err := viper.Unmarshal(&c); err != nil {
		return err
	}
	return nil
}

// shared func for print maps args
func printArgs(logger *utils.AppLogger, kv map[string]any) {
	for k, v := range kv {
		logger.LeveledFuncF(utils.LogVerbose, logger.Printf, "%s - %v", k, v)
	}
}
