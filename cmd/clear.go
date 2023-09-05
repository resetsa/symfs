package cmd

import (
	"errors"
	"fmt"
	"resetsa/symfs/utils"

	"github.com/spf13/cobra"
)

// checkCmd represents the check command
var clearCmd = &cobra.Command{
	Use:   "clear",
	Short: "Clear data with columns",
	Long:  "Clear all generated data and drop column.",
	RunE:  runCleaner,
}

func init() {
	rootCmd.AddCommand(clearCmd)
}

func runCleaner(cmd *cobra.Command, args []string) error {
	// disable help and errors output
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "start clean phase")
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "force - %t", Force)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "hosts - %v", Conf.Nodes)
	if !Force {
		return errors.New("use force flag for action")
	}
	sess, err := utils.InitSession(&Conf)
	Logger.LeveledFunc(utils.LogVerbose, Logger.Print, "init session to Cassandra")
	if err != nil {
		return err
	}
	defer func() {
		Logger.LeveledFunc(utils.LogVerbose, Logger.Print, "close session to Cassandra")
		if !sess.Closed() {
			sess.Close()
		}
		Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "stop clean phase")
	}()
	truncateTable := fmt.Sprintf(utils.TruncateTableTmpl, Conf.Keyspace, Conf.Column)
	dropTable := fmt.Sprintf(utils.DropTableTmpl, Conf.Keyspace, Conf.Column)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "truncate table %s.%s", Conf.Keyspace, Conf.Column)
	err_t := utils.ExecQuery(sess, truncateTable)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "drop table %s.%s", Conf.Keyspace, Conf.Column)
	err_d := utils.ExecQuery(sess, dropTable)
	switch {
	case err_t != nil:
		return err_t
	case err_d != nil:
		return err_d
	default:
		Logger.LeveledFuncF(utils.LogInfo, Logger.Printf, "clean data in table %s.%s", Conf.Keyspace, Conf.Column)
		return nil
	}
}
