package cmd

import (
	"errors"
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
	// print args
	kvArgs := map[string]any{"force": Force, "hosts": Conf.Nodes}
	printArgs(&Logger, kvArgs)
	// validation args
	if !Force {
		return errors.New("use force flag for action")
	}
	// init session
	Logger.LeveledFunc(utils.LogVerbose, Logger.Print, "init session to Cassandra")
	sess, err := utils.InitSession(&Conf)
	if err != nil {
		return err
	}
	// clean session on exit
	defer func() {
		Logger.LeveledFunc(utils.LogVerbose, Logger.Print, "close session to Cassandra")
		if !sess.Closed() {
			sess.Close()
		}
		Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "stop clean phase")
	}()
	// render sql query
	querys := utils.QueryHolder{}
	querys.RenderSql(Conf.Keyspace, Conf.Column, 60, Conf.TTL)
	// truncate table
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "truncate table %s.%s", Conf.Keyspace, Conf.Column)
	err_t := utils.ExecQuery(sess, querys.TruncateSql)
	// drop table
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "drop table %s.%s", Conf.Keyspace, Conf.Column)
	err_d := utils.ExecQuery(sess, querys.DropSql)
	// error processing
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
