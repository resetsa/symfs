/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"resetsa/symfs/conf"
	"resetsa/symfs/utils"

	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
)

// syncCmd represents the sync command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync data",
	Long:  "Sync data from file and column family",
	RunE:  runnerSync,
}

func init() {
	rootCmd.AddCommand(syncCmd)
	syncCmd.Flags().StringVar(&InFile, "filepath", "./generated.csv", "input file with vids")
	syncCmd.Flags().BoolVar(&Select, "select", false, "run select first")
	syncCmd.Flags().StringVar(&Delim, "delim", ",", "set delimeters symbol")
}

func runnerSync(cmd *cobra.Command, args []string) error {
	var fsEntryS []utils.FsEntry
	var checked, updated int64
	// disable help and errors output
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "start sync phase")
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "filename - %s", InFile)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "select - %t", Select)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "delim - \"%s\"", Delim)
	fileExist, err := utils.PathExists(InFile)
	if !fileExist || err != nil {
		return err
	}
	if fsEntryS, err = utils.ReadFromFile(InFile, Delim); err != nil {
		return err
	}
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "read from file %d records", len(fsEntryS))
	Logger.LeveledFunc(utils.LogVerbose, Logger.Print, "init session to Cassandra")
	sess, err := utils.InitSession(&Conf)
	if err != nil {
		return err
	}
	defer func() {
		Logger.LeveledFunc(utils.LogVerbose, Logger.Print, "close session to Cassandra")
		if !sess.Closed() {
			sess.Close()
		}
		Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "stop sync phase")
	}()
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "create columns %s.%s if not exist", Conf.Keyspace, Conf.Column)
	createTable := fmt.Sprintf(utils.CreateTableTmpl, Conf.Keyspace, Conf.Column)
	if err := utils.ExecQuery(sess, createTable); err != nil {
		return err
	}
	for _, entry := range fsEntryS {
		switch Select {
		case true:
			checked += 1
			isUpdated, err := updateWithSelect(sess, Conf, entry)
			if err != nil {
				return err
			}
			if isUpdated {
				updated += 1
			}
		case false:
			if err = utils.UpdateEntry(sess, Conf.Keyspace, Conf.Column, Conf.TTL, entry); err != nil {
				return err
			}
			updated += 1
		}
	}
	Logger.LeveledFuncF(utils.LogInfo, Logger.Printf, "entry check/updated: %d/%d", checked, updated)
	return nil
}

func updateWithSelect(sess *gocql.Session, conf conf.Config, entry utils.FsEntry) (isUpdated bool, err error) {
	if !utils.EntryExist(sess, conf.Keyspace, conf.Column, entry) {
		err := utils.UpdateEntry(sess, conf.Keyspace, conf.Column, conf.TTL, entry)
		if err != nil {
			return false, err
		}
		isUpdated = true
	}
	return isUpdated, nil
}
