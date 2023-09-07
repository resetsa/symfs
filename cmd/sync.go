/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
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
	syncCmd.Flags().IntVar(&BatchStringSize, "bsize", 100000, "set delimeters symbol")
}

func runnerSync(cmd *cobra.Command, args []string) error {
	var exist, updated int64
	// disable help and errors output
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	printArgs()
	// check file exist and accessed
	if fileExist, err := utils.PathExists(InFile); !fileExist || err != nil {
		return err
	}
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
	querys := utils.QueryHolder{}
	querys.RenderSql(Conf.Keyspace, Conf.Column, 60, Conf.TTL)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "create columns %s.%s if not exist", Conf.Keyspace, Conf.Column)
	if err := utils.ExecQuery(sess, querys.CreateSql); err != nil {
		return err
	}
	scanner, err := utils.NewStringReader(InFile, BatchStringSize)
	if err != nil {
		return err
	}
	// for gc run
	defer func() {
		scanner = nil
	}()
	for {
		lines := scanner.ReadBatch()
		Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "read %v records from file", len(lines))
		if len(lines) == 0 {
			break
		}
		for _, line := range lines {
			isExisted, IsUpdated, err := syncEntry(sess, querys.InsertSql, querys.SelectSql, line)
			if err != nil {
				return err
			}
			if isExisted {
				exist++
			}
			if IsUpdated {
				updated++
			}
		}
		Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "entry exist/updated: %v/%v", exist, updated)
	}

	Logger.LeveledFuncF(utils.LogInfo, Logger.Printf, "total entry exist/updated: %v/%v", exist, updated)
	return nil
}

func syncEntry(sess *gocql.Session, insertQuery, selectQuery string, entryString string) (isExisted, isUpdated bool, err error) {
	entry, err := utils.NewFsEntryFromString(entryString, Delim)
	if err != nil {
		return isExisted, isUpdated, err
	}
	if Select {
		return updateWithSelect(sess, insertQuery, selectQuery, entry)
	}
	return updateWithoutSelect(sess, insertQuery, entry)
}

func updateWithSelect(sess *gocql.Session, insertQuery, selectQuery string, entry utils.FsEntry) (isExisted, isUpdated bool, err error) {
	if !utils.EntryExist(sess, selectQuery, entry) {
		err := utils.UpdateEntry(sess, insertQuery, entry)
		if err != nil {
			return isExisted, isUpdated, err
		}
		isUpdated = true
		return isExisted, isUpdated, nil
	}
	isExisted = true
	return isExisted, isUpdated, nil
}

func updateWithoutSelect(sess *gocql.Session, insertQuery string, entry utils.FsEntry) (isExisted, isUpdated bool, err error) {
	if err = utils.UpdateEntry(sess, insertQuery, entry); err != nil {
		return isExisted, isUpdated, err
	}
	isUpdated = true
	return isExisted, isUpdated, err
}

func printArgs() {
	Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "start sync phase")
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "filename - %s", InFile)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "select - %t", Select)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "delim - \"%s\"", Delim)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "batch size - %v", BatchStringSize)
}
