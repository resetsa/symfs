package cmd

import (
	"errors"
	"resetsa/symfs/utils"

	"sync/atomic"

	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// syncCmd represents the sync command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync data",
	Long:  "Sync data from file and column family",
	RunE:  runnerSync,
}

const (
	maxParallel = 50
	minParalell = 0
)

type SyncResult struct {
	isExisted, isUpdated bool
	err                  error
}

func init() {
	rootCmd.AddCommand(syncCmd)
	syncCmd.Flags().StringVar(&InFile, "filepath", "./generated.csv", "input file with vids")
	syncCmd.Flags().BoolVar(&Select, "select", false, "run select first")
	syncCmd.Flags().StringVar(&Delim, "delim", ",", "set delimeters symbol")
	syncCmd.Flags().IntVar(&BatchStringSize, "bsize", 100000, "set batch read size")
	syncCmd.Flags().IntVar(&Parallel, "parallel", 4, "set parallem threads")
}

func runnerSync(cmd *cobra.Command, args []string) error {
	// TODO need be simpler and be refactored
	// counters for operations
	var exist, updated atomic.Uint64
	// disable help and errors output
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	printArgs()
	// check parallel level
	if Parallel <= minParalell || Parallel > maxParallel {
		return errors.New("set parallel between [1, 50)")
	}
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
	g := new(errgroup.Group)
	g.SetLimit(Parallel)
	for {
		// slice for save goroutine func
		var startFuncs []func() error
		lines := scanner.ReadBatch()
		// break if lines is out
		if len(lines) == 0 {
			break
		}
		Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "read %v records from file", len(lines))
		// generate func for run as goroutine
		for _, line := range lines {
			startFuncs = append(startFuncs, genFunc(sess, querys, line, &exist, &updated))
		}
		// run goroutine with limits
		for _, f := range startFuncs {
			started := g.TryGo(f)
			if !started {
				if err := g.Wait(); err != nil {
					return err
				}
				g.Go(f)
			}
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}
	Logger.LeveledFuncF(utils.LogInfo, Logger.Printf, "total entry exist/updated: %v/%v", exist.Load(), updated.Load())
	return nil
}

// func for generate func have signature call func() error
func genFunc(sess *gocql.Session, querys utils.QueryHolder, entryString string, exist, updated *atomic.Uint64) func() error {
	return func() error {
		// main sync logic
		sResult := syncEntry(sess, querys, entryString)
		if sResult.err != nil {
			return sResult.err
		}
		// inc counters
		if sResult.isExisted {
			exist.Add(1)
		}
		if sResult.isUpdated {
			updated.Add(1)
		}
		return nil
	}
}

func syncEntry(sess *gocql.Session, querys utils.QueryHolder, entryString string) SyncResult {
	entry, err := utils.NewFsEntryFromString(entryString, Delim)
	if err != nil {
		return SyncResult{err: err}
	}
	if Select {
		return updateWithSelect(sess, querys.InsertSql, querys.SelectSql, entry)
	}
	return updateWithoutSelect(sess, querys.InsertSql, entry)
}

func updateWithSelect(sess *gocql.Session, insertQuery, selectQuery string, entry utils.FsEntry) SyncResult {
	if !utils.EntryExist(sess, selectQuery, entry) {
		err := utils.UpdateEntry(sess, insertQuery, entry)
		if err != nil {
			return SyncResult{err: err}
		}
		return SyncResult{isUpdated: true}
	}
	return SyncResult{
		isUpdated: false,
		isExisted: true,
	}
}

func updateWithoutSelect(sess *gocql.Session, insertQuery string, entry utils.FsEntry) SyncResult {
	if err := utils.UpdateEntry(sess, insertQuery, entry); err != nil {
		return SyncResult{err: err}
	}
	return SyncResult{isUpdated: true}
}

func printArgs() {
	Logger.LeveledFunc(utils.LogVerbose, Logger.Println, "start sync phase")
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "filename - %s", InFile)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "select - %t", Select)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "delim - \"%s\"", Delim)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "batch size - %v", BatchStringSize)
	Logger.LeveledFuncF(utils.LogVerbose, Logger.Printf, "parallel threads - %v", Parallel)
}
