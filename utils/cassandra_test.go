package utils

import (
	"fmt"
	"resetsa/symfs/conf"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

var (
	checkExistEntrys = []string{
		"exist/56499193-4570-11ee-871e-0019d14ccba6,false,false,https://localhost//2023/08/28/09/58/4856499193-4570-11ee-871e-0019d14ccba6",
		"exist/56499193-4570-11ee-871e-0019d14ccba2,false,false,https://localhost//2023/08/28/09/58/4856499193-4570-11ee-871e-0019d14ccba2",
		"exist/56499193-4570-11ee-871e-0019d14ccba3,false,false,https://localhost//2023/08/28/09/58/4856499193-4570-11ee-871e-0019d14ccba3",
	}
	checkInsert = []string{
		"insert/56499193-4570-11ee-871e-0019d14ccba6,false,false,https://localhost//2023/08/28/09/58/4856499193-4570-11ee-871e-0019d14ccba6",
		"insert/56499193-4570-11ee-871e-0019d14ccba2,false,false,https://localhost//2023/08/28/09/58/4856499193-4570-11ee-871e-0019d14ccba2",
		"insert/56499193-4570-11ee-871e-0019d14ccba3,false,false,https://localhost//2023/08/28/09/58/4856499193-4570-11ee-871e-0019d14ccba3",
	}
)

func TestInitSession(t *testing.T) {
	config := ReadConfig()
	sess, err := InitSession(&config)
	if err != nil {
		t.Errorf("Session not init to work instance on %s", config.Nodes)
		t.Error(err)
	}
	if !sess.Closed() {
		t.Logf("Close work session to %s", config.Nodes)
		sess.Close()
	}

	config.Nodes[0] = "127.0.0.1"
	if _, err := InitSession(&config); err == nil {
		t.Errorf("Session init to unwork instance on %s", config.Nodes)
		t.Error(err)
	}
}

func TestCreateTable(t *testing.T) {
	config := ReadConfig()
	sess, _ := InitSession(&config)
	defer sess.Close()
	createDdl := fmt.Sprintf(CreateTableTmpl, config.Keyspace, config.Column)
	dropDdl := fmt.Sprintf(DropTableTmpl, config.Keyspace, config.Column)
	if err := ExecQuery(sess, createDdl); err != nil {
		t.Errorf("Cannot create table %s", config.Column)
		t.Error(err)
	}
	if err := ExecQuery(sess, createDdl); err != nil {
		t.Errorf("Cannot recreate table %s", config.Column)
		t.Error(err)
	}
	if err := ExecQuery(sess, dropDdl); err != nil {
		t.Errorf("Cannot drop table %s", config.Column)
		t.Error(err)
	}
}

func TestEntryExist(t *testing.T) {
	config := ReadConfig()
	sess, _ := InitSession(&config)
	defer sess.Close()
	// prepare for check
	createDdl := fmt.Sprintf(CreateTableTmpl, config.Keyspace, config.Column)
	dropDdl := fmt.Sprintf(DropTableTmpl, config.Keyspace, config.Column)
	ExecQuery(sess, createDdl)
	insertQuery := fmt.Sprintf(InsertVidTmpl, config.Keyspace, config.Column)
	selectQuery := fmt.Sprintf(SelectVidTmpl, config.Keyspace, config.Column)
	// insert records
	for _, entryString := range checkExistEntrys {
		vid, archived, deleted, url := func(s string, del string) (v string, a bool, d bool, u string) {
			result := strings.Split(s, del)
			v = result[0]
			a, _ = strconv.ParseBool(result[1])
			d, _ = strconv.ParseBool(result[1])
			u = result[3]
			return
		}(entryString, ",")
		if err := sess.Query(insertQuery, vid, archived, deleted, url).Exec(); err != nil {
			t.Error(err)
		}
	}
	// check func
	for _, entryString := range checkExistEntrys {
		fsEntry, err := NewFsEntryFromString(entryString, ",")
		if err != nil {
			t.Error(err)
		}
		if !EntryExist(sess, selectQuery, fsEntry) {
			t.Error("Entry exist, but not detected")
		}
	}
	fakeFsEntry := FsEntry{
		vid:      "vid-not-exist",
		archived: false,
		deleted:  false,
		url:      "wrong-url",
	}
	if EntryExist(sess, selectQuery, fakeFsEntry) {
		t.Error("Entry not exist, but not detected")
	}
	ExecQuery(sess, dropDdl)
}

func TestUpdateEntry(t *testing.T) {
	config := ReadConfig()
	sess, _ := InitSession(&config)
	defer sess.Close()
	// prepare for check
	createDdl := fmt.Sprintf(CreateTableTmpl, config.Keyspace, config.Column)
	dropDdl := fmt.Sprintf(DropTableTmpl, config.Keyspace, config.Column)
	ExecQuery(sess, createDdl)
	// check func
	insertQuery := fmt.Sprintf(InsertVidTmpl, config.Keyspace, config.Column)
	for _, entryString := range checkInsert {
		fsEntry, err := NewFsEntryFromString(entryString, ",")
		if err != nil {
			t.Error(err)
		}
		if err := UpdateEntry(sess, insertQuery, fsEntry); err != nil {
			t.Error("Entry does not create by update")
		}
	}
	ExecQuery(sess, dropDdl)
}

func ReadConfig() conf.Config {
	var c conf.Config
	viper.SetConfigName("symfs.yaml")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("c:\\tools\\study_go\\symfs\\")
	err := viper.ReadInConfig()
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	if err := viper.Unmarshal(&c); err != nil {
		panic(fmt.Errorf("fatal on parse config file: %s", err))
	}
	return c
}
