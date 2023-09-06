package utils

import (
	"resetsa/symfs/conf"

	"github.com/gocql/gocql"
)

var (
	CreateTableTmpl string = `CREATE TABLE IF NOT EXISTS %s.%s (
		vid text PRIMARY KEY,
		archived boolean,
		deleted boolean,
		url text
	);`
	DropTableTmpl     string = "DROP TABLE IF EXISTS %s.%s;"
	TruncateTableTmpl string = "TRUNCATE TABLE %s.%s;"
	SelectVidTmpl     string = "SELECT vid, url from %s.%s WHERE vid = ?;"
	InsertVidTmpl     string = "INSERT INTO %s.%s (vid, archived, deleted, url) VALUES (?,?,?,?) USING TTL %d;"
)

type nopLogger struct{}

func (n nopLogger) Print(_ ...interface{})            {}
func (n nopLogger) Printf(_ string, _ ...interface{}) {}
func (n nopLogger) Println(_ ...interface{})          {}

func InitSession(config *conf.Config) (*gocql.Session, error) {
	cluster := gocql.NewCluster(config.Nodes...)
	cluster.Keyspace = config.Keyspace
	cluster.SslOpts = &gocql.SslOptions{
		CertPath:               config.Auth.CertFile,
		KeyPath:                config.Auth.KeyFile,
		CaPath:                 config.Auth.CaFile,
		EnableHostVerification: false,
	}
	cluster.Logger = nopLogger{}
	return cluster.CreateSession()
}

func ExecQuery(session *gocql.Session, queryStr string) error {
	return session.Query(queryStr).Exec()
}

func EntryExist(session *gocql.Session, selectQuery string, entry FsEntry) bool {
	result := false
	iter := session.Query(selectQuery, entry.vid).Iter()
	for {
		row := make(map[string]interface{})
		// Not return any data
		if !iter.MapScan(row) {
			break
		}
		result = true
	}
	return result
}

func UpdateEntry(session *gocql.Session, insertQuery string, entry FsEntry) error {
	return session.Query(insertQuery, entry.vid, entry.archived, entry.deleted, entry.url).Exec()
}
