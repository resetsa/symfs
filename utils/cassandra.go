package utils

import (
	"fmt"
	"resetsa/symfs/conf"

	"github.com/gocql/gocql"
)

const (
	CreateTableTmpl string = `CREATE TABLE IF NOT EXISTS %s.%s (
		vid text PRIMARY KEY,
		archived boolean,
		deleted boolean,
		url text
	) with gc_grace_seconds = %d;`
	DropTableTmpl     string = "DROP TABLE IF EXISTS %s.%s;"
	TruncateTableTmpl string = "TRUNCATE TABLE %s.%s;"
	SelectVidTmpl     string = "SELECT vid, url from %s.%s WHERE vid = ?;"
	InsertVidTmpl     string = "INSERT INTO %s.%s (vid, archived, deleted, url) VALUES (?,?,?,?) USING TTL %d;"
)

// struct for render CQL query
type QueryHolder struct {
	CreateSql   string
	DropSql     string
	TruncateSql string
	SelectSql   string
	InsertSql   string
}

func (Q *QueryHolder) RenderSql(keyspace string, column string, gcGracePeriod int, ttl int) {
	Q.CreateSql = fmt.Sprintf(CreateTableTmpl, keyspace, column, gcGracePeriod)
	Q.DropSql = fmt.Sprintf(DropTableTmpl, keyspace, column)
	Q.TruncateSql = fmt.Sprintf(TruncateTableTmpl, keyspace, column)
	Q.SelectSql = fmt.Sprintf(SelectVidTmpl, keyspace, column)
	Q.InsertSql = fmt.Sprintf(InsertVidTmpl, keyspace, column, ttl)
}

// stub type for disable logging
type nopLogger struct{}

func (n nopLogger) Print(_ ...interface{})            {}
func (n nopLogger) Printf(_ string, _ ...interface{}) {}
func (n nopLogger) Println(_ ...interface{})          {}

// init session to cassandra with tls
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

// run simple query
func ExecQuery(session *gocql.Session, queryStr string) error {
	return session.Query(queryStr).Exec()
}

// check entry exist
func EntryExist(session *gocql.Session, selectQuery string, entry FsEntry) (result bool) {
	iter := session.Query(selectQuery, entry.vid).Iter()
	for {
		row := make(map[string]interface{})
		// not return any data
		if !iter.MapScan(row) {
			break
		}
		result = true
	}
	return result
}

// update entry - simple insert
func UpdateEntry(session *gocql.Session, insertQuery string, entry FsEntry) error {
	return session.Query(insertQuery, entry.vid, entry.archived, entry.deleted, entry.url).Exec()
}
