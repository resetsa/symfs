package conf

import (
	"fmt"
)

// struct for describe config
type Config struct {
	Nodes    []string
	Auth     Auth
	Keyspace string
	Column   string
	TTL      int
	Timeout  int
}

type Auth struct {
	CertFile string
	KeyFile  string
	CaFile   string
}

func (C Config) String() string {
	return fmt.Sprintf("Hosts: %s\nKeyspace: %s\nColumn: %s ", C.Nodes, C.Keyspace, C.Column)
}
