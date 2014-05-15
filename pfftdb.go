package pfftdb

import (
	"fmt"
	"strings"

	log "github.com/golang/glog"
)

// DBConf holds connection information for a database type.
type DBConf struct {
	Name   string // db name
	Hosts  string
	User   string
	Pass   string
	Graphs []string
}

// Store
type Store struct {
	Port   string // socket to listen on
	Env    string // environment
	Driver Driver
	API    *API
}

// Filter is used for query filters.
type Filter struct {
	Key string      `json:"key"`
	Op  string      `json:"op"`
	Val interface{} `json:"val"`
}

// Overrides help with triple query optimizations.
type Overrides struct {
	Subs  []string
	Preds []string
	Objs  []interface{}
}

// Options are the query and triples options.
type Options struct {
	Limit           uint       `json:"limit"`
	Offset          uint       `json:"offset"`
	OrderBy         string     `json:"orderby"`
	Filter          []*Filter  `json:"filter"`   //query only
	Distinct        bool       `json:"distinct"` // query only
	Select          []string   `json:"select"`   // query only
	Optional        []uint     `json:"optional"` // query only
	TripleOverrides *Overrides // used by Query to get triples.
}

// Driver defines the functionality for a datastore driver.
type Driver interface {
	Graph(string) (*Graph, bool)
	GraphsList() []string
	Create(string) (*Graph, error)
	Connect(string)
	AddBulk(string, []*Triple) (int, error)
	RemoveBulk(string, []*Triple) error
	Add(string, string, string, interface{}) error
	Drop(string) error        // drop graph and indexes
	Index(string, bool) error // index graph
	Remove(string, string, string, interface{}) error
	RemoveAll(string) error
	Count(string, string, string, interface{}) (uint, error)
	Triples(string, string, string, interface{}, *Options) []*Triple
	Pinger()
	Close()
}

// NewDriver
func NewDriver(driverType string, dbConf *DBConf) (Driver, error) {
	switch driverType {
	case "mongo":
		m, err := NewMongo(dbConf.Hosts, dbConf.Name, dbConf.Graphs)
		if err != nil {
			return nil, err
		}
		return m, nil
	case "postgres":
		return nil, fmt.Errorf("not yet implemented: %s", driverType)
	}
	return nil, fmt.Errorf("driver not defined: %s", driverType)
}

// New initializes a new store with graph.
// graphs creates or uses each graph name provided.
func New(httpAPIPort, env, dbType, dbHosts, dbName, dbUser, dbPass, graphs, webDir string) (*Store, error) {
	gs := strings.Split(graphs, ",")
	dbConf := &DBConf{
		Name:   dbName,
		Hosts:  dbHosts,
		User:   dbUser,
		Pass:   dbPass,
		Graphs: gs,
	}

	d, err := NewDriver(dbType, dbConf)
	if err != nil {
		log.Fatal(err)
	}

	var a *API
	a, err = NewAPI(httpAPIPort, env, webDir, d)
	if err != nil {
		log.Fatal(err)
	}

	s := &Store{
		Env:    env,
		Driver: d,
		API:    a,
	}

	return s, nil
}

// Close [...]
func (s *Store) Close() {

}
