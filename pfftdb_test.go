package pfftdb

import (
	"fmt"
	"log"
	"os"
	"testing"
)

const (
	DBNAME     = "test"
	DBUSER     = ""
	DBPASS     = ""
	ENV        = "testing"
	TESTGRAPH  = "test"
	TESTGRAPH2 = "test2"
)

var (
	DBPATH  = "localhost"
	APIPORT = "9998"

	goPath = fmt.Sprintf("%s", os.Getenv("GOPATH"))
	dbType = "mongo"
	webDir = ""
	//webDir = goPath + "/src/pfftdb/web"
	STORE   *Store
	GRPH    *Graph
	GRPH2   *Graph // so far only used for graph_test TestMerge
	TESTAPI *API
)

func init() {
	var err error
	dbpath := os.Getenv("DBPATH")
	if dbpath != "" {
		DBPATH = dbpath
	}
	apiport := os.Getenv("APIPORT")
	if dbpath != "" {
		APIPORT = apiport
	}

	STORE, err = New(APIPORT, ENV, dbType, DBPATH, DBNAME, DBUSER, DBPASS, TESTGRAPH, webDir)
	if err != nil {
		log.Fatal(err)
	}
	TESTAPI = STORE.API

	var ok bool
	GRPH, ok = STORE.Driver.Graph(TESTGRAPH)
	if !ok {
		log.Fatal("couldn't get graph")
	}

	GRPH2, err = STORE.Driver.Create(TESTGRAPH2)
	if err != nil {
		log.Fatal("couldn't create graph2")
	}
}

func TestNewDriver(t *testing.T) {
	dbConf := &DBConf{Hosts: DBPATH}
	_, err := NewDriver("mongo", dbConf)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDriver("fake", dbConf)
	if err == nil {
		t.Fatal("should get error")
	}
}

func TestCloseStore(t *testing.T) {
	//store.Close()
}
