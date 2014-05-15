package client

import (
	"fmt"
	"log"
	"testing"

	"github.com/pkar/pfftdb"
)

const (
	httpAPIPort = "9977"
	env         = "testing"
	dbType      = "mongo"
	dbName      = "test"
	dbPath      = "localhost"
	dbUser      = ""
	dbPass      = ""
	TESTGRAPH   = "test"
)

var (
	store *pfftdb.Store
	cl    *Client
)

func init() {
	var err error
	store, err = pfftdb.New(httpAPIPort, env, dbType, dbPath, dbName, dbUser, dbPass, TESTGRAPH, "")
	if err != nil {
		log.Fatal(err)
	}
}

func cleanup() {
	tr, err := cl.Triples(TESTGRAPH, "", "", "", nil)
	if err != nil {
		log.Println(err)
	}
	cl.Remove(TESTGRAPH, tr)
}

func TestNewClient(t *testing.T) {
	var err error
	cl, err = NewClient(fmt.Sprintf("localhost:%s,localhost:%s", httpAPIPort, httpAPIPort))
	if err != nil {
		t.Fatal(err)
	}
}

func TestAdd(t *testing.T) {
	cleanup()
	defer cleanup()

	triples := []*pfftdb.Triple{&pfftdb.Triple{"a", "b", "c"}}
	_, err := cl.Add(TESTGRAPH, triples)
	if err != nil {
		t.Error(err)
	}

	tr, err := cl.Triples(TESTGRAPH, "", "", "", nil)
	if err != nil {
		t.Error(err)
	}
	if len(tr) != 1 {
		t.Error("length should be 1")
	}
}

func TestRemove(t *testing.T) {
	cleanup()
	defer cleanup()

	triples := []*pfftdb.Triple{&pfftdb.Triple{"a", "b", "c"}}
	_, err := cl.Add(TESTGRAPH, triples)
	if err != nil {
		t.Error(err)
	}
	err = cl.Remove(TESTGRAPH, triples)
	if err != nil {
		t.Error(err)
	}

	tr, err := cl.Triples(TESTGRAPH, "", "", "", nil)
	if err != nil {
		t.Error(err)
	}
	if len(tr) != 0 {
		t.Error("length should be 0")
	}

}

func TestValue(t *testing.T) {
	cleanup()
	defer cleanup()

	triples := []*pfftdb.Triple{
		&pfftdb.Triple{"a", "b", "c"},
		&pfftdb.Triple{"d", "e", "f"},
	}
	cl.Add(TESTGRAPH, triples)

	val, err := cl.Value(TESTGRAPH, "", "b", "c")
	if err != nil {
		t.Error(err)
	}

	if val != "a" {
		t.Error("didn't get back a got: ", val)
	}

	val, err = cl.Value(TESTGRAPH, "a", "", "c")
	if err != nil {
		t.Error(err)
	}

	if val != "b" {
		t.Error("didn't get back b got: ", val)
	}

	val, err = cl.Value(TESTGRAPH, "a", "b", "")
	if err != nil {
		t.Error(err)
	}

	if val != "c" {
		t.Error("didn't get back c got: ", val)
	}

	val, err = cl.Value(TESTGRAPH, "aa", "b", "")
	if err != nil {
		t.Error(err)
	}

	if val != nil {
		t.Error("didn't get back empty result got: ", val)
	}
}

func TestTriples(t *testing.T) {
	cleanup()
	defer cleanup()

	triples := []*pfftdb.Triple{
		&pfftdb.Triple{"a", "b", "c"},
		&pfftdb.Triple{"d", "e", "f"},
	}
	cl.Add(TESTGRAPH, triples)
	tr, err := cl.Triples(TESTGRAPH, "", "", "", nil)
	if err != nil {
		t.Error(err)
	}
	if len(tr) != 2 {
		t.Error("length should be 2")
	}
}

func TestCount(t *testing.T) {
	cleanup()
	defer cleanup()

	triples := []*pfftdb.Triple{
		&pfftdb.Triple{"a", "b", "c"},
		&pfftdb.Triple{"d", "e", "f"},
		&pfftdb.Triple{"d", "d", "f"},
	}
	cl.Add(TESTGRAPH, triples)
	c, err := cl.Count(TESTGRAPH, "", "", "")
	if err != nil {
		t.Error(err)
	}
	if c != 3 {
		t.Error("length should be 3")
	}
}

func TestQuery(t *testing.T) {
	cleanup()
	defer cleanup()

	triples := []*pfftdb.Triple{
		&pfftdb.Triple{"a", "b", "c"},
		&pfftdb.Triple{"d", "e", "f"},
	}
	cl.Add(TESTGRAPH, triples)
	clauses := []*pfftdb.Triple{
		&pfftdb.Triple{"?a", "b", "c"},
	}
	bindings, err := cl.Query(TESTGRAPH, clauses, nil)
	if err != nil {
		t.Error(err)
	}
	if bindings[0]["a"] != "a" {
		t.Error(bindings)
	}
}

func TestInference(t *testing.T) {
	cleanup()
	defer cleanup()
	err := cl.Inference(TESTGRAPH, "geo")
	if err != nil {
		t.Error(err)
	}
}

func TestPath(t *testing.T) {
	cleanup()
	defer cleanup()

}
