package pfftdb

import (
	"log"
	"reflect"
	"testing"

	"labix.org/v2/mgo"
	//"labix.org/v2/mgo/bson"
)

const (
	MONGOHOSTS = "localhost:27017"
)

var (
	db       *mgo.Database
	MONGO    *Mongo
	TESTCOL  *mgo.Collection
	TESTCOL2 *mgo.Collection
)

func init() {
	var err error
	MONGO, err = NewMongo(MONGOHOSTS, "test", []string{"test", "test2"})
	if err != nil {
		log.Fatal(err)
	}

	sessionCopy := MONGO.Session.Copy()
	TESTCOL = sessionCopy.DB("test").C("test")
	TESTCOL2 = sessionCopy.DB("test").C("test2")

	cleanupMongo()
}

func TestGraph(t *testing.T) {
	_, ok := MONGO.Graph(TESTGRAPH)
	if !ok {
		t.Fatal("failed to get graph")
	}
}

func TestConnect(t *testing.T) {
}

func TestPinger(t *testing.T) {
}

func cleanupMongo() {
	TESTCOL.RemoveAll(nil)
	TESTCOL2.RemoveAll(nil)
	// Cannot drop collection due to unique index not recreated
	//MONGO.Graphs[TESTGRAPH].Col.DropCollection()
	//MONGO.Graphs[TESTGRAPH2].Col.DropCollection()
}

func TestIsEmpty(t *testing.T) {
	if isEmpty("") != true {
		t.Error("should be empty")
	}
	if isEmpty(nil) != true {
		t.Error("should be empty")
	}

	if isEmpty("abc") == true {
		t.Error("should not be empty")
	}
}

func TestMongoDrop(t *testing.T) {
}

func TestMongoIndex(t *testing.T) {
}

func TestMongoRemoveAll(t *testing.T) {
	//cleanupGraph()
	//defer cleanupGraph()

	data := []*Triple{
		&Triple{"a", "b", "c"},
		&Triple{"a", "b", "d"},
		&Triple{"a", "c", "d"},
		&Triple{"a", "c", "d"},
	}
	MONGO.AddBulk(TESTGRAPH, data)

	count, _ := TESTCOL.Count()
	if count != 3 {
		t.Errorf("should have only inserted 3 got %d", count)
	}

	MONGO.RemoveAll(TESTGRAPH)
	count, _ = TESTCOL.Count()
	if count != 0 {
		t.Errorf("should have 0 got %d", count)
	}

	MONGO.AddBulk(TESTGRAPH, data)
	count, _ = TESTCOL.Count()
	if count != 3 {
		t.Errorf("should have recreated indexes, have only inserted 3 got %d", count)
		sessionCopy := MONGO.Session.Copy()
		defer sessionCopy.Close()
		col := sessionCopy.DB(MONGO.DBName).C("test")
		inds, _ := col.Indexes()
		t.Errorf("Indexes: %+v", inds)
	}
}

func TestMongoAddBulkLarge(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	n := 30005

	iter := make([]struct{}, n)
	triples := []*Triple{}
	for i := range iter {
		triples = append(triples, &Triple{"aaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbb", i})
	}
	total, err := GRPH.AddBulk(TESTGRAPH, triples)
	if err != nil {
		t.Error(err)
	}
	if total != n {
		t.Error("added %d != total %d", n, total)
	}

	count, _ := TESTCOL.Count()
	if count != n {
		t.Errorf("count %d != n %d", count, n)
	}
}

func TestMongoRemoveBulkLarge(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	n := 10005

	iter := make([]struct{}, n)
	triples := []*Triple{}
	for i := range iter {
		triples = append(triples, &Triple{"aaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbb", i})
	}
	GRPH.AddBulk(TESTGRAPH, triples)
	GRPH.RemoveBulk(TESTGRAPH, triples)

	count, _ := TESTCOL.Count()
	if count != 0 {
		t.Errorf("count %d != 0", count)
	}

}

func TestMongoAddBulk(t *testing.T) {
	cleanupMongo()
	defer cleanupMongo()

	data := []*Triple{
		&Triple{"a", "b", "c"},
		&Triple{"a", "b", "d"},
		&Triple{"a", "c", "d"},
		&Triple{"a", "c", "d"},
	}
	MONGO.AddBulk(TESTGRAPH, data)

	triples := MONGO.Triples(TESTGRAPH, "", "", nil, nil)
	if len(triples) != 3 {
		t.Error("didn't get 3 triples, got:", len(triples))
		for _, tr := range triples {
			t.Log(tr)
		}
	}

	data = []*Triple{
		&Triple{"a", "b", "c"},
		&Triple{"c", "c", "d"},
		&Triple{"a", "b", "d"},
		&Triple{"a", "c", "d"},
		&Triple{"a", "c", "d"},
	}
	MONGO.AddBulk(TESTGRAPH, data)
	triples = MONGO.Triples(TESTGRAPH, "", "", nil, nil)
	if len(triples) != 4 {
		t.Error("didn't get 4 triples, got:", len(triples))
		for _, tr := range triples {
			t.Log(tr)
		}
	}
}

func TestMongoAdd(t *testing.T) {
	cleanupMongo()
	defer cleanupMongo()

	MONGO.Add(TESTGRAPH, "a", "b", "c")
	MONGO.Add(TESTGRAPH, "a", "b", 2)
	MONGO.Add(TESTGRAPH, "m", "n", "p")

	err := MONGO.Add(TESTGRAPH, "m", "n", "") // should fail
	if err == nil {
		t.Fatal("should have gotten error")
	}
	err = MONGO.Add(TESTGRAPH, "", "", "") // should fail
	if err == nil {
		t.Fatal("should have gotten error")
	}

	count, _ := TESTCOL.Count()
	if count != 3 {
		t.Error("add failed should have created 3 docs but got ", count)
	}

	// test missing graph
	err = MONGO.Add("", "m", "n", "") // should fail
	if err == nil {
		t.Fatal("should have gotten error, missing graph")
	}
}

func TestMongoRemoveBulk(t *testing.T) {
	cleanupMongo()
	defer cleanupMongo()

	data := []*Triple{
		&Triple{"a", "b", "c"},
		&Triple{"a", "b", "d"},
		&Triple{"a", "c", "d"},
	}

	MONGO.AddBulk(TESTGRAPH, data)

	MONGO.RemoveBulk(TESTGRAPH, data)

	count, _ := MONGO.Count(TESTGRAPH, "", "", nil)
	if count != 0 {
		t.Fatal("didn't remove 3 triples, got:", count)
	}
}

func TestMongoRemove(t *testing.T) {
	cleanupMongo()
	defer cleanupMongo()

	triples := MONGO.Triples(TESTGRAPH, "", "", "", nil)
	if len(triples) != 0 {
		t.Error("should start with zero items, got: ", len(triples))
	}

	// Add 8 items
	MONGO.Add(TESTGRAPH, "a", "b", "c")
	MONGO.Add(TESTGRAPH, "a", "b", 2)
	MONGO.Add(TESTGRAPH, "a", "b", 9.0)
	MONGO.Add(TESTGRAPH, "m", "n", "p")
	MONGO.Add(TESTGRAPH, "m", "n", 2)
	MONGO.Add(TESTGRAPH, "x", "y", "z")
	MONGO.Add(TESTGRAPH, "xx", "yy", "zz")
	// Add to another graph
	MONGO.Add(TESTGRAPH2, "xx", "yy", "zz")

	// remove single item
	MONGO.Remove(TESTGRAPH, "a", "", 2)
	triples = MONGO.Triples(TESTGRAPH, "", "", "", nil)
	if len(triples) != 6 {
		t.Error("remove failed should be 6 got ", len(triples))
	}

	// remove all sub
	MONGO.Remove(TESTGRAPH, "a", "", nil)
	triples = MONGO.Triples(TESTGRAPH, "", "", "", nil)
	if len(triples) != 4 {
		t.Error("remove failed should be 4 got ", len(triples))
	}

	// remove all items from graph
	MONGO.Remove(TESTGRAPH, "", "", nil)
	triples = MONGO.Triples(TESTGRAPH, "", "", "", nil)
	if len(triples) != 0 {
		t.Error("remove failed should be 0 got ", len(triples))
	}

	// test missing graph
	err := MONGO.Remove("", "m", "n", "") // should fail
	if err == nil {
		t.Fatal("should have gotten error, missing graph")
	}
}

func TestMongoTriples(t *testing.T) {
	cleanupMongo()
	defer cleanupMongo()

	MONGO.Add(TESTGRAPH, "a", "b", "c")
	MONGO.Add(TESTGRAPH, "m", "n", "p")
	MONGO.Add(TESTGRAPH, "m", "n", 2)
	MONGO.Add(TESTGRAPH, "m", "n", 3)
	MONGO.Add(TESTGRAPH, "m", "y", 3)

	// all graph triples
	// nil nil nil
	triples := MONGO.Triples(TESTGRAPH, "", "", nil, nil)
	if len(triples) != 5 {
		t.Fatal("didn't get 5 triples, got:", len(triples))
	}

	// Test sort
	triples = MONGO.Triples(TESTGRAPH, "", "", nil, &Options{OrderBy: "s"})
	if triples[0][0] != "a" {
		t.Error("sort failed")
	}

	triples = MONGO.Triples(TESTGRAPH, "", "", nil, &Options{OrderBy: "-s"})
	if triples[0][0] != "m" {
		t.Error("sort failed")
	}

	// Test limit
	triples = MONGO.Triples(TESTGRAPH, "", "", nil, &Options{Limit: 1})
	if len(triples) != 1 {
		t.Fatal("didn't get 1 triples with limit, got:", len(triples))
	}

	triplesSkip := MONGO.Triples(TESTGRAPH, "", "", nil, &Options{Limit: 1, Offset: 2})
	if len(triplesSkip) != 1 {
		t.Error("didn't get 1 triples with limit and skip of 1, got:", len(triplesSkip))
	}
	if reflect.DeepEqual(triples, triplesSkip) {
		for i, tr := range triplesSkip {
			t.Error(tr, triples[i])
		}
	}

	// sub nil nil
	triples = MONGO.Triples(TESTGRAPH, "m", "", nil, nil)
	if len(triples) != 4 {
		t.Error("didn't get 4 triples, got:", len(triples))
	}

	// sub pred nil
	triples = MONGO.Triples(TESTGRAPH, "m", "n", nil, nil)
	if len(triples) != 3 {
		t.Error("didn't get 3 triples, got:", len(triples))
	}

	// sub nil obj
	triples = MONGO.Triples(TESTGRAPH, "m", "", 2, nil)
	if len(triples) != 1 {
		t.Error("didn't get 1 triples, got:", len(triples))
	}

	// sub pred obj
	triples = MONGO.Triples(TESTGRAPH, "m", "n", 2, nil)
	if len(triples) != 1 {
		t.Error("didn't get 1 triples, got:", len(triples))
	}

	// nil pred obj
	triples = MONGO.Triples(TESTGRAPH, "", "n", 2, nil)
	if len(triples) != 1 {
		t.Error("didn't get 1 triples, got:", len(triples))
	}

	// nil nil obj
	triples = MONGO.Triples(TESTGRAPH, "", "", 2, nil)
	if len(triples) != 1 {
		t.Error("didn't get 1 triples, got:", len(triples))
	}

	// test missing graph
	triples = MONGO.Triples("", "", "", "", nil)
	if triples != nil {
		t.Error("should have gotten no triples, missing graph")
	}
}

func TestClose(t *testing.T) {
	//MONGO.Close()
}
