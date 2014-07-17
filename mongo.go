package pfftdb

import (
	"fmt"
	"io"
	"math/rand"
	//lg "log"
	//"os"
	"sync"
	"time"

	log "github.com/golang/glog"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

// isEmpty just checks if the value given is empty.
func isEmpty(val interface{}) bool {
	if val == "" || val == nil {
		return true
	}
	return false
}

// MongoGraph
type MongoGraph struct {
	Graph   *Graph
	ColName string
}

// Mongo
type Mongo struct {
	Session *mgo.Session
	Hosts   string
	DBName  string // db name
	Graphs  map[string]*MongoGraph
	muGraph sync.Mutex
}

// TripleDoc represents the triplestore, where Objs is a set of interface{}
type TripleDoc struct {
	//GraphID string      "g"
	Sub  string      "s"
	Pred string      "p"
	Obj  interface{} "o"
}

// NewMongo
func NewMongo(hosts, dbName string, graphs []string) (*Mongo, error) {
	m := &Mongo{
		Hosts:   hosts,
		DBName:  dbName,
		Graphs:  map[string]*MongoGraph{},
		muGraph: sync.Mutex{},
	}
	m.Connect(hosts)
	for _, graphName := range graphs {
		_, err := m.Create(graphName)
		if err != nil {
			log.Error(err)
			continue
		}
	}

	return m, nil
}

// Graphs returns a list of created graphs.
// TODO run through collections as well and get distinct graphs.
func (m *Mongo) GraphsList() []string {
	graphs := []string{}
	graphsSet := map[string]bool{}

	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	db := sessionCopy.DB(m.DBName)

	names, err := db.CollectionNames()
	if err != nil {
		log.Error(err)
		return graphs
	}
	for _, name := range names {
		if name == "system.indexes" {
			continue
		}
		graphsSet[name] = true
		c := db.C(name)
		var gNames []string
		c.Find(bson.M{}).Distinct("g", gNames)
		for _, gName := range gNames {
			graphsSet[gName] = true
		}
	}

	for k, _ := range graphsSet {
		graphs = append(graphs, k)
	}
	return graphs
}

// Graph returns the internal graph given a name.
func (m *Mongo) Graph(name string) (*Graph, bool) {
	m.muGraph.Lock()
	defer m.muGraph.Unlock()
	g, ok := m.Graphs[name]
	if ok {
		return g.Graph, true
	}
	return nil, false
}

// Drop drops a collection
func (m *Mongo) Drop(gid string) error {
	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(gid)
	err := col.DropCollection()

	if err != nil {
		log.Error(err, " graph:", gid)
		return err
	}
	return nil
}

// Index creates the default indeces for the graph.
func (m *Mongo) Index(gid string, background bool) error {
	m.Session.ResetIndexCache()

	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(gid)

	cInfo := &mgo.CollectionInfo{DisableIdIndex: true}
	err := col.Create(cInfo)
	if err != nil {
		log.Error(err)
	}

	/*
		// TODO figure out the magic of mongo indexes
		index := mgo.Index{
			Key:        []string{"g", "s", "p", "o"},
			Background: false,
			Sparse:     true,
			Unique:     true,
			DropDups:   true,
		}
		err := col.EnsureIndex(index)
		return err
	*/

	index := mgo.Index{
		Key:        []string{"g", "s"},
		Background: background,
		Sparse:     true,
	}
	err = col.EnsureIndex(index)
	if err != nil {
		log.Error(err)
		//return err
	}
	log.V(2).Infof("%+v", index)

	index.Key = []string{"g", "o"}
	err = col.EnsureIndex(index)
	if err != nil {
		log.Error(err)
		//return err
	}
	log.V(2).Infof("%+v", index)

	index.Key = []string{"g", "p"}
	err = col.EnsureIndex(index)
	if err != nil {
		log.Error(err)
		//return err
	}
	log.V(2).Infof("%+v", index)

	index.Key = []string{"g", "s", "p"}
	err = col.EnsureIndex(index)
	if err != nil {
		log.Error(err)
		//return err
	}
	log.V(2).Infof("%+v", index)

	index.Key = []string{"g", "s", "o"}
	err = col.EnsureIndex(index)
	if err != nil {
		log.Error(err)
		//return err
	}
	log.V(2).Infof("%+v", index)

	index.Key = []string{"g", "p", "o"}
	err = col.EnsureIndex(index)
	if err != nil {
		log.Error(err)
		//return err
	}
	log.V(2).Infof("%+v", index)

	index.Key = []string{"g", "s", "p", "o"}
	index.Unique = true
	index.DropDups = true
	err = col.EnsureIndex(index)
	if err != nil {
		log.Error(err)
		//return err
	}
	log.V(2).Infof("%+v", index)

	return nil
}

// Create adds a graph
func (m *Mongo) Create(name string) (*Graph, error) {
	if name == "" {
		return nil, fmt.Errorf("missing name")
	}
	m.muGraph.Lock()
	defer m.muGraph.Unlock()

	// Check if graph already exists
	g, ok := m.Graphs[name]
	if ok {
		return g.Graph, nil
	}

	err := m.Index(name, true)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	m.Graphs[name] = &MongoGraph{}
	m.Graphs[name].Graph, err = NewGraph(name, m)
	if err != nil {
		log.Error(err)
		delete(m.Graphs, name)
		return nil, err
	}
	m.Graphs[name].ColName = name
	return m.Graphs[name].Graph, nil
}

// Connect establishes a database connection.
func (m *Mongo) Connect(hosts string) {
	log.Infof("connecting session to hosts:%s", hosts)

	for {
		session, err := mgo.DialWithTimeout(m.Hosts, 10*time.Second)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Second)
			continue
		}
		session.SetMode(mgo.Strong, true)
		//mgo.SetDebug(true)
		//mgo.SetLogger(lg.New(os.Stderr, "", lg.LstdFlags))
		session.SetSocketTimeout(120 * time.Second)

		m.Session = session

		go m.Pinger()
		return
	}
}

// AddBulk bulk inserts documents. It removes invalid ones and returns the number inserted.
// This has inconsistency issues with inserting bulk and error handling because of mongo.
// If a bulk insert fails mongo stops, the driver doesnt currently support continue on error.
// if the err is EOF there is no way to know the number of documents inserted, so the total
// returned may be zero, but there may have still been inserts......
func (m *Mongo) AddBulk(graph string, triples []*Triple) (int, error) {
	g, ok := m.Graphs[graph]
	if !ok {
		return 0, fmt.Errorf("graph not found %s", graph)
	}
	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(g.ColName)

	tripleDocs := []interface{}{}
	for _, tr := range triples {
		if tr == nil {
			continue
		}
		sub, ok := tr[0].(string)
		if !ok || sub == "" {
			continue
		}
		pred, ok := tr[1].(string)
		if !ok || pred == "" {
			continue
		}
		if isEmpty(tr[2]) {
			continue
		}
		tripleDocs = append(tripleDocs, bson.M{"g": graph, "s": tr[0], "p": tr[1], "o": tr[2]})
	}

	// TODO go back to bulk insert when continueOnError added to Insert in mgo driver
	total := len(tripleDocs)
	if total == 0 {
		return 0, nil
	}
	var err error

	// mongo has a maxMessageSizeBytes, so split up if docs too large.
	// tries bulk insert, then if err occurs does individual inserts.
	if total > 10000 {
		start := 0
		const inc = 10000
		for start < total {
			if start+inc > total {
				// finish off remainder
				err = col.Insert(tripleDocs[start:]...)
				if err == io.EOF {
					return 0, err
				}

				if err != nil {
					for _, t := range tripleDocs[start:] {
						err = col.Insert(t)
						if err != nil {
							total--
						}
					}
				}
				break
			}
			err = col.Insert(tripleDocs[start : start+inc]...)
			if err == io.EOF {
				return 0, err
			}

			if err != nil {
				for _, t := range tripleDocs[start : start+inc] {
					err = col.Insert(t)
					if err != nil {
						total--
					}
				}
			}
			start += inc
		}
	} else if total > 0 {
		err = col.Insert(tripleDocs...)
		if err != nil {
			for _, t := range tripleDocs {
				err = col.Insert(t)
				total--
			}
		}
	}
	if err != nil {
		errStr := err.Error()
		// ignore duplicate key error
		if len(errStr) > 6 && errStr[:6] == "E11000" {
			return total, nil
		}
		log.Error(err)
	}
	return total, err
}

// Add upserts a triple with the given graph ID. Currently not in use, AddBulk instead.
func (m *Mongo) Add(graph, sub, pred string, obj interface{}) error {
	g, ok := m.Graphs[graph]
	if !ok {
		return fmt.Errorf("graph not found %s", graph)
	}
	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(g.ColName)

	if graph == "" || sub == "" || pred == "" || isEmpty(obj) {
		return fmt.Errorf("missing components graph:%s sub:%s pred:%s obj:%s", graph, sub, pred, obj)
	}

	trDoc := bson.M{"g": graph, "s": sub, "p": pred, "o": obj}
	_, err := col.Upsert(trDoc, trDoc)
	if err != nil {
		log.Error(err)
	}
	return err
}

// RemoveBulk builds each query from a triple and calls remove. $or doesn't use the index
func (m *Mongo) RemoveBulk(graph string, triples []*Triple) error {
	g, ok := m.Graphs[graph]
	if !ok {
		return fmt.Errorf("graph not found %s", graph)
	}
	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(g.ColName)

	for _, tr := range triples {
		if tr == nil {
			continue
		}
		if sub, ok := tr[0].(string); ok {
			if pred, ok := tr[1].(string); ok {
				query := m.BuildQuery(graph, sub, pred, tr[2], nil)
				if sub == "" && pred == "" && (tr[2] == nil || tr[2] == "") {
					return m.RemoveAll(graph)
				} else {
					_, err := col.RemoveAll(query)
					if err == io.EOF {
						return err
					}
				}
			}
		}
	}
	return nil
}

// RemoveAll clears out a collection named graph. It then rebuilds the indexes.
func (m *Mongo) RemoveAll(graph string) error {
	if _, ok := m.Graphs[graph]; ok {
		err := m.Drop(graph)
		if err != nil {
			log.Error(err)
			return err
		}

		err = m.Index(graph, true)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	return nil
}

// Remove removes set of triples from a graph depending on the given sub, pred, obj.
func (m *Mongo) Remove(graph, sub, pred string, obj interface{}) error {
	g, ok := m.Graphs[graph]
	if !ok {
		return fmt.Errorf("graph not found %s", graph)
	}

	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(g.ColName)

	var err error
	sEmpty := isEmpty(sub)
	pEmpty := isEmpty(pred)
	oEmpty := isEmpty(obj)

	// TODO move switches to most likely order.
	switch {
	case sEmpty && pEmpty && oEmpty:
		// nil nil nil
		_, err = col.RemoveAll(bson.M{"g": graph})
	case sEmpty && pEmpty && !oEmpty:
		// nil nil obj
		_, err = col.RemoveAll(bson.M{"g": graph, "o": obj})
	case sEmpty && !pEmpty && !oEmpty:
		// nil pred obj
		_, err = col.RemoveAll(bson.M{"g": graph, "p": pred, "o": obj})
	case !sEmpty && !pEmpty && !oEmpty:
		// sub pred obj
		_, err = col.RemoveAll(bson.M{"g": graph, "s": sub, "p": pred, "o": obj})
	case !sEmpty && pEmpty && oEmpty:
		// sub nil nil
		_, err = col.RemoveAll(bson.M{"g": graph, "s": sub})
	case !sEmpty && !pEmpty && oEmpty:
		// sub pred nil
		_, err = col.RemoveAll(bson.M{"g": graph, "s": sub, "p": pred})
	case !sEmpty && pEmpty && !oEmpty:
		// sub nil obj
		_, err = col.RemoveAll(bson.M{"g": graph, "s": sub, "o": obj})
	}

	return err
}

// Count
func (m *Mongo) Count(graph, sub, pred string, obj interface{}) (uint, error) {
	g, ok := m.Graphs[graph]
	if !ok {
		log.Error("missing graph ", graph)
		return 0, fmt.Errorf("missing graph %s", graph)
	}
	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(g.ColName)

	query := m.BuildQuery(graph, sub, pred, obj, nil)
	count, err := col.Find(query).Count()
	return uint(count), err
}

// Build query creates a mongo query from the given sub, pred, obj
func (m *Mongo) BuildQuery(graph, sub, pred string, obj interface{}, overrides *Overrides) bson.M {
	query := bson.M{"g": graph}

	sEmpty := isEmpty(sub)
	pEmpty := isEmpty(pred)
	oEmpty := isEmpty(obj)

	switch {
	case graph == "":
		// all items in collection, never executed
	case sEmpty && pEmpty && oEmpty:
		// nil nil nil
	case !sEmpty && pEmpty && oEmpty:
		// sub nil nil
		query["s"] = sub
	case !sEmpty && !pEmpty && oEmpty:
		// sub pred nil
		query["s"] = sub
		query["p"] = pred
	case !sEmpty && pEmpty && !oEmpty:
		// sub nil obj
		query["s"] = sub
		query["o"] = obj
	case !sEmpty && !pEmpty && !oEmpty:
		// sub pred obj
		query["s"] = sub
		query["p"] = pred
		query["o"] = obj
	case sEmpty && !pEmpty && oEmpty:
		// nil pred nil
		query["p"] = pred
	case sEmpty && pEmpty && !oEmpty:
		// nil nil obj
		query["o"] = obj
	case sEmpty && !pEmpty && !oEmpty:
		// nil pred obj
		query["p"] = pred
		query["o"] = obj
	}
	if overrides != nil {
		if len(overrides.Subs) > 0 {
			query["s"] = bson.M{"$in": overrides.Subs}
		}
		if len(overrides.Preds) > 0 {
			query["p"] = bson.M{"$in": overrides.Preds}
		}
		if len(overrides.Objs) > 0 {
			query["o"] = bson.M{"$in": overrides.Objs}
		}
	}
	return query
}

// Triples
func (m *Mongo) Triples(graph, sub, pred string, obj interface{}, options *Options) []*Triple {
	g, ok := m.Graphs[graph]
	if !ok {
		log.Error("missing graph ", graph)
		return nil
	}
	sessionCopy := m.Session.Copy()
	defer sessionCopy.Close()
	col := sessionCopy.DB(m.DBName).C(g.ColName)

	var query bson.M
	if options != nil {
		query = m.BuildQuery(graph, sub, pred, obj, options.TripleOverrides)
	} else {
		query = m.BuildQuery(graph, sub, pred, obj, nil)
	}
	// Note that skip only makes sense in the case of sorted results, so if
	// no orderby is given a default subject is used.
	var iter *mgo.Iter
	switch {
	default:
		// no options
		iter = col.Find(query).Iter()
	case options == nil:
		// no options
		iter = col.Find(query).Iter()
	case options.Limit == 0 && options.Offset == 0 && options.OrderBy == "":
		// no options
		iter = col.Find(query).Iter()
	case options.Limit != 0 && options.Offset != 0 && options.OrderBy != "":
		// limit,  orderby
		iter = col.Find(query).Limit(int(options.Limit)).Skip(int(options.Offset)).Sort(options.OrderBy).Iter()
	case options.Limit != 0 && options.Offset != 0 && options.OrderBy == "":
		// limit, skip
		iter = col.Find(query).Limit(int(options.Limit)).Skip(int(options.Offset)).Sort("s").Iter()
	case options.Limit != 0 && options.Offset == 0 && options.OrderBy != "":
		// limit, orderby
		iter = col.Find(query).Limit(int(options.Limit)).Sort(options.OrderBy).Iter()
	case options.Limit != 0 && options.Offset == 0 && options.OrderBy == "":
		// limit
		iter = col.Find(query).Limit(int(options.Limit)).Iter()
	case options.Limit == 0 && options.Offset != 0 && options.OrderBy != "":
		// skip, orderby
		iter = col.Find(query).Skip(int(options.Offset)).Sort(options.OrderBy).Iter()
	case options.Limit == 0 && options.Offset != 0 && options.OrderBy == "":
		// skip
		iter = col.Find(query).Skip(int(options.Offset)).Sort("s").Iter()
	case options.Limit == 0 && options.Offset == 0 && options.OrderBy != "":
		// orderby
		iter = col.Find(query).Sort(options.OrderBy).Iter()
	}

	tripleDocs := []*TripleDoc{}
	err := iter.All(&tripleDocs)
	if err != nil {
		log.Error(err)
		return nil
	}
	results := []*Triple{}
	for _, res := range tripleDocs {
		results = append(results, &Triple{res.Sub, res.Pred, res.Obj})
	}

	return results
}

// Pinger checks for connection loss. It starts at a random
// time to prevent all apps pinging simultaneously. Pings are
// sent every 5 seconds.
func (m *Mongo) Pinger() {
	rand.Seed(time.Now().UTC().UnixNano())
	// Start pinger on a random schedule
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	for {
		log.Infof("ping hosts:%s", m.Hosts)
		err := m.Session.Ping()
		if err != nil {
			log.Error(err)
			m.Connect(m.Hosts)
			return
		}
		time.Sleep(40 * time.Second)
	}
}

// Close shuts down the mongo db session.
func (m *Mongo) Close() {
	m.Session.Close()
}
