package pfftdb

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
)

const (
	// subject-predicate nil value
	SPEMPTY = ""
)

// Bindings is a map of variable bindings.
type Bindings map[string]interface{}

// Triple subject-predicate-object
type Triple [3]interface{}

// Graph [...]
type Graph struct {
	GraphID string
	Driver  Driver
	mu      *sync.Mutex
}

// Adjacent is used to build a graph traversal path
type Adjacent struct {
	ID   string
	Next *Adjacent
}

// bindingChunks are for query and split up
// large slices of bindings into chunks to be processed in
// goroutines.
func bindingChunks(b []Bindings, n int) [][]Bindings {
	l := len(b)
	if l == 0 {
		return nil
	}
	if l <= n {
		return [][]Bindings{b[:l]}
	}

	total := (l / n)
	if l%n != 0 {
		total++
	}
	chunks := make([][]Bindings, total)
	for i, _ := range chunks {
		start := i*n + i
		end := i*n + i + n
		if start >= end || end >= total {
			break
		}
		if end > l {
			end = l
		}
		chunks[i] = b[start:end]
	}

	return chunks
}

// SubPred converts interface subject and predictate to string
// and returns error if not possible.
func SubPred(s, p interface{}) (string, string, error) {
	if sub, ok := s.(string); ok {
		if pred, ok := p.(string); ok {
			return sub, pred, nil
		}
	}
	return "", "", fmt.Errorf("sub or pred not string %v %v", s, p)
}

// New [...]
func NewGraph(id string, driver Driver) (*Graph, error) {
	if id == "" {
		return nil, fmt.Errorf("graph id not given")
	}
	g := &Graph{
		GraphID: id,
		Driver:  driver,
		mu:      &sync.Mutex{},
	}
	return g, nil
}

// copy duplicates a binding item
func (b Bindings) copy() Bindings {
	tmp := make(Bindings, len(b))
	for k, v := range b {
		tmp[k] = v
	}
	return tmp
}

// AddBulk
func (g *Graph) AddBulk(graph string, triples []*Triple) (int, error) {
	start := time.Now()
	defer func() { log.Info("Graph.AddBulk ", time.Since(start)) }()

	return g.Driver.AddBulk(graph, triples)
}

// RemoveBulk
func (g *Graph) RemoveBulk(graph string, triples []*Triple) error {
	start := time.Now()
	defer func() { log.Info("Graph.RemoveBulk ", time.Since(start)) }()

	return g.Driver.RemoveBulk(graph, triples)
}

// Add adds a single triple
func (g *Graph) Add(sub, pred string, obj interface{}) error {
	start := time.Now()
	defer func() { log.Info("Graph.Add", time.Since(start)) }()

	if sub == "" {
		return fmt.Errorf("missing SUB sub:%s - pred:%s - obj:%v", sub, pred, obj)
	}
	if pred == "" {
		return fmt.Errorf("missing PRED sub:%s - pred:%s - obj:%v", sub, pred, obj)
	}
	if obj == nil || obj == "" {
		return fmt.Errorf("missing OBJ sub:%s - pred:%s - obj:%v", sub, pred, obj)
	}

	err := g.Driver.Add(g.GraphID, sub, pred, obj)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// Remove removes a triple.
func (g *Graph) Remove(sub, pred string, obj interface{}) error {
	start := time.Now()
	defer func() { log.Info("Graph.Remove ", time.Since(start)) }()

	err := g.Driver.Remove(g.GraphID, sub, pred, obj)
	if err != nil {
		//log.Error(err)
	}
	return err
}

// Drop removes a graph.
func (g *Graph) Drop(gid string) error {
	start := time.Now()
	defer func() { log.Info("Graph.Drop ", time.Since(start)) }()

	err := g.Driver.Drop(gid)
	if err != nil {
		log.Error(err)
	}
	return err
}

// Index indexes a graph.
func (g *Graph) Index(gid string, background bool) error {
	start := time.Now()
	defer func() { log.Info("Graph.Index ", time.Since(start)) }()

	err := g.Driver.Index(gid, background)
	if err != nil {
		log.Error(err)
	}
	return err
}

// Triples get triples for a query from the driver.
func (g *Graph) Triples(sub, pred string, obj interface{}, options *Options) ([]*Triple, error) {
	start := time.Now()
	defer func() { log.Info("Graph.Triples ", time.Since(start)) }()

	triples := g.Driver.Triples(g.GraphID, sub, pred, obj, options)
	return triples, nil
}

// Count get the number of triples for a query from the driver.
func (g *Graph) Count(sub, pred string, obj interface{}) (uint, error) {
	start := time.Now()
	defer func() { log.Info("Graph.Count ", time.Since(start)) }()

	count, err := g.Driver.Count(g.GraphID, sub, pred, obj)
	return count, err
}

// Value returns a singular value within a triple. if any of sub, pred or obj is empty that is the value.
func (g *Graph) Value(sub, pred string, obj interface{}) (interface{}, error) {
	start := time.Now()
	defer func() { log.Info("Graph.Value ", time.Since(start)) }()

	triples, err := g.Triples(sub, pred, obj, &Options{Limit: 1})
	if err != nil {
		log.Error(err)
		return nil, err
	}
	for _, trp := range triples {
		s, p, err := SubPred(trp[0], trp[1])
		if err == nil {
			if sub == SPEMPTY {
				return s, nil
			}
			if pred == SPEMPTY {
				return p, nil
			}
		}
		return trp[2], nil
	}
	return nil, fmt.Errorf("not found")
}

// Merge merges another graph with this one,
// if the two have consistent identifiers, magic happens
func (g *Graph) Merge(g2 *Graph) error {
	start := time.Now()
	defer func() { log.Info("Graph.Merge ", time.Since(start)) }()

	triples, err := g2.Triples(SPEMPTY, SPEMPTY, nil, nil)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, trp := range triples {
		sub, pred, err := SubPred(trp[0], trp[1])
		if err == nil {
			g.Add(sub, pred, trp[2])
		}
	}
	return nil
}

// queryBinding creates a new binding if the given triple is a match, otherwise
// it returns nil.
func queryBinding(binding Bindings, triple *Triple, bindingPositions map[string]uint) *Bindings {
	var tmp Bindings
	for variable, position := range bindingPositions {
		// if variable isn't in binding, add it to new tmp binding.
		if _, ok := binding[variable]; !ok {
			if tmp == nil {
				tmp = Bindings{variable: triple[position]}
			} else {
				tmp[variable] = triple[position]
			}
			continue
		}

		// variable is in binding already, check if it matches the triples
		// position, if not don't add this binding
		if binding[variable] != triple[position] {
			tmp = nil
			return nil
		}
	}
	if tmp == nil {
		return &binding
	}
	for k, v := range binding {
		tmp[k] = v
	}
	return &tmp
}

// Query takes an array of triple bindings, ie [[?id, "something", "?var2"],...]
// and returns an array of the given variables with ?
func (g *Graph) Query(clauses []*Triple, options *Options) (bindings []Bindings) {
	start := time.Now()
	defer func() { log.Info("Graph.Query ", time.Since(start)) }()

	if options == nil {
		options = &Options{}
	}
	optionalMap := map[uint]bool{}
	for _, key := range options.Optional {
		optionalMap[key] = true
	}

	// iterate each clause noting the position of ?variables
	// replace each ?variable with EMPTY to use the Triples method
	for clauseIndex, clause := range clauses {
		bindingPositions := map[string]uint{}
		query := &Triple{}

		// build a query from the given clause
		for i, itemInterface := range clause {
			item, ok := itemInterface.(string)
			if ok && strings.HasPrefix(item, "?") {
				query[i] = SPEMPTY
				bindingPositions[item[1:]] = uint(i)
				continue
			}
			query[i] = item
		}

		// make sure query sub and pred are strings
		sub, pred, err := SubPred(query[0], query[1])
		if err != nil {
			continue
		}
		opts := &Options{}
		// add overiddes for query optimizations.
		if len(bindings) > 0 && len(bindingPositions) > 0 {
			opts.TripleOverrides = &Overrides{Subs: []string{}, Preds: []string{}, Objs: []interface{}{}}
			for bindingKey, bindingPos := range bindingPositions {
				for _, binding := range bindings {
					if val, ok := binding[bindingKey]; ok {
						switch bindingPos {
						case 0:
							if v, ok := val.(string); ok {
								opts.TripleOverrides.Subs = append(opts.TripleOverrides.Subs, v)
							}
						case 1:
							if v, ok := val.(string); ok {
								opts.TripleOverrides.Preds = append(opts.TripleOverrides.Preds, v)
							}
						case 2:
							opts.TripleOverrides.Objs = append(opts.TripleOverrides.Objs, val)
						}
					}
				}
			}
		}
		triples, err := g.Triples(sub, pred, query[2], opts)
		if err != nil {
			log.Error(err)
			bindings = nil
			return
		}
		if len(triples) == 0 {
			if _, ok := optionalMap[uint(clauseIndex)]; !ok {
				bindings = nil
				return
			}
			continue
		}

		if len(bindings) == 0 {
			// The first time around it looks at the position
			// of the variable in the clause and matches
			// it to an existing binding
			bindings = make([]Bindings, len(triples))
			for i, triple := range triples {
				binding := make(Bindings, len(bindingPositions))
				for variable, position := range bindingPositions {
					binding[variable] = triple[position]
				}
				bindings[i] = binding
			}
			continue
		}

		// each triple is compared to existing bindings
		// if it matches its added to the bindings
		// or if not it is removed
		newBindings := []Bindings{}

		//t := time.Now()
		for _, binding := range bindings {
			for _, triple := range triples {
				tmp := queryBinding(binding, triple, bindingPositions)
				if tmp != nil {
					newBindings = append(newBindings, *tmp)
				}
			}
		}
		//log.Error(time.Since(t))
		bindings = newBindings

		/*
			tmpBindings := make([]Bindings, len(bindings)*len(triples))
			wg := &sync.WaitGroup{}
			i := 0
			for _, binding := range bindings {
				wg.Add(1)
				go func(binding_ Bindings, i_ int) {
					for _, triple := range triples {
						defer wg.Done()
						tmp := queryBinding(binding_, triple, bindingPositions)
						if tmp != nil && len(*tmp) > 0 {
							tmpBindings[i_] = *tmp
						}
						i++
					}
				}(binding, i)
			}
			wg.Wait()

			bindings = nil
			for _, b := range tmpBindings {
				if b != nil {
					bindings = append(bindings, b)
				}
			}
			tmpBindings = nil
		*/
	}

	// filter results TODO refactor hell
	if len(options.Filter) > 0 {
		filterSlice := []Bindings{}
		for _, filter := range options.Filter {
			for _, b := range bindings {
				switch filter.Val.(type) {
				case int:
					var val int
					var ok bool
					if val, ok = b[filter.Key].(int); !ok {
						continue
					}
					switch filter.Op {
					case ">":
						if val < filter.Val.(int) {
							continue
						}
					case "<":
						if val > filter.Val.(int) {
							continue
						}
					case "==":
						if val != filter.Val.(int) {
							continue
						}
					case "!=":
						if val == filter.Val.(int) {
							continue
						}
					case "<=":
						if val >= filter.Val.(int) {
							continue
						}
					case ">=":
						if val <= filter.Val.(int) {
							continue
						}
					}
				case float64:
					var val float64
					var ok bool
					if val, ok = b[filter.Key].(float64); !ok {
						continue
					}
					switch filter.Op {
					case ">":
						if val < filter.Val.(float64) {
							continue
						}
					case "<":
						if val > filter.Val.(float64) {
							continue
						}
					case "==":
						if val != filter.Val.(float64) {
							continue
						}
					case "!=":
						if val == filter.Val.(float64) {
							continue
						}
					case "<=":
						if val >= filter.Val.(float64) {
							continue
						}
					case ">=":
						if val <= filter.Val.(float64) {
							continue
						}
					}
				case string:
					var val string
					var ok bool
					if val, ok = b[filter.Key].(string); !ok {
						continue
					}
					switch filter.Op {
					case ">":
						if strings.ToLower(val) < strings.ToLower(filter.Val.(string)) {
							continue
						}
					case "<":
						if strings.ToLower(val) > strings.ToLower(filter.Val.(string)) {
							continue
						}
					case "==":
						if strings.ToLower(val) != strings.ToLower(filter.Val.(string)) {
							continue
						}
					case "!=":
						if strings.ToLower(val) == strings.ToLower(filter.Val.(string)) {
							continue
						}
					case "LIKE":
						if !strings.HasPrefix(strings.ToLower(val), strings.ToLower(filter.Val.(string))) {
							continue
						}
					}
				}
				filterSlice = append(filterSlice, b)
			}
		}
		bindings = filterSlice
	}

	// if Select is present in options, remove any variables not selected.
	if len(options.Select) > 0 && options.Select[0] != "?COUNT" {
		selectMap := map[string]bool{}
		for _, key := range options.Select {
			selectMap[key] = true
		}

		for _, b := range bindings {
			for key, _ := range b {
				if _, ok := selectMap[key]; !ok {
					delete(b, key)
				}
			}
		}
	}

	// return only distinct items.
	if options.Distinct {
		distinctMap := map[string]bool{}
		distinctSlice := []Bindings{}
		for _, b := range bindings {
			hash := fmt.Sprintf("%s", b)
			if _, ok := distinctMap[hash]; !ok {
				distinctMap[hash] = true
				distinctSlice = append(distinctSlice, b)
			}
		}
		bindings = distinctSlice
	}

	// sort
	if options.OrderBy != "" {
		if options.OrderBy[:1] == "-" {
			sort.Sort(bindingSlice{Key: options.OrderBy[1:], Asc: false, Bindings: bindings})
		} else {
			sort.Sort(bindingSlice{Key: options.OrderBy, Asc: true, Bindings: bindings})
		}
	}

	// limit and offset
	total := len(bindings)
	if options.Limit != 0 || options.Offset != 0 {
		if total > int(options.Limit)+int(options.Offset) {
			if options.Limit > 0 {
				bindings = bindings[options.Offset : options.Offset+options.Limit]
			} else {
				bindings = bindings[options.Offset:]
			}
		}
	}

	return
}

// bindingSlice implements the sort interface
type bindingSlice struct {
	Key      string
	Asc      bool
	Bindings []Bindings
}

// Len is part of sort.Interface.
func (s bindingSlice) Len() int {
	return len(s.Bindings)
}

// Swap is part of sort.Interface.
func (s bindingSlice) Swap(i, j int) {
	s.Bindings[i], s.Bindings[j] = s.Bindings[j], s.Bindings[i]
}

// Less is part of sort.Interface.
func (s bindingSlice) Less(i, j int) bool {
	if l, ok := s.Bindings[i][s.Key].(string); ok {
		if r, ok := s.Bindings[j][s.Key].(string); ok {
			if s.Asc {
				return strings.ToLower(l) < strings.ToLower(r)
			}
			return strings.ToLower(l) > strings.ToLower(r)
		}
	}
	return false
}

// ApplyInference [...]
func (g *Graph) ApplyInference(inf Inference) {
	start := time.Now()
	defer func() { log.Info("Graph.ApplyInference ", time.Since(start)) }()

	inf.Apply(g)
}

// bfs breadth first search a start and end point
func (g *Graph) bfs(startID, endID, predAdj string) (int, []*Adjacent) {
	itemIDs := []*Adjacent{&Adjacent{ID: startID}}

	// Keep track of items found
	visitedIDs := map[string]struct{}{startID: struct{}{}}
	iterations := 0
	for len(itemIDs) > 0 {
		iterations++
		// Get adjacent items for item
		adjIDs := []*Adjacent{}
		for _, parent := range itemIDs {
			triples, err := g.Triples(SPEMPTY, predAdj, parent.ID, nil)
			if err != nil {
				log.Error(err)
				continue
			}
			for _, triple := range triples {
				sub, _, err := SubPred(triple[0], triple[1])
				if err == nil {
					if _, ok := visitedIDs[sub]; !ok {
						visitedIDs[sub] = struct{}{}

						adjIDs = append(adjIDs, &Adjacent{sub, &Adjacent{parent.ID, parent.Next}})
						if sub == endID {
							return iterations, adjIDs
						}
					}
				}
			}
		}

		// Get adjacent items
		nextItemIDs := []*Adjacent{}
		for _, adj := range adjIDs {
			triples, err := g.Triples(adj.ID, predAdj, SPEMPTY, nil)
			if err != nil {
				log.Error(err)
				continue
			}
			for _, triple := range triples {
				itemID, ok := triple[2].(string)
				if !ok {
					continue
				}
				// If not visited mark and check if found or get next items
				if _, ok := visitedIDs[itemID]; !ok {
					visitedIDs[itemID] = struct{}{}
					if itemID == endID {
						return iterations, []*Adjacent{&Adjacent{itemID, &Adjacent{adj.ID, adj.Next}}}
					}
					nextItemIDs = append(nextItemIDs, &Adjacent{itemID, &Adjacent{adj.ID, adj.Next}})
				}
			}
		}
		itemIDs = nextItemIDs
	}
	return iterations, itemIDs
}

// Path finds the shortest path between two points.(ALPHA)
// predName is the identifier, like name
// predAdj is the predicate used, like starring or friends_with
func (g *Graph) Path(start, end, predName, predAdj string) ([]string, error) {
	startT := time.Now()
	defer func() { log.Info("Graph.Path ", time.Since(startT)) }()

	names := []string{}
	s, err := g.Value(SPEMPTY, predName, start)
	if err != nil {
		log.Error(err)
		return names, err
	}
	startID, ok := s.(string)
	if !ok {
		return names, err
	}
	e, err := g.Value(SPEMPTY, predName, end)
	if err != nil {
		log.Error(err)
		return names, err
	}
	endID, ok := e.(string)
	if !ok {
		return names, err
	}
	if startID == SPEMPTY || endID == SPEMPTY {
		return names, fmt.Errorf("start or end empty")
	}
	_, result := g.bfs(startID, endID, predAdj)
	for len(result) > 0 {
		next := result[0].Next
		val, err := g.Value(result[0].ID, predName, SPEMPTY)
		if err != nil {
			log.Error(err)
			continue
		}
		v, ok := val.(string)
		if !ok {
			continue
		}
		names = append(names, v)
		if next == nil {
			break
		}
		result = []*Adjacent{next}
	}

	return names, nil
}

// Load [...]
func (g *Graph) Load(csvFile io.Reader) error {
	startT := time.Now()
	defer func() { log.Info("Graph.Load", time.Since(startT)) }()

	csvReader := csv.NewReader(csvFile)
	csvReader.TrailingComma = true
	for {
		fields, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error(err)
			continue
		}
		if len(fields) != 3 {
			log.Error("Nothing there ", fields)
			continue
		}
		if fields[0] == SPEMPTY || fields[1] == SPEMPTY {
			log.Error("Invalid line ", fields)
			continue
		}
		g.Add(fields[0], fields[1], fields[2])
	}
	return nil
}

// Save [...]
func (g *Graph) Save(csvFile io.Writer) error {
	startT := time.Now()
	defer func() { log.Info("Graph.Save", time.Since(startT)) }()

	csvWriter := csv.NewWriter(csvFile)
	triples, err := g.Triples(SPEMPTY, SPEMPTY, nil, nil)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, triple := range triples {
		sub, pred, err := SubPred(triple[0], triple[1])
		if err == nil {
			switch triple[2].(type) {
			case string:
				csvWriter.Write([]string{sub, pred, fmt.Sprintf("%s", triple[2])})
			case float64, float32:
				csvWriter.Write([]string{sub, pred, fmt.Sprintf("%f", triple[2])})
			case int, uint, uint32, uint64:
				csvWriter.Write([]string{sub, pred, fmt.Sprintf("%d", triple[2])})
			}
		}
	}
	csvWriter.Flush()
	return nil
}
