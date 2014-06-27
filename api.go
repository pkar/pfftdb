package pfftdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	log "github.com/golang/glog"
)

// API ...
type API struct {
	Env    string
	Port   string
	Driver Driver
	WebDir string // only used for demo graph visualization.
}

// GraphsResponse for getting a graph list.
type GraphsResponse struct {
	Data []string `json:"data"`
}

// DataRequest for add and remove.
type DataRequest struct {
	Graph  string            `json:"graph"`
	Prefix map[string]string `json:"prefix"`
	Data   []*Triple         `json:"data"`
}

// DataResponse for add and remove.
type DataResponse struct {
	Graph string `json:"graph"`
	Data  uint   `json:"data"`
}

// TriplesRequest is the json used to query the triples endpoint.
type TriplesRequest struct {
	Graph   string            `json:"graph"`
	Prefix  map[string]string `json:"prefix"`
	Sub     string            `json:"sub"`
	Pred    string            `json:"pred"`
	Obj     interface{}       `json:"obj"`
	Limit   uint              `json:"limit"`
	Offset  uint              `json:"offset"`
	OrderBy string            `json:"orderby"`
}

// TriplesResponse is whats returned from the triples endpoint.
type TriplesResponse struct {
	Graph string    `json:"graph"`
	Data  []*Triple `json:"data"`
}

// TriplesCountResponse returns the number of triples for a request.
type TriplesCountResponse struct {
	Graph string `json:"graph"`
	Data  uint   `json:"data"`
}

// ValueRequest is the json used to get a singular value.
type ValueRequest struct {
	Graph  string            `json:"graph"`
	Prefix map[string]string `json:"prefix"`
	Sub    string            `json:"sub"`
	Pred   string            `json:"pred"`
	Obj    interface{}       `json:"obj"`
}

// ValueResponse is whats returned from the value endpoint.
type ValueResponse struct {
	Graph string      `json:"graph"`
	Data  interface{} `json:"data"`
}

// QueryRequest is whats used for the query endpoint.
type QueryRequest struct {
	Graph    string            `json:"graph"`
	Prefix   map[string]string `json:"prefix"`
	Data     []*Triple         `json:"data"`
	Select   []string          `json:"select"`
	Distinct bool              `json:"distinct"`
	Optional []uint            `json:"optional"`
	Limit    uint              `json:"limit"`
	Offset   uint              `json:"offset"`
	OrderBy  string            `json:"orderby"`
	Filter   []*Filter         `json:"filter"`
}

// QueryResponse is whats returned from the query endpoint.
type QueryResponse struct {
	Graph string     `json:"graph"`
	Data  []Bindings `json:"data"`
}

// QueryCountResponse returns the number of results for a query.
type QueryCountResponse struct {
	Graph   string `json:"graph"`
	Request QueryRequest
	Data    uint `json:"data"`
}

// PathResponse returns a path for given query.
type PathResponse struct {
	Graph  string            `json:"graph"`
	Prefix map[string]string `json:"prefix"`
	Data   []string          `json:"data"`
}

// PrefixMap replaces in place defined prefixes in a set of triples.
func PrefixMap(prefixes map[string]string, triples []*Triple) {
	for prefix, replace := range prefixes {
		for i, triple := range triples {
			if triple != nil {
				for j, item := range triple {
					if itemStr, ok := item.(string); ok {
						if strings.HasPrefix(itemStr, prefix+":") {
							triples[i][j] = strings.Replace(itemStr, prefix+":", replace, 1)
						}
					}
				}
			}
		}
	}
}

// PrefixMapTriple replaces in place defined prefixes in sub, pred and obj
func PrefixMapTriple(prefixes map[string]string, sub, pred string, obj interface{}) (string, string, interface{}) {
	for prefix, replace := range prefixes {
		if strings.HasPrefix(sub, prefix+":") {
			sub = strings.Replace(sub, prefix+":", replace, 1)
		}
		if strings.HasPrefix(pred, prefix+":") {
			pred = strings.Replace(pred, prefix+":", replace, 1)
		}
		if o, ok := obj.(string); ok {
			if strings.HasPrefix(o, prefix+":") {
				obj = strings.Replace(o, prefix+":", replace, 1)
			}
		}
	}
	return sub, pred, obj
}

// Graph retrieves or creates a new graph if not loaded.
func (a *API) Graph(name string) (*Graph, bool) {
	g, ok := a.Driver.Graph(name)
	if ok {
		return g, ok
	}

	// Create graph if it doesn't exist
	var err error
	g, err = a.Driver.Create(name)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	return g, true
}

// PingHandler returns PONG
func (a *API) PingHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprint(w, "PONG")
}

// IndexHandler indexes a graph.
func (a *API) IndexHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "POST" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	name := req.FormValue("graph")
	if name == "" {
		e := badRequest("name not provided")
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	background := req.FormValue("background")
	b, err := strconv.ParseBool(background)
	if err != nil {
		b = true
	}
	err = a.Driver.Index(name, b)
	if err != nil {
		log.Error(err)
		e := internalServerError(err.Error())
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}
}

// DropHandler removes a graph.
func (a *API) DropHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "POST" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	name := req.FormValue("graph")
	if name == "" {
		e := badRequest("graph name required")
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return

	}
	err := a.Driver.Drop(name)
	if err != nil {
		e := internalServerError(err.Error() + " graph:" + name)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "OK")
}

// GraphsListHandler returns a current list of graphs created.
func (a *API) GraphsListHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	data := a.Driver.GraphsList()
	graphsResponse := GraphsResponse{Data: data}
	p, err := json.Marshal(graphsResponse)
	if err != nil {
		e := internalServerError(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, string(p))
}

// DataHandler handles add and remove requests. POST adds and DELETE removes.
// A request looks like, see README.md
// {
//	"graph": "user",
//	"prefix": {
//		"eu": "https://eurisko.io/rdf/0.1/",
//	},
// "data": [
//		["_:1", "rdf:type", "foaf:Person"],
//	]
// }
func (a *API) DataHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	data := DataRequest{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	g, ok := a.Graph(data.Graph)
	if !ok {
		e := badRequest("Bad request, graph not found: " + data.Graph)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	PrefixMap(data.Prefix, data.Data)

	switch req.Method {
	case "POST":
		total, err := g.AddBulk(data.Graph, data.Data)
		if err != nil {
			e := badRequest(err.Error())
			log.Error(e)
			http.Error(w, e["err"].(string), http.StatusBadRequest)
			return
		}
		p, err := json.Marshal(&DataResponse{Graph: data.Graph, Data: uint(total)})
		if err != nil {
			e := internalServerError(err.Error())
			log.Error(e)
			http.Error(w, e["err"].(string), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(p))
		return
	case "DELETE":
		err := g.RemoveBulk(data.Graph, data.Data)
		if err != nil {
			e := badRequest(err.Error())
			log.Error(e)
			http.Error(w, e["err"].(string), http.StatusBadRequest)
			return
		}

		fmt.Fprint(w, "OK")
		return
	}
	e := methodNotAllowed(req.Method)
	log.Error(e)
	http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
}

// ValueHandler returns a singular value given a sub, pred, obj
func (a *API) ValueHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "POST" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		e := badRequest(err.Error())
		log.Error(err)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	data := ValueRequest{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	g, ok := a.Graph(data.Graph)
	if !ok {
		e := badRequest("Graph not found: " + data.Graph)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	sub, pred, obj := PrefixMapTriple(data.Prefix, data.Sub, data.Pred, data.Obj)
	val, err := g.Value(sub, pred, obj)
	if err != nil && err.Error() != "not found" {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	valueResponse := ValueResponse{Graph: data.Graph, Data: val}
	p, err := json.Marshal(valueResponse)
	if err != nil {
		e := internalServerError(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, string(p))
	return
}

// TriplesHandler returns a set of triples based on given query
// which is just an triple array.
func (a *API) TriplesHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "POST" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	data := TriplesRequest{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	g, ok := a.Graph(data.Graph)
	if !ok {
		e := badRequest("Graph not found: " + data.Graph)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	sub, pred, obj := PrefixMapTriple(data.Prefix, data.Sub, data.Pred, data.Obj)
	opts := &Options{Limit: data.Limit, Offset: data.Offset, OrderBy: data.OrderBy}
	triples, err := g.Triples(sub, pred, obj, opts)
	if err != nil {
		e := internalServerError(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}

	triplesResponse := TriplesResponse{}
	for _, triple := range triples {
		triplesResponse.Data = append(triplesResponse.Data, triple)
	}
	triplesResponse.Graph = data.Graph
	p, err := json.Marshal(triplesResponse)
	if err != nil {
		e := internalServerError(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, string(p))
	return
}

// TriplesCountHandler returns the number of triples for a query.
func (a *API) TriplesCountHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "POST" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	data := TriplesRequest{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	g, ok := a.Graph(data.Graph)
	if !ok {
		e := badRequest("Graph not found: " + data.Graph)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	sub, pred, obj := PrefixMapTriple(data.Prefix, data.Sub, data.Pred, data.Obj)
	count, err := g.Count(sub, pred, obj)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	countResponse := TriplesCountResponse{Graph: data.Graph, Data: count}
	p, err := json.Marshal(countResponse)
	if err != nil {
		e := internalServerError(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, string(p))
	return
}

// QueryHandler returns an array of bound variables.
func (a *API) QueryHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "POST" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	data := QueryRequest{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		e := badRequest(err.Error() + string(body))
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	g, ok := a.Graph(data.Graph)
	if !ok {
		e := badRequest("Graph not found: " + data.Graph)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	PrefixMap(data.Prefix, data.Data)

	opts := &Options{
		Select:   data.Select,
		Optional: data.Optional,
		Limit:    data.Limit,
		Offset:   data.Offset,
		OrderBy:  data.OrderBy,
		Filter:   data.Filter,
		Distinct: data.Distinct,
	}
	bindings := g.Query(data.Data, opts)
	queryResponse := QueryResponse{Graph: data.Graph, Data: bindings}

	// return count if requested
	if len(data.Select) > 0 && data.Select[0] == "?COUNT" {
		queryResponse := QueryCountResponse{
			Graph:   data.Graph,
			Request: data,
			Data:    uint(len(bindings)),
		}
		p, err := json.Marshal(queryResponse)
		if err != nil {
			e := internalServerError(err.Error())
			log.Error(e)
			http.Error(w, e["err"].(string), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(p))
		return
	}

	p, err := json.Marshal(queryResponse)
	if err != nil {
		e := internalServerError(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, string(p))
	return
}

// InferenceHandler ...
func (a *API) InferenceHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "PUT" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	inferenceVar := req.FormValue("inference")
	graphName := req.FormValue("graph")
	inf, ok := Inferences[inferenceVar]
	if !ok {
		e := badRequest("Inference not found: " + inferenceVar)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	g, ok := a.Graph(graphName)
	if !ok {
		e := badRequest("Graph not found: " + graphName)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	g.ApplyInference(inf)
	fmt.Fprint(w, "OK")
}

// PathHandler returns path from a start to end.
func (a *API) PathHandler(w http.ResponseWriter, req *http.Request) {
	if req.Body == nil {
		http.Error(w, "no request body", http.StatusBadRequest)
		return
	}

	if req.Method != "GET" {
		e := methodNotAllowed(req.Method)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusMethodNotAllowed)
		return
	}

	start := req.FormValue("start")
	end := req.FormValue("end")
	predName := req.FormValue("predicateName")
	predAdj := req.FormValue("predicateAdjacent")
	graphName := req.FormValue("graph")
	g, ok := a.Graph(graphName)
	if !ok {
		e := badRequest("Graph not found: " + graphName)
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}

	result, err := g.Path(start, end, predName, predAdj)
	if err != nil {
		e := badRequest(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	resp := PathResponse{Graph: graphName, Data: result}
	p, err := json.Marshal(resp)
	if err != nil {
		e := internalServerError(err.Error())
		log.Error(e)
		http.Error(w, e["err"].(string), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, string(p))
}

// Run starts up a server and endpoints. It serves
// files from the web directory.
func (a *API) Run() {
	http.HandleFunc("/v1/ping", a.PingHandler)
	http.HandleFunc("/v1/graphs", a.GraphsListHandler)
	http.HandleFunc("/v1/data", a.DataHandler)
	http.HandleFunc("/v1/triples", a.TriplesHandler)
	http.HandleFunc("/v1/triples/count", a.TriplesCountHandler)
	http.HandleFunc("/v1/value", a.ValueHandler)
	http.HandleFunc("/v1/query", a.QueryHandler)
	http.HandleFunc("/v1/index", a.IndexHandler)
	http.HandleFunc("/v1/drop", a.DropHandler)
	http.HandleFunc("/v1/path", a.PathHandler)
	http.HandleFunc("/v1/inference", a.InferenceHandler)
	// graph viz
	if a.WebDir != "" {
		http.Handle("/", http.FileServer(http.Dir(a.WebDir)))
	}
	err := http.ListenAndServe(":"+a.Port, nil)
	if err != nil {
		log.Fatal(err, a)
	}
}

// Close noop
func (a *API) Close() {

}

// NewAPI creates an api server, it runs with a.Run() in a separate goroutine.
func NewAPI(port, env, webDir string, driver Driver) (*API, error) {
	a := &API{
		Env:    env,
		Port:   port,
		Driver: driver,
		WebDir: webDir,
	}

	go a.Run()
	return a, nil
}
