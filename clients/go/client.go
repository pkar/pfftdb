package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/pkar/pfftdb"
)

// maxRetries
var maxRetries = 3

// Prefix rdf prefix
var Prefix = map[string]string{
	"foaf":     "http://xmlns.com/foaf/0.1/",
	"dc":       "http://purl.org/dc/elements/1.1/",
	"rdf":      "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	"rdfs":     "http://www.w3.org/2000/01/rdf-schema#",
	"owl":      "http://www.w3.org/2002/07/owl#",
	"geonames": "http://www.geonames.org/ontology#",
	"geo":      "http://www.w3.org/2003/01/geo/wgs84_pos#",
	"skos":     "http://www.w3.org/2004/02/skos/core#",
	"rss":      "http://purl.org/rss/1.0/",
	"dbpedia":  "http://dbpedia.org/resource/",
	"eurisko":  "http://eurisko.io/rdf/0.1/eurisko/",
	"location": "http://eurisko.io.com/rdf/0.1/location/",
	"fb":       "http://eurisko.io.com/rdf/0.1/facebook/",
	"dena":     "http://eurisko.io.com/rdf/0.1/dena/",
	"mobage":   "http://eurisko.io.com/rdf/0.1/mobage/",
}

// Connection holds the urls and http client for making calls.
type Connection struct {
	URLS     map[string]string
	HostPort string
	Client   *http.Client
}

// Client
type Client struct {
	Pool         []*Connection
	Total        int
	CurrentIndex int
	mu           *sync.Mutex
}

// NewClient
func NewClient(hostPorts string) (*Client, error) {
	hosts := strings.Split(hostPorts, ",")
	pool := []*Connection{}
	for _, hostPort := range hosts {
		if hostPort == "" {
			continue
		}
		urls := map[string]string{
			"ping":      "http://" + hostPort + "/v1/ping",
			"add":       "http://" + hostPort + "/v1/data",
			"remove":    "http://" + hostPort + "/v1/data",
			"value":     "http://" + hostPort + "/v1/value",
			"triples":   "http://" + hostPort + "/v1/triples",
			"count":     "http://" + hostPort + "/v1/triples/count",
			"query":     "http://" + hostPort + "/v1/query",
			"drop":      "http://" + hostPort + "/v1/drop",
			"index":     "http://" + hostPort + "/v1/index",
			"path":      "http://" + hostPort + "/v1/path",
			"inference": "http://" + hostPort + "/v1/inference",
		}
		conn := &Connection{URLS: urls, HostPort: hostPort, Client: &http.Client{}}
		pool = append(pool, conn)
	}
	total := len(pool)

	if total == 0 {
		return nil, fmt.Errorf("no hosts given")
	}

	c := &Client{Pool: pool, Total: total, mu: &sync.Mutex{}}
	return c, nil
}

// Next just wraps around the available connections
func (c *Client) Next() *Connection {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn := c.Pool[c.CurrentIndex]
	c.CurrentIndex++
	if c.CurrentIndex == c.Total {
		c.CurrentIndex = 0
	}

	return conn
}

// Do performs a request and retries maxRetries
func (c *Client) Do(r *http.Request, conn *Connection) (*http.Response, error) {
	retries := 0
	var resp *http.Response
	var err error
	for retries < maxRetries {
		resp, err = conn.Client.Do(r)
		if err == nil {
			break
		}
		retries++
		log.Errorf("retry: %d err: %v", retries, err)
		time.Sleep(200 * time.Millisecond)
		conn = c.Next()
	}

	return resp, err
}

// Add adds triples to the given graph
func (c *Client) Add(grph string, triples []*pfftdb.Triple) (uint, error) {
	start := time.Now()
	defer func() { log.Info("Client.Add ", time.Since(start)) }()

	conn := c.Next()

	req := pfftdb.DataRequest{
		Graph:  grph,
		Data:   triples,
		Prefix: Prefix,
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Error(err)
		return 0, err
	}

	r, err := http.NewRequest("POST", conn.URLS["add"], strings.NewReader(string(b)))
	if err != nil {
		log.Error(err)
		return 0, err
	}

	resp, err := c.Do(r, conn)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("%s", body)
	}

	data := pfftdb.DataResponse{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Errorf("%#v", err)
		return 0, err
	}
	return data.Data, nil
}

// Drop drops a graph and its indexes.
func (c *Client) Drop(grph string) error {
	start := time.Now()
	defer func() { log.Info("Client.Drop ", time.Since(start)) }()

	conn := c.Next()

	v := url.Values{}
	v.Set("graph", grph)

	r, err := http.NewRequest("POST", conn.URLS["drop"], strings.NewReader(string(v.Encode())))
	if err != nil {
		log.Error(err)
		return err
	}
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")

	resp, err := c.Do(r, conn)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("%s", body)
	}

	return nil
}

// Index indexes a graph
func (c *Client) Index(grph string, background bool) error {
	start := time.Now()
	defer func() { log.Info("Client.Index ", time.Since(start)) }()

	conn := c.Next()
	if grph == "" {
		log.Error("graph name not provided")
		return fmt.Errorf("graph name not provided")
	}

	v := url.Values{}
	v.Set("graph", grph)
	if background {
		v.Add("background", "true")
	} else {
		v.Add("background", "false")
	}

	r, err := http.NewRequest("POST", conn.URLS["index"], strings.NewReader(string(v.Encode())))
	if err != nil {
		log.Error(err)
		return err
	}
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")

	resp, err := c.Do(r, conn)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s", body)
	}

	return nil
}

// Remove removes triples from the given graph
func (c *Client) Remove(grph string, triples []*pfftdb.Triple) error {
	start := time.Now()
	defer func() { log.Info("Client.Remove ", time.Since(start)) }()

	conn := c.Next()

	req := pfftdb.DataRequest{
		Graph:  grph,
		Data:   triples,
		Prefix: Prefix,
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Error(err)
		return err
	}

	r, err := http.NewRequest("DELETE", conn.URLS["remove"], strings.NewReader(string(b)))
	if err != nil {
		log.Error(err)
		return err
	}

	resp, err := c.Do(r, conn)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s", body)
	}
	return nil
}

// Get a unique value
func (c *Client) Value(grph string, sub, pred string, obj interface{}) (interface{}, error) {
	start := time.Now()
	defer func() { log.Info("Client.Value ", time.Since(start)) }()

	conn := c.Next()

	req := pfftdb.ValueRequest{
		Graph:  grph,
		Prefix: Prefix,
		Sub:    sub,
		Pred:   pred,
		Obj:    obj,
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	r, err := http.NewRequest("POST", conn.URLS["value"], strings.NewReader(string(b)))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	resp, err := c.Do(r, conn)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s", string(body))
	}

	data := pfftdb.ValueResponse{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Errorf("%#v", err)
		return nil, err
	}
	return data.Data, nil
}

// Triples [...]
func (c *Client) Triples(grph, sub, pred string, obj interface{}, options *pfftdb.Options) ([]*pfftdb.Triple, error) {
	start := time.Now()
	defer func() { log.Info("Client.Triples ", time.Since(start)) }()

	conn := c.Next()

	req := pfftdb.TriplesRequest{
		Graph:  grph,
		Sub:    sub,
		Pred:   pred,
		Obj:    obj,
		Prefix: Prefix,
	}
	if options != nil {
		req.Limit = options.Limit
		req.Offset = options.Offset
		req.OrderBy = options.OrderBy
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	r, err := http.NewRequest("POST", conn.URLS["triples"], strings.NewReader(string(b)))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	resp, err := c.Do(r, conn)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s", string(body))
	}

	data := pfftdb.TriplesResponse{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return data.Data, nil
}

// Count [...]
func (c *Client) Count(grph, sub, pred string, obj interface{}) (uint, error) {
	start := time.Now()
	defer func() { log.Info("Client.Count ", time.Since(start)) }()

	conn := c.Next()

	req := pfftdb.TriplesRequest{
		Graph: grph,
		Sub:   sub,
		Pred:  pred,
		Obj:   obj,
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Error(err)
		return 0, err
	}

	r, err := http.NewRequest("POST", conn.URLS["count"], strings.NewReader(string(b)))
	if err != nil {
		log.Error(err)
		return 0, err
	}

	resp, err := c.Do(r, conn)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("%s", string(body))
	}

	data := pfftdb.TriplesCountResponse{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Error(err)
		return 0, err
	}
	return data.Data, nil
}

// Query[...]
func (c *Client) Query(grph string, clauses []*pfftdb.Triple, options *pfftdb.Options) ([]pfftdb.Bindings, error) {
	start := time.Now()
	defer func() { log.Info("Client.Query ", time.Since(start)) }()

	conn := c.Next()

	req := pfftdb.QueryRequest{
		Graph:  grph,
		Data:   clauses,
		Prefix: Prefix,
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	r, err := http.NewRequest("POST", conn.URLS["query"], strings.NewReader(string(b)))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	resp, err := c.Do(r, conn)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s", string(body))
	}

	data := pfftdb.QueryResponse{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return data.Data, nil
}

// Inference [...]
func (c *Client) Inference(grph, name string) error {
	start := time.Now()
	defer func() { log.Info("Client.Inference ", time.Since(start)) }()

	conn := c.Next()

	v := url.Values{}
	v.Set("graph", grph)
	v.Add("inference", name)

	req, err := http.NewRequest("PUT", conn.URLS["inference"], strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := conn.Client.Do(req)
	if err != nil {
		log.Error(err)
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s", body)
	}

	return nil
}

// Path [...]
func (c *Client) Path(grph, start, end, predName, predAdj string) ([]string, error) {
	startT := time.Now()
	defer func() { log.Info("Client.Path ", time.Since(startT)) }()

	conn := c.Next()

	v := url.Values{}
	v.Set("graph", grph)
	v.Set("start", start)
	v.Set("end", end)
	v.Set("predicateName", predName)
	v.Set("predicateAdjacent", predAdj)

	req, err := http.NewRequest("GET", conn.URLS["path"], strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := conn.Client.Do(req)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s", body)
	}

	data := pfftdb.PathResponse{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return data.Data, nil
}
