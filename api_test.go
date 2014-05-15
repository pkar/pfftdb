package pfftdb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestPrefixMap(t *testing.T) {
	prefixes := map[string]string{
		"foaf": "http://foaf/",
		"abc":  "http://abc/",
		"cde":  "http://cde/",
	}
	triples := []*Triple{
		&Triple{"foaf:123", "abc:555", "999"},
		&Triple{"cde:123", "555", "999"},
	}
	PrefixMap(prefixes, triples)
	if triples[0][0].(string) != "http://foaf/123" {
		t.Error("did not replace", triples[0])
	}

	if triples[0][1].(string) != "http://abc/555" {
		t.Error("did not replace", triples[0])
	}

	if triples[1][0].(string) != "http://cde/123" {
		t.Error("did not replace", triples[1])
	}
}

func TestPrefixMapTriple(t *testing.T) {
	prefixes := map[string]string{
		"foaf": "http://foaf/",
	}
	sub := "foaf:a"
	pred := "foaf:b"
	var obj interface{}
	obj = "foaf:c"

	sub, pred, obj = PrefixMapTriple(prefixes, sub, pred, obj)

	if sub != "http://foaf/a" {
		t.Error("did not replace sub: ", sub)
	}
	if pred != "http://foaf/b" {
		t.Error("did not replace pred: ", pred)
	}
	if obj != "http://foaf/c" {
		t.Error("did not replace obj: ", obj)
	}
}

func TestPing(t *testing.T) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%s/v1/ping", APIPORT), nil)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.PingHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestGraphsList(t *testing.T) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%s/v1/graphs", APIPORT), nil)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.GraphsListHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	body := w.Body.String()

	graphs := GraphsResponse{}
	err = json.Unmarshal([]byte(body), &graphs)
	if err != nil {
		t.Fatal(err, body)
	}
	graphNames := map[string]bool{}
	for _, name := range graphs.Data {
		graphNames[name] = true
	}
	if _, ok := graphNames["test"]; !ok {
		t.Errorf("didn't get back test graph name: %v", graphNames)
	}
	if _, ok := graphNames["test2"]; !ok {
		t.Errorf("didn't get back test graph name: %v", graphNames)
	}
}

func TestDataHandler(t *testing.T) {
	rec := fmt.Sprintf(`{
		"graph": "%s",
		"data":[
			["a", "foaf:knows", "b"],
			["b", "foaf:knows", "a"]
		],
		"prefix": {
			"foaf": "http://foaf/"
		}
	}`, TESTGRAPH)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/data", APIPORT), strings.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.DataHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}

	g, _ := STORE.Driver.Graph(TESTGRAPH)
	triples, err := g.Triples("", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(triples) != 2 {
		t.Fatal(triples)
	}

	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://localhost:%s/v1/data", APIPORT), strings.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}

	w = httptest.NewRecorder()
	TESTAPI.DataHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}

	triples, err = g.Triples("a", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(triples) != 0 {
		t.Fatal(triples)
	}

	/*
		// invalid triple
		rec = fmt.Sprintf(`{
			"graph": "%s",
			"data":[
				["a", "foaf:knows", "b"],
				["b", "foaf:knows", "a"],
				null
			],
			"prefix": {
				"foaf": "http://foaf/"
			}
		}`, TESTGRAPH)

		req, err = http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/data", APIPORT), strings.NewReader(rec))
		if err != nil {
			t.Fatal(err)
		}

		w = httptest.NewRecorder()
		TESTAPI.DataHandler(w, req)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	*/
}

func TestValueHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("a", "friends_with", "b")

	rec := fmt.Sprintf(`{
		"graph": "%s",
		"sub": "a",
		"pred": "friends_with",
		"obj": null
	}`, TESTGRAPH)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/value", APIPORT), strings.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.ValueHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	body := w.Body.String()

	vals := ValueResponse{}
	err = json.Unmarshal([]byte(body), &vals)
	if err != nil {
		t.Fatal(err)
	}
	if vals.Data != "b" {
		t.Fatalf("didn't get b value got:%v", vals.Data)
	}
}

func TestTriplesHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("a", "friends_with", "b")
	g.Add("b", "friends_with", "a")

	rec := fmt.Sprintf(`{
		"graph": "%s",
		"sub": "",
		"pred": "friends_with",
		"obj": null
	}`, TESTGRAPH)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/triples", APIPORT), strings.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.TriplesHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	body := w.Body.String()

	triples := TriplesResponse{}
	err = json.Unmarshal([]byte(body), &triples)
	if err != nil {
		t.Fatal(err)
	}
	if len(triples.Data) != 2 {
		t.Fatal(triples)
	}
}

func TestTriplesCountHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("a", "friends_with", "b")
	g.Add("b", "friends_with", "a")

	rec := fmt.Sprintf(`{
		"graph": "%s",
		"sub": "",
		"pred": "",
		"obj": null
	}`, TESTGRAPH)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/triples/count", APIPORT), strings.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.TriplesCountHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	body := w.Body.String()

	triples := TriplesCountResponse{}
	err = json.Unmarshal([]byte(body), &triples)
	if err != nil {
		t.Fatal(err)
	}
	if triples.Data != 2 {
		t.Errorf("count should be 2 got: %d", triples.Data)
	}
}

func TestQueryHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("a", "friends_with", "b")
	g.Add("b", "friends_with", "a")

	rec := fmt.Sprintf(`{
		"graph": "%s",
		"data":[
			["?person", "friends_with", "b"]
		]
	}`, TESTGRAPH)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/query", APIPORT), strings.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.QueryHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	bindings := QueryResponse{}
	err = json.Unmarshal([]byte(w.Body.String()), &bindings)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings.Data) != 1 {
		t.Fatal(bindings)
	}
}

func TestQueryCountHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("a", "friends_with", "b")
	g.Add("b", "friends_with", "a")
	g.Add("c", "friends_with", "b")

	rec := fmt.Sprintf(`{
		"graph": "%s",
		"select": ["?COUNT"],
		"data":[
			["?person", "friends_with", "b"]
		]
	}`, TESTGRAPH)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/query", APIPORT), strings.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	TESTAPI.QueryHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	bindings := QueryCountResponse{}
	err = json.Unmarshal([]byte(w.Body.String()), &bindings)
	if err != nil {
		t.Fatal(err)
	}
	if bindings.Data != 2 {
		t.Errorf("count should have been 2 got: %d", bindings.Data)
	}
}

func TestDropHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("a", "b", "c")

	v := url.Values{}
	v.Set("graph", TESTGRAPH)
	v.Add("test", "t")
	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/drop", APIPORT), strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	TESTAPI.DropHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestIndexHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("a", "b", "c")

	v := url.Values{}
	v.Set("graph", TESTGRAPH)
	v.Add("background", "false")
	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/index", APIPORT), strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	TESTAPI.IndexHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestInferenceHandler(t *testing.T) {
	g, _ := STORE.Driver.Graph(TESTGRAPH)
	g.Add("location:barncompany60614", "location:address", "950 W Wrightwood Ave Chicago, IL 60614")

	v := url.Values{}
	v.Set("graph", TESTGRAPH)
	v.Add("inference", "geo")
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:%s/v1/inference", APIPORT), strings.NewReader(v.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	TESTAPI.InferenceHandler(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestPathHandler(t *testing.T) {
	//g, _ := STORE.Driver.Graph(TESTGRAPH)

}
