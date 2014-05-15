# Go client

```go
package main

import (
	"log"

	"github.com/pkar/pfftdb/clients/go"
	"github.com/pkar/pfftdb"
)

func main() {
	// Connect
	c, err := client.NewClient("localhost:6666")
	if err != nil {
		log.Fatal(err)
	}
	testGraph := "test"

	// Add
	triples := []pfftdb.Triple{pfftdb.Triple{"a", "b", "c"}}
	err := c.Add(testGraph, triples)
	if err != nil {
		log.Fatal(err)
	}

	// Value
	val, err := cl.Value(TESTGRAPH, "", "b", "c")

	// Remove
	triples = []graph.Triple{graph.Triple{"a", "", ""}}
	err = c.Remove(testGraph, triples)
	if err != nil {
		log.Fatal(err)
	}

	// Triples
	triples2, err := c.Triples(testGraph, "", "", nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", triples2)

  // Count
	count, err := c.Count(TESTGRAPH, "", "", "")

	// Query
	clauses := []*pfftdb.Triple{&pfftdb.Triple{"?name", "b", "c"}}
	bindings, err := c.Query(testGraph, clauses, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", bindings)

	// Inference
	ok, err = c.Inference(testGraph, "geo")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", ok)
}
```
