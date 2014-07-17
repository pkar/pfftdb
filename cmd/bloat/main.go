package main

import (
	"flag"
	"fmt"

	log "github.com/golang/glog"
	"github.com/pkar/pfftdb"
	"github.com/pkar/pfftdb/clients/go"
)

func userTriples(id string) []*pfftdb.Triple {
	triples := []*pfftdb.Triple{
		&pfftdb.Triple{"_:" + id, "fb:id", "_:fb_" + id},
		&pfftdb.Triple{"_:fb_" + id, "fb:first_name", "f" + id},
		&pfftdb.Triple{"_:fb_" + id, "fb:last_name", "l" + id},
		&pfftdb.Triple{"_:fb_" + id, "fb:username", "u" + id},
		&pfftdb.Triple{"_:fb_" + id, "fb:email", "email" + id},
	}
	return triples
}

func main() {
	flag.Parse()
	c, err := client.NewClient("localhost:9666")
	if err != nil {
		log.Fatal(err)
	}

	iter := make([]struct{}, 10000)
	triples := []*pfftdb.Triple{}
	for i := range iter {
		user := userTriples(fmt.Sprintf("%d", i))
		for _, tr := range user {
			triples = append(triples, tr)
		}
	}

	log.Info("adding")
	total, err := c.Add("user", triples)
	log.Info(total, err)

	q := []*pfftdb.Triple{
		&pfftdb.Triple{"?id", "fb:id", "_:fb_4444"},
		&pfftdb.Triple{"_:fb_4444", "fb:first_name", "?fname"},
		&pfftdb.Triple{"_:fb_4444", "fb:last_name", "?lname"},
		&pfftdb.Triple{"_:fb_4444", "fb:email", "?email"},
	}
	d, err := c.Query("user", q, nil)
	log.Info(d, err)
}
