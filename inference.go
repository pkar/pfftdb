package pfftdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	log "github.com/golang/glog"
)

// Inferences is initialized on load
var Inferences map[string]Inference

// Inference [...]
type Inference interface {
	Apply(*Graph)
	Triples(map[string]interface{}) ([]*Triple, error)
	//Remove()
}

// GoogleGeoResp from the google maps api.
type GoogleGeoResp struct {
	Results []struct {
		AddrComponents []struct {
			LongName  string   `json:"long_name"`
			ShortName string   `json:"short_name"`
			Types     []string `json:"types"`
		} `json:"address_components"`
		FormattedAddress string `json:"formatted_address"`
		Geometry         struct {
			Location struct {
				Lat float64 `json:"lat"`
				Lng float64 `json:"lng"`
			} `json:"location"`
			LocationType string `json:"location_type"`
			ViewPort     interface{}
		} `json:"geometry"`
	} `json:"results"`
	Status string
}

// GeoRule provides latitude and logitude triples
// for a given address. It searches for location:address within triples
// and applies lat and lng triples to that address.
type GeoRule struct {
}

func init() {
	Inferences = map[string]Inference{
		"geo": GeoRule{},
	}
}

// googleCall performs a remote request to google.
var googleCall = func(address string) ([]byte, error) {
	resp, err := http.Get(fmt.Sprintf(`http://maps.googleapis.com/maps/api/geocode/json?address=%s&sensor=false`, url.QueryEscape(address)))
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

	return body, err
}

// Queries
func (gr GeoRule) Apply(g *Graph) {
	type Key struct {
		Place   string
		Address string
	}
	seen := map[Key]struct{}{}
	bindings := []Bindings{}

	triples := []*Triple{
		&Triple{"?placeid", "location:address", "?address"},
	}
	for _, loc := range g.Query(triples, nil) {
		placeID := fmt.Sprintf("%v", loc["placeid"])
		addr := fmt.Sprintf("%v", loc["address"])
		if _, ok := seen[Key{placeID, addr}]; ok {
			continue
		}
		bindings = append(bindings, loc)
		seen[Key{placeID, addr}] = struct{}{}
	}

	for _, bind := range bindings {
		trps, err := gr.Triples(bind)
		if err != nil {
			log.Error(err)
			if err.Error() == "OVER_QUERY_LIMIT" {
				return
			}
		}
		for _, triple := range trps {
			sub, pred, err := SubPred(triple[0], triple[1])
			if err != nil {
				log.Error(err)
				continue
			}
			err = g.Add(sub, pred, triple[2])
			if err != nil {
				log.Error(err)
				if err.Error() == "OVER_QUERY_LIMIT" {
				}
			}
		}
	}
}

// Triples
func (m GeoRule) Triples(args map[string]interface{}) ([]*Triple, error) {
	triples := []*Triple{}

	if _, ok := args["placeid"].(string); ok {
		addr, ok := args["address"].(string)
		if !ok {
			return nil, fmt.Errorf("missing address")
		}
		time.Sleep(500 * time.Millisecond)
		body, err := googleCall(addr)
		if err != nil {
			log.Error(err, body)
			return nil, err
		}

		ggr := GoogleGeoResp{}
		err = json.Unmarshal(body, &ggr)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		if ggr.Status == "OVER_QUERY_LIMIT" {
			log.Errorf("%#v", ggr)
			return nil, fmt.Errorf(ggr.Status)
		}

		if len(ggr.Results) == 0 {
			log.Errorf("%#v", ggr)
			return nil, fmt.Errorf("no results")
		}
		lat := ggr.Results[0].Geometry.Location.Lat
		lng := ggr.Results[0].Geometry.Location.Lng

		triples = append(triples, &Triple{addr, "location:lat", lat})
		triples = append(triples, &Triple{addr, "location:lng", lng})
	}
	return triples, nil
}
