package pfftdb

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

var (
	nTriples int = 0
)

var testCSV = `
/en/apollo_13,name,Apollo 13
/en/apollo_13,starring,/en/kevin_bacon
/en/apollo_13,starring,/en/tom_hanks

/en/a_few_good_men,name,A Few Good Men
/en/a_few_good_men,starring,/en/tom_cruise
/en/a_few_good_men,starring,/en/kevin_bacon
/en/a_few_good_men,directed_by,/en/rob_reiner

/en/top_gun,name,Top Gun
/en/top_gun,starring,/en/tom_cruise
/en/top_gun,starring,/en/val_kilmer

/en/rob_reiner,name,Rob Reiner
/en/tom_hanks,name,Tom Hanks
/en/kevin_bacon,name,Kevin Bacon
/en/val_kilmer,name,Val Kilmer
/en/tom_cruise,name,Tom Cruise

/en/paul,is_not,human
/en/paul,has,hands
/en/paul,name,Pavlos
/en/paul,likes,/en/scotch
/en/paul,likes,/en/scooters
/en/paul,loves,/en/scotch
/en/paul,lives_in,/en/san_francisco
/en/paul,location:address,"666 8th Ave, San Francisco, CA"
/en/paul,friends_with,/en/larry
/en/paul,friends_with,/en/fuod
/en/fuod,friends_with,/en/paul
/en/fuod,friends_with,/en/winona
/en/winona,friends_with,/en/fuod
/rel1,start,2001
/rel1,end,2003
/rel1,with,/en/paul
/rel1,with,/en/fuod
/rel2,start,2005
/rel2,end,2012
/rel2,with,/en/paul
/rel2,with,/en/larry
/en/larry,name,Larry Schmiegal
/en/larry,likes,/en/beer
/en/larry,hates,/en/scotch
/en/larry,lives_in,/en/los_angeles
/en/larry,location:address,"1535 Vine Street, Los Angeles, CA 90028"
/en/fuod,name,Fuod Ramseys
/en/fuod,lives_in,/en/chicago
/en/fuod,likes,/en/scotch
/en/winona,name,Winona Winone
/en/winona,lives_in,san_francisco
/en/winona,location:address,"661 8th Ave, San Francisco, CA"

/en/scotch,is,good
/en/beer,is,carbonated

/rehab1,name,Betty Ford
/rehab1,start,2005
/rehab1,end,2006
/rehab1,person,/en/paul
/rehab1,person,/en/larry

/en/asia,name,Asia
/en/china,name,China
/en/china,inside,/en/asia

/en/europe,name,Europe
/en/greece,name,Greece
/en/greece,inside,Europe
/en/greece,population,11000000

/en/athens,name,Athens
/en/athens,inside,/en/greece
/en/athens,population,3370000

/en/usa,name,United States of America
/en/democratic,name,Democratic Party
/en/republican,name,Republican Party

/en/california,inside,/en/usa
/en/california,name,California
/en/illinois,inside,/en/usa
/en/illinois,name,Illinois

/en/san_francisco,inside,/en/california
/en/san_francisco,name,San Francisco
/en/san_francisco,mayor,/en/ed_lee
/en/ed_lee,name,Ed Lee
/en/san_fancisco,population,825000

/en/att_ballpark,inside,/en/san_francisco
/en/att_ballpark,name,AT&T Park
/en/att_ballpark,location:address,"24 Willie Mays Plaza, San Francisco, CA 94107"
/en/att_ballpark,is_a,tourist_attraction

/en/jehovas,inside,/en/san_francisco
/en/jehovas,name,Jehova's Witnesses
/en/jehovas,location:address,"2500 Fulton Street, San Francisco, CA 94118"
/en/jehovas,is_a,tourist_attraction

/en/katias_russian_tea_room,inside,/en/san_francisco
/en/katias_russian_tea_room,name,Katia's Russian Tea Room
/en/katias_russian_tea_room,location:address,"600 5th Avenue, San Francisco, CA 94118"
/en/katias_russian_tea_room,is_a,restaurant
/en/katias_russian_tea_room,cost,expensive

/en/doodoos,inside,/en/san_francisco
/en/doodoos,name,Doo Doos Hot Dogs
/en/doodoos,location:address,"600 7th Avenue, San Francisco, CA 94118"
/en/doodoos,is_a,restaurant
/en/doodoos,cost,cheap

/en/mcdernals,inside,/en/san_francisco
/en/mcdernals,name,Mcdernals Fish House
/en/mcdernals,location:address,"600 6th Avenue, San Francisco, CA 94118"
/en/mcdernals,is_a,restaurant
/en/mcdernals,cost,cheap

/en/taco_hut,inside,/en/san_francisco
/en/taco_hut,name,Fuods Taco and Pizza Shop With Free Pudding
/en/taco_hut,location:address,"600 4th Avenue, San Francisco, CA 94118"
/en/taco_hut,is_a,restaurant
/en/taco_hut,cost,midrange

/en/los_angeles,inside,/en/california
/en/los_angeles,name,Los Angeles
/en/los_angeles,mayor,/en/eric_garcetti
/en/los_angeles,population,3800000
/en/eric_garcetti,name,Eric Garcetti
/en/eric_garcetti,born,1971
/en/eric_garcetti,party,/en/democratic

/en/san_diego,inside,/en/california
/en/san_diego,name,San Diego
/en/san_diego,population,1300000

murder_rate,name,Murder Rate
murder_rate,per_capita,100000

/en/chicago,inside,/en/illinois
/en/chicago,name,Chicago
/en/chicago,population,2700000
/en/chicago,mayor,Al Capone's ghost
/en/chicago,murder_rate,9.72
`

var testGoogleGeo = `
{ "results" : [ { "address_components" : [ { "long_name" : "666", "short_name" : "666", "types" : [ "street_number" ] }, { "long_name" : "7th Avenue", "short_name" : "7th Ave", "types" : [ "route" ] }, { "long_name" : "Inner Richmond", "short_name" : "Inner Richmond", "types" : [ "neighborhood", "political" ] }, { "long_name" : "San Francisco", "short_name" : "SF", "types" : [ "locality", "political" ] }, { "long_name" : "San Francisco", "short_name" : "San Francisco", "types" : [ "administrative_area_level_2", "political" ] }, { "long_name" : "California", "short_name" : "CA", "types" : [ "administrative_area_level_1", "political" ] }, { "long_name" : "United States", "short_name" : "US", "types" : [ "country", "political" ] }, { "long_name" : "94118", "short_name" : "94118", "types" : [ "postal_code" ] } ], "formatted_address" : "666 7th Avenue, San Francisco, CA 94118, USA", "geometry" : { "bounds" : { "northeast" : { "lat" : 37.7758944, "lng" : -122.4649686 }, "southwest" : { "lat" : 37.7758935, "lng" : -122.464987 } }, "location" : { "lat" : 37.7758944, "lng" : -122.4649686 }, "location_type" : "RANGE_INTERPOLATED", "viewport" : { "northeast" : { "lat" : 37.77724293029149, "lng" : -122.4636288197085 }, "southwest" : { "lat" : 37.7745449697085, "lng" : -122.4663267802915 } } }, "types" : [ "street_address" ] } ], "status" : "OK" }
`

func init() {
	csvLines := strings.Split(testCSV, "\n")
	for _, c := range csvLines {
		if c != "" {
			nTriples++
		}
	}
}

func cleanupGraph() {
	STORE.Driver.RemoveAll(TESTGRAPH)
}

func TestBindingChunks(t *testing.T) {
	b := []Bindings{}
	for _, _ = range make([]struct{}, 1000) {
		b = append(b, Bindings{})
	}

	var btests = []struct {
		inb []Bindings
		inn int
		out int
	}{
		{nil, 10, 0},
		{nil, 1, 0},
		{b[0:1], 10, 1},
		{b[0:2], 10, 1},
		{b[0:3], 10, 1},
		{b[0:8], 10, 1},
		{b[0:9], 10, 1},
		{b[0:11], 10, 2},
		{b[0:13], 10, 2},
		{b[0:13], 5, 3},
		{b[0:900], 100, 9},
		{b[0:907], 100, 10},
		{b, 100, 10},
	}
	for i, tt := range btests {
		out := bindingChunks(tt.inb, tt.inn)
		if len(out) != tt.out {
			t.Error(fmt.Sprintf("%d didn't create %d chunks got:", i, tt.out), len(out))
		}
	}
}

func TestNewGraph(t *testing.T) {
	_, err := NewGraph(TESTGRAPH, STORE.Driver)
	if err != nil {
		t.Fatal(err)
	}
	cleanupGraph()
}

func TestAdd(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	GRPH.Add("paul", "is_not", "human")
	GRPH.Add("paul", "is_not", 123)
	GRPH.Add("paul", "", 123) // invalid try

	triples, err := GRPH.Triples("", "", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if len(triples) != 2 {
		t.Error("len triples should be 2 got: ", triples)
	}
}

func TestRemove(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	GRPH.Add("paul", "is_not", "human")
	GRPH.Add("paul", "is_not", "person")
	GRPH.Add("paul", "foaf", 123)
	GRPH.Add("paul", "foaf", "abc")
	GRPH.Add("berta", "foaf", "abc")
	GRPH.Add("berta", "dd", 1.0)

	GRPH.Remove("paul", "is_not", "human")

	triples, err := GRPH.Triples("", "", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if len(triples) != 5 {
		t.Error(triples)
	}

	GRPH.Remove("paul", "is_not", "person")

	triples, err = GRPH.Triples("", "", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if len(triples) != 4 {
		t.Error(triples)
	}

	GRPH.Remove("paul", "", nil)

	triples, err = GRPH.Triples("", "", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if len(triples) != 2 {
		t.Error(triples)
	}

	GRPH.Remove("", "", nil)

	triples, err = GRPH.Triples("", "", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if len(triples) != 0 {
		t.Error(triples)
	}
}

func TestCount(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	GRPH.Add("paul", "is_not", "human")
	GRPH.Add("paul", "has", "hands")
	GRPH.Add("paul", "likes", "scotch")

	c, err := GRPH.Count("", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if c != 3 {
		t.Errorf("should have gotten 3 got : %d", c)
	}
}

func TestTriples(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)
	GRPH.Load(csvFile)

	var tests = []struct {
		s, p, o string
		out     int
	}{
		// nil nil nil
		{SPEMPTY, SPEMPTY, SPEMPTY, nTriples},

		// sub nil nil
		{"/en/paul", SPEMPTY, SPEMPTY, 10},
		{"/en/beer", SPEMPTY, SPEMPTY, 1},
		{"likes", SPEMPTY, SPEMPTY, 0},

		// sub pred nil
		{"/en/paul", "likes", SPEMPTY, 2},

		// sub nil obj
		{"/en/paul", SPEMPTY, "/en/scotch", 2},

		// sub pred obj
		{"/en/larry", "likes", "/en/beer", 1},

		// nil pred nil
		{SPEMPTY, "likes", SPEMPTY, 4},

		// nil pred obj
		{SPEMPTY, "likes", "/en/scotch", 2},

		// nil nil obj
		{SPEMPTY, SPEMPTY, "/en/scotch", 4},
	}

	for i, test := range tests {
		triples, err := GRPH.Triples(test.s, test.p, test.o, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(triples) != test.out {
			t.Errorf("#%d   %s-%s-%s   out: should be %d got %d", i, test.s, test.p, test.o, test.out, len(triples))
		}
	}
}

func TestMergeGraphs(t *testing.T) {
	defer cleanupGraph()

	GRPH.Add("paul", "is_not", "human")
	GRPH.Add("paul", "has", "hands")
	GRPH.Add("paul", "likes", "scotch")

	//defer STORE.Driver.RemoveAll(TESTGRAPH2)
	GRPH2.Merge(GRPH)
	triples, err := GRPH2.Triples(SPEMPTY, SPEMPTY, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(triples) != 3 {
		t.Error("Merge failed should have 3 got:", len(triples))
	}
}

func TestValue(t *testing.T) {
	defer cleanupGraph()

	GRPH.Add("paul", "is_not", "human")
	GRPH.Add("paul", "has", "hands")

	val, err := GRPH.Value("paul", "is_not", "")
	if err != nil {
		t.Error(err)
	}
	if val != "human" {
		t.Error("human not returned")
	}

	val, err = GRPH.Value("", "is_not", "human")
	if err != nil {
		t.Error(err)
	}
	if val != "paul" {
		t.Error("paul not returned got ", val)
	}
}

func TestQuery(t *testing.T) {
	defer cleanupGraph()

	GRPH.Add("paul", "is_not", "human")
	GRPH.Add("paul", "likes", "turtles")
	GRPH.Add("paul", "has", "hands")
	GRPH.Add("winona", "likes", "turtles")
	GRPH.Add("winona", "likes", "chickens")
	GRPH.Add("winona", "likes", 1.0)
	GRPH.Add("winona", "likes", 1)

	res := GRPH.Query([]*Triple{
		&Triple{"?person", "likes", "turtles"},
		&Triple{"?person", "likes", "?thing"},
	}, nil)
	if len(res) != 4 {
		t.Error(res)
	}
	if _, ok := res[0]["person"]; !ok {
		t.Error(res)
	}
	if _, ok := res[0]["thing"]; !ok {
		t.Error(res)
	}

	csvFile := strings.NewReader(testCSV)
	GRPH.Load(csvFile)

	res = GRPH.Query([]*Triple{}, nil)
	if len(res) != 0 {
		t.Error("Should be 0 got ", len(res), res)
	}

	// make sure query chain stops if no results
	res = GRPH.Query([]*Triple{
		&Triple{"/en/aul", "?pred", "?val"},
		&Triple{"/en/paul", "?pred", "?val"},
	}, nil)
	if len(res) != 0 {
		t.Error("Should be 0 got ", len(res), res)
	}

	res = GRPH.Query([]*Triple{
		&Triple{"/en/paul", "?pred", "?val"},
	}, nil)
	if len(res) != 10 {
		t.Error("Should be 10 got ", len(res), res)
	}

	// filter TODO
	GRPH.Add("ff", "testfilter", 3)
	GRPH.Add("ff", "testfilter", 4)
	GRPH.Add("ff", "testfilter", 5)
	GRPH.Add("ff", "testfilter", 6.0)
	GRPH.Add("ff", "testfilter", 7.0)
	GRPH.Add("ff", "testfilter", 8.0)
	GRPH.Add("ff", "testfilter", "abc")
	GRPH.Add("ff", "testfilter", "def")
	GRPH.Add("ff", "testfilter", "ghi")
	res = GRPH.Query([]*Triple{
		&Triple{"?id", "testfilter", "?val"},
	}, &Options{Filter: []*Filter{&Filter{"val", ">", 4}}})
	if len(res) != 2 {
		t.Error("Should be 2 got ", len(res))
	}
	res = GRPH.Query([]*Triple{
		&Triple{"?id", "testfilter", "?val"},
	}, &Options{Filter: []*Filter{&Filter{"val", ">", 5}}})
	if len(res) != 1 {
		t.Error("Should be 1 got ", len(res))
	}
	res = GRPH.Query([]*Triple{
		&Triple{"?id", "testfilter", "?val"},
	}, &Options{Filter: []*Filter{&Filter{"val", ">", 7.0}}})
	if len(res) != 2 {
		t.Error("Should be 2 got ", len(res))
	}
	res = GRPH.Query([]*Triple{
		&Triple{"?id", "testfilter", "?val"},
	}, &Options{Filter: []*Filter{&Filter{"val", ">", 8.0}}})
	if len(res) != 1 {
		t.Error("Should be 1 got ", len(res))
	}
	res = GRPH.Query([]*Triple{
		&Triple{"?id", "testfilter", "?val"},
	}, &Options{Filter: []*Filter{&Filter{"val", ">", "b"}}})
	if len(res) != 2 {
		t.Error("Should be 2 got ", len(res))
	}
	res = GRPH.Query([]*Triple{
		&Triple{"?id", "testfilter", "?val"},
	}, &Options{Filter: []*Filter{&Filter{"val", "LIKE", "de"}}})
	if len(res) != 1 {
		t.Error("Should be 1 got ", len(res))
	}

	// distinct TODO

	// sort TODO
	res = GRPH.Query([]*Triple{
		&Triple{"/en/paul", "?pred", "?val"},
	}, &Options{OrderBy: "-val"})
	/*
		for _, r := range res {
			t.Log(r)
		}
	*/
	res = GRPH.Query([]*Triple{
		&Triple{"/en/paul", "?pred", "?val"},
	}, &Options{OrderBy: "val"})
	/*
		for _, r := range res {
			t.Log(r)
		}
	*/

	// select
	res = GRPH.Query([]*Triple{
		&Triple{"/en/paul", "?pred", "?val"},
		&Triple{"?val", "?pred2", "?val2"},
	}, &Options{Select: []string{"val"}})
	for _, b := range res {
		if _, ok := b["pred"]; ok {
			t.Error("pred should not be selected")
		}
		if _, ok := b["pred2"]; ok {
			t.Error("pred2 should not be selected")
		}
		if _, ok := b["val2"]; ok {
			t.Error("val2 should not be selected")
		}
	}

	// optional
	res = GRPH.Query([]*Triple{
		&Triple{"/en/paul", "name", "?val"},
		&Triple{"/en/paul", "fake", "?fake"},
		&Triple{"/en/paul", "location:address", "?addr"},
	}, nil)
	if len(res) != 0 {
		t.Error("should have gotten no results")
	}

	res = GRPH.Query([]*Triple{
		&Triple{"/en/paul", "name", "?name"},
		&Triple{"/en/paul", "fake", "?fake"},
		&Triple{"/en/paul", "location:address", "?addr"},
	}, &Options{Optional: []uint{1}})
	if len(res) == 0 {
		t.Error("should have gotten results")
	}
	if _, ok := res[0]["name"]; !ok {
		t.Error("should have gotten a name")
	}
	if _, ok := res[0]["addr"]; !ok {
		t.Error("should have gotten an address")
	}
}

func TestQueryLarge(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	triples := []*Triple{}
	for i, _ := range make([]struct{}, 10000) {
		x := fmt.Sprintf("%d", i)
		y := fmt.Sprintf("%d", i+1)
		z := fmt.Sprintf("%d", i+2)
		triples = append(triples, &Triple{x, y, z})
	}
	GRPH.AddBulk(TESTGRAPH, triples)

	res := GRPH.Query([]*Triple{
		&Triple{"?x", "?y", "?z"},
		&Triple{"?x", "100", "101"},
	}, nil)
	t.Log(len(res))
	/*
		for _, r := range res {
			t.Error(r)
		}
	*/
}

func TestQueryRace(t *testing.T) {
	cleanupGraph()
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)
	GRPH.Load(csvFile)

	GRPH.Add("_:mobageid1", "mobage:name", "sparkles")
	GRPH.Add("_:mobageid1", "dena:deviceid", "_:deviceid1")
	GRPH.Add("_:mobageid1", "dena:deviceid", "_:deviceid2")

	res := GRPH.Query([]*Triple{
		&Triple{"?userid", "dena:deviceid", "_:deviceid1"},
	}, nil)
	if len(res) != 1 {
		t.Error("didn't get back userid got: ", res)
	}

	GRPH.Add("_:facebookid1", "foaf:name", "Charlie Sparklepants")
	GRPH.Add("_:facebookid1", "dena:deviceid", "_:deviceid1")
	GRPH.Add("_:facebookid1", "dena:deviceid", "_:deviceid2")
	res = GRPH.Query([]*Triple{
		&Triple{"?accountid", "dena:deviceid", "_:deviceid1"},
	}, nil)
	if len(res) != 2 {
		t.Error("didn't get back 2 userid got: ", res)
	}
	res = GRPH.Query([]*Triple{
		&Triple{"?accountid", "dena:deviceid", "_:deviceid2"},
	}, nil)
	if len(res) != 2 {
		t.Error("didn't get back 2 userid got: ", res)
	}
	res = GRPH.Query([]*Triple{
		&Triple{"?accountid", "dena:deviceid", "_:deviceid3"},
	}, nil)
	if len(res) != 0 {
		t.Error("should get back nothing got: ", res)
	}

	GRPH.Add("_:twitterid1", "foaf:name", "Charlie Sparklepants")
	GRPH.Add("_:twitterid1", "dena:deviceid", "_:deviceid1")
	GRPH.Add("_:twitterid1", "dena:deviceid", "_:deviceid3")
	res = GRPH.Query([]*Triple{
		&Triple{"?accountid", "dena:deviceid", "_:deviceid3"},
		&Triple{"?accountid", "dena:deviceid", "?deviceid"},
		&Triple{"?otheraccountid", "dena:deviceid", "?deviceid"},
	}, nil)
	if len(res) != 4 {
		t.Fatalf("didn't get 4 back got: %d", len(res))
	}

	i := 0
	var wg sync.WaitGroup
	for {
		if i > 100 {
			break
		}
		wg.Add(1)
		go func(i_ int) {
			defer wg.Done()

			res := GRPH.Query([]*Triple{
				&Triple{"?accountid", "dena:deviceid", "_:deviceid3"},
				&Triple{"?accountid", "dena:deviceid", "?deviceid"},
				&Triple{"?otheraccountid", "dena:deviceid", "?deviceid"},
			}, nil)
			if len(res) != 4 {
				t.Errorf("%d didn't get 4 back got: %d", i_, len(res))
			}
		}(i)
		i++
	}
	wg.Wait()
}

func TestApplyInference(t *testing.T) {
	defer cleanupGraph()

	GRPH.Add("winona", "location:address", "1234 x street")

	//csvFile := strings.NewReader(testCSV)
	//g.Load(csvFile)

	googleCall = func(add string) ([]byte, error) {
		return []byte(testGoogleGeo), nil
	}

	geo := GeoRule{}
	GRPH.ApplyInference(geo)

	triplesLatitude, _ := GRPH.Triples("1234 x street", "location:lat", "", nil)
	triplesLongitude, _ := GRPH.Triples("1234 x street", "location:lng", "", nil)
	if len(triplesLatitude) == 0 {
		t.Error(triplesLatitude)
	}
	if len(triplesLongitude) == 0 {
		t.Error(triplesLongitude)
	}
}

func TestPath(t *testing.T) {
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)
	GRPH.Load(csvFile)

	var tests = []struct {
		s1, s2, pred, predAdj string
		out                   []string
	}{
		{"Tom Cruise", "Kevin Bacon", "name", "starring", []string{"Kevin Bacon", "A Few Good Men", "Tom Cruise"}},
		{"Tom Hanks", "Kevin Bacon", "name", "starring", []string{"Kevin Bacon", "Apollo 13", "Tom Hanks"}},
		{"Val Kilmer", "Kevin Bacon", "name", "starring", []string{"Kevin Bacon", "A Few Good Men", "Tom Cruise", "Top Gun", "Val Kilmer"}},
		{"Pavlos", "Winona Winone", "name", "friends_with", []string{"Winona Winone", "Fuod Ramseys", "Pavlos"}},
		{"Larry Schmiegal", "Winona Winone", "name", "friends_with", []string{"Winona Winone", "Fuod Ramseys", "Pavlos", "Larry Schmiegal"}},
	}
	for i, test := range tests {
		path, _ := GRPH.Path(test.s1, test.s2, test.pred, test.predAdj)
		if len(path) != len(test.out) {
			t.Error("%d %s to %s out: %+v != %+v", i, test.s1, test.s2, test.out, path)
		}
		for j, node := range test.out {
			if path[j] != node {
				t.Errorf("%d %s to %s out: %+v != %+v", i, test.s1, test.s2, test.out, path)
			}
		}
	}
}

func TestSave(t *testing.T) {
	defer cleanupGraph()

	GRPH.Add("paul", "is_not", "human")
	GRPH.Add("paul", "has", "hands")

	filename := goPath + "/mvtest.csv"
	csvFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		t.Error(err)
	}
	defer csvFile.Close()

	err = GRPH.Save(csvFile)
	if err != nil {
		t.Error(err)
	}
	err = os.Remove(goPath + "/mvtest.csv")
	if err != nil {
		t.Error(err)
	}
}

func TestLoad(t *testing.T) {
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)
	err := GRPH.Load(csvFile)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAdd(b *testing.B) {
	cleanupGraph()
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)
	GRPH.Load(csvFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GRPH.Add("winona", "location:address", "1234 x street")
		GRPH.Add("winona", "location:address", "1235 x street")
		GRPH.Add("winona", "location:address", "1236 x street")
		GRPH.Add("winona", "location:address", "1237 x street")
		GRPH.Add("winona", "location:address", "1238 x street")
	}
}

func BenchmarkLoad(b *testing.B) {
	cleanupGraph()
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GRPH.Load(csvFile)
	}
}

func BenchmarkQuery(b *testing.B) {
	cleanupGraph()
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)
	GRPH.Load(csvFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GRPH.Query([]*Triple{
			&Triple{"/en/paul", "has", "?has"},
			&Triple{"/en/paul", "name", "?name"},
			&Triple{"/en/paul", "likes", "?likes"},
		}, nil)
	}
}

func BenchmarkTriples(b *testing.B) {
	cleanupGraph()
	defer cleanupGraph()

	csvFile := strings.NewReader(testCSV)
	GRPH.Load(csvFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GRPH.Triples("", "likes", nil, nil)
	}
}

func genSSOTriples(n int) []*Triple {
	triples := []*Triple{
		&Triple{fmt.Sprintf("_:mobageid%d", n), "mobage:name", fmt.Sprintf("parkles%d", n)},
		&Triple{fmt.Sprintf("_:mobageid%d", n), "dena:deviceid", "_:deviceid1"},
		&Triple{fmt.Sprintf("_:mobageid%d", n), "dena:deviceid", "_:deviceid2"},
		&Triple{fmt.Sprintf("_:facebookid%d", n), "foaf:name", fmt.Sprintf("sparkles%d", n)},
		&Triple{fmt.Sprintf("_:facebookid%d", n), "dena:deviceid", "_:deviceid1"},
		&Triple{fmt.Sprintf("_:facebookid%d", n), "dena:deviceid", "_:deviceid2"},
		&Triple{fmt.Sprintf("_:twitterid%d", n), "foaf:name", fmt.Sprintf("sparkles%d", n)},
		&Triple{fmt.Sprintf("_:twitterid%d", n), "dena:deviceid", "_:deviceid1"},
		&Triple{fmt.Sprintf("_:twitterid%d", n), "dena:deviceid", "_:deviceid3"},
	}

	return triples
}

func addBulkUsers() {
	triples := []*Triple{}
	for i := 0; i < 500; i++ {
		trs := genSSOTriples(i)
		for _, tr := range trs {
			triples = append(triples, tr)
		}
	}
	GRPH.AddBulk(TESTGRAPH, triples)
}

// TODO
func BenchmarkSSOQuery(b *testing.B) {
	addBulkUsers()

	b.ResetTimer()
	i := 0
	var wg sync.WaitGroup
	for {
		if i > 10 {
			break
		}
		wg.Add(1)
		go func(j int) {
			defer wg.Done()

			GRPH.Query([]*Triple{
				&Triple{"?accountid", "dena:deviceid", "_:deviceid3"},
				&Triple{"?accountid", "dena:deviceid", "?deviceid"},
				&Triple{"?otheraccountid", "dena:deviceid", "?deviceid"},
			}, nil)
		}(i)
		i++
	}
	wg.Wait()
}
