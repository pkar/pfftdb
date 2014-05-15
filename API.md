# API

## GRAPHS
### GET /v1/graphs
Get a list of current graphs.

#### Response
```javascript
200
{
	"data": ["user"]
}
```

#### Response error
```javascript
400 Bad Request
```

#### curl
```bash
$ curl http://localhost:9666/v1/graphs
{"data": ["user"]}
```

## ADD
### POST /v1/data
Add a list of triples. The total number inserted is returned. Invalid triples(no sub, pred, or obj) are removed from the insert.

#### JSON Parameters
* <b>graph</b> (required) graph name.
* <b>prefix</b> (optional) uri prefix, will replace all items in data. For example foaf:name = http://xmlns.com/foaf/0.1/name
* <b>data</b> (required) triples, uses prefix if defined.

#### Request
```javascript
{
	"graph": "user",
	"prefix": {
		"eu": "https://eurisko.io/rdf/0.1/",
		"foaf": "http://xmlns.com/foaf/0.1/",
		"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	},
	"data": [
		["_:1", "rdf:type", "foaf:Person"],
		["_:1", "foaf:name", "Albert"],
		["_:1", "eu:username", "alberto1"],
		["_:2", "foaf:name", "Barry"],
		["_:2", "eu:username", "bthug"],
		["_:3", "foaf:name", "Charles"],
		["_:3", "eu:username", "cthug"],
		["_:1", "", "_:2"],
		["_:1", "foaf:knows", "_:3"]
	]
}
```

#### Response
```javascript
200 {"total": 8}
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
```

#### curl
```bash
# Note if no prefix is defined you have to specifiy the full uri of subjects and predicates.
$ curl -d '{"graph":"user", "data": [["_:1", "http://xmlns.com/foaf/0.1/name", "Albert"]]'  http://localhost:9666/v1/data
{"total": 1}
```

## DELETE
### DELETE /v1/data
Remove a list of triples.

#### JSON Parameters
* <b>graph</b> (required) graph name.
* <b>prefix</b> (optional) uri prefix, will replace all items in data. For example foaf:name = http://xmlns.com/foaf/0.1/name
* <b>data</b> (required) triples uses prefix if defined. Empty strings mean delete all for sub, pred, obj.

#### Request
```javascript
{
	"graph": "user",
	"prefix": {
		"foaf": "http://xmlns.com/foaf/0.1/"
	},
	"data": [
		["_:1", "foaf:name", "Albert"],
		["_:3", "foaf:name", ""]
	]
}
```

#### Response
```javascript
200 OK
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
```

#### curl
```bash
# Note if no prefix is defined you have to specifiy the full uri of subjects and predicates.
$ curl -X DELETE -d '{"graph":"user", "data": [["_:1", "http://xmlns.com/foaf/0.1/name", "Albert"]]'  http://localhost:9666/v1/data
OK
```

## TRIPLES
### POST /v1/triples
Get a list of triples. The reason this is json encoded is to preserve the type for the obj parameter(bool,int,string,etc)

#### JSON Parameters
* <b>graph</b> (required) graph
* <b>prefix</b> (optional) uri prefix, will replace all items in data. For example foaf:name = http://xmlns.com/foaf/0.1/name
* <b>sub</b> (optional) subject
* <b>pred</b> (optional) predicate
* <b>obj</b> (optional) object, can be nil
* <b>limit</b> (optional:default 20) number of triples to return
* <b>offset</b> (optional:default 0) skip to
* <b>orderby</b> (optional string) sort by sub(s), pred(p), obj(o). A minus in front of the character means descending.

```javascript
{
	"graph": "user",
	"prefix": {"foaf": "http://foaf"},
	"sub": "_:1",
	"pred": "http://xmlns.com/foaf/0.1/knows",
	"obj": "_:2",
	"limit": 1,
	"offset": 0,
	"orderby": "-s"
}
```

#### Response
```javascript
200
{
	"graph": "user",
	"data": [
		["_:1", "http://xmlns.com/foaf/0.1/knows", "_:2"],
	]
}
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```

#### curl
```bash
$ curl  -d '{"graph": "user", "sub": "_:1", "pred": "", obj: "_:2"}' http://localhost:9666/v1/triples
{"graph": "user", "data":[["_:1", "http://xmlns.com/foaf/0.1/knows", "_:2"]]}
```

## TRIPLES COUNT
### POST /v1/triples/count
Get the number of triples for a query.

#### JSON Parameters
* <b>graph</b> (required) graph
* <b>prefix</b> (optional) uri prefix, will replace all items in data. For example foaf:name = http://xmlns.com/foaf/0.1/name
* <b>sub</b> (optional) subject
* <b>pred</b> (optional) predicate
* <b>obj</b> (optional) object, can be nil

```javascript
{
	"graph": "user",
	"prefix": {"foaf": "http://foaf"},
	"sub": "",
	"pred": "",
	"obj": ""
}
```

#### Response
```javascript
200
{
	"graph": "user",
	"data": 101
}
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```

#### curl
```bash
$ curl  -d '{"graph": "user", "sub": "", "pred": "", obj: ""}' http://localhost:9666/v1/triples/count
{"graph": "user", "data": 101}
```

## VALUE
### POST /v1/value
Get a single value from a triple.

#### JSON Parameters
* <b>graph</b> (required) graph
* <b>prefix</b> (optional) uri prefix, will replace all items in data. For example foaf:name = http://xmlns.com/foaf/0.1/name
* <b>sub</b> (optional) subject
* <b>pred</b> (optional) predicate
* <b>obj</b> (optional) object, can be nil

```javascript
{
	"graph": "user",
	"prefix": {"foaf": "http://foaf"},
	"sub": "_:1",
	"pred": "http://xmlns.com/foaf/0.1/knows",
	"obj": nil,
}
```

#### Response
```javascript
200
{
	"graph": "user",
	"data": "_:2"
}
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```

#### curl
```bash
$ curl  -d '{"graph": "user", "sub": "_:1", "pred": "", obj: ""}' http://localhost:9666/v1/value
{"graph": "user", "data": "_:2"}
```

## QUERY
### POST /v1/query
Get a list of bound variables.

#### JSON Parameters
* <b>graph</b> (required) graph
* <b>data</b> (required) query bindings in triples, uses prefix if defined.
* <b>prefix</b> (optional) uri prefix, will replace all items in data query.
* <b>select</b> (required) array of which variables to return, if empty all variables returned. If count ?COUNT data contains a count.
* <b>optional</b> array of the indexes within data of the triples to treat as optional, meaning return results even if the optional triple has none.
* <b>distinct</b> (optional:default true) return distinct results
* <b>limit</b> (optional:default 20) number of items to return.
* <b>offset</b> (optional:default 0) skip.
* <b>orderby</b> (optional) sort by variable, a minus in front of string means descending sort.
* <b>filter</b> (optional) array of filters [{key: 'clicks', op: '<', val: 3}, {key: 'age', op: '>', val: 20}]

```javascript
{
	"graph": "user",
	"prefix": {
		"eu": "https://eurisko.io/rdf/0.1/",
		"foaf": "http://xmlns.com/foaf/0.1/"
	},
	"select": ["?userid", "?knows_name", "?knows_username"],
	"data": [
		["?userid", "foaf:knows", "?knowsid"],
		["?knowsid", "foaf:name", "?knows_name"]
		["?knowsid", "eu:username", "?knows_username"]
	],
	"optional": [1, 2],
	"distinct": true,
	"limit": 2,
	"offset": 10,
	"orderby": "-userid"
}
```

#### Response
```javascript
200
{
	"graph": "user",
	"data": [
		{"userid": "_:1", "knows_name": "Barry", "knows_username": "bthug"}, 
		{"userid": "_:1", "knows_name": "Charles", "knows_username": "cthug"}
	]
}
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```

#### curl
```bash
$ curl -d '{"graph": "user", "prefix": {"eu": "https://eurisko.io/rdf/0.1/"}, "data": [["?userid", "eu:name", "Albert"]]'  http://localhost:9666/v1/query
{"graph": "user", "data":[["userid", "https://eurisko.io/rdf/0.1/user/2", "name": "Albert"]]}
```

## Inference
### PUT /v1/inference
Apply named inference rule

#### Parameters
* <b>graph</b> (required) graph
* <b>inferenece</b> (required) name of the inference to apply, (geo)

#### Response
```javascript
200
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```

## PATH
### GET /v1/path
Get any paths available from a start to end node.

#### Parameters
* <b>graph</b> (required) graph
* <b>start</b> 
* <b>end</b> 
* <b>predicateName</b> 
* <b>predicateAdjacent</b> 

#### Response
```javascript
200 
{
	"graph": "user",
	"data": ["a", "connects", "to", "b"]
}
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```

## DROP
### POST /v1/drop
Drop a graph and it's indexes

#### JSON Parameters
* <b>graph</b> (required) graph name.

#### Response
```javascript
200 OK 
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```

## INDEX
### POST /v1/index
Index a graph

#### JSON Parameters
* <b>graph</b> (required) graph name.
* <b>background</b> (optional) index in the background.

#### Response
```javascript
200 OK 
```

#### Response error
```javascript
400 Bad Request
405 Method Not Allowed
500 Internal Server Error
```
