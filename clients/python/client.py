import csv
import sys
import json
import requests
import pprint

PREFIX = {
  'foaf': 'http://xmlns.com/foaf/0.1/',
  'dc': 'http://purl.org/dc/elements/1.1/',
  'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
  'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
  'owl': 'http://www.w3.org/2002/07/owl#',
  'geonames': 'http://www.geonames.org/ontology#',
  'geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
  'skos': 'http://www.w3.org/2004/02/skos/core#',
  'rss': 'http://purl.org/rss/1.0/',
  'dbpedia': 'http://dbpedia.org/resource/',
	"eurisko": "http://eurisko.io.com/rdf/0.1/eurisko/",
	"location": "http://eurisko.io.com/rdf/0.1/location/",
	"fb": "http://eurisko.io.com/rdf/0.1/facebook/",
	"dena": "http://eurisko.io.com/rdf/0.1/dena/",
	"mobage": "http://eurisko.io.com/rdf/0.1/mobage/",
}
#PREFIX = {}

class Pfft:
  """
  A pfftdb communicator.
  """
  host_port = "127.0.0.1:9666"
  graph = ""
  debug = False

  def __init__(self, host_port, graph, debug=False):
    """
    Initialize a socket to host:port
    """
    if host_port:
      self.host_port = host_port
    self.graph = graph
    self.debug = debug

  def add(self, triples):
    """
    Add a list of triples

    >>> self.add([("a", "b", "c"), ("b", "c", "d")])
    OK
    """
    req = {
      'graph': self.graph, 
      'data': triples,
      'prefix': PREFIX,
    }
    if self.debug:
      pprint.pprint(req)
    r = requests.post('http://' + self.host_port + '/v1/data', data=json.dumps(req))
    return r.text

  def remove_all(self):
    """
    Remove all triples in the graph
    """
    req = {
      'graph': self.graph, 
      'data': [['', '', '']],
      'prefix': PREFIX,
    }
    if self.debug:
      pprint.pprint(req)
    r = requests.delete('http://' + self.host_port + '/v1/data', data=json.dumps(req))
    return r.text

  def remove(self, triples):
    """
    Remove a list of triples

    >>> self.remove([("a", "b", "c"), ("b", "c", "d")])
    OK
    """
    req = {
      'graph': self.graph, 
      'data': triples,
      'prefix': PREFIX,
    }
    if self.debug:
      pprint.pprint(req)
    r = requests.delete('http://' + self.host_port + '/v1/data', data=json.dumps(req))
    return r.text

  def triples(self, sub, pred, obj, options=None):
    """
    Get a list of triples

    >>> self.triples("", "", "")
    [{"a": "b", "c"}, {"a": "b", "d"}]
    """
    req = {
      "graph": self.graph,
      "sub": sub,
      "pred": pred,
      "obj": obj,
      'prefix': PREFIX,
    }
    if options:
      for k, v in options.iteritems():
        req[k] = v

    if self.debug:
      pprint.pprint(req)
    r = requests.post('http://' + self.host_port + '/v1/triples', data=json.dumps(req))
    if r.status_code == requests.codes.ok:
      return r.json['data'] if isinstance(r.json, dict) else r.json()['data']
    return r.text

  def value(self, sub, pred, obj):
    """
    Get a value of a triple

    >>> self.value("a", "b", "")
    c
    """
    req = {
      "graph": self.graph,
      "sub": sub,
      "pred": pred,
      "obj": obj,
      'prefix': PREFIX,
    }

    if self.debug:
      pprint.pprint(req)
    r = requests.post('http://' + self.host_port + '/v1/value', data=json.dumps(req))
    if r.status_code == requests.codes.ok:
      return r.json['data'] if isinstance(r.json, dict) else r.json()['data']
    return r.text

  def count(self, sub, pred, obj):
    req = {
      "graph": self.graph,
      "sub": sub,
      "pred": pred,
      "obj": obj,
      'prefix': PREFIX,
    }
    if self.debug:
      pprint.pprint(req)
    r = requests.post('http://' + self.host_port + '/v1/triples/count', data=json.dumps(req))
    if r.status_code == requests.codes.ok:
      return r.json['data'] if isinstance(r.json, dict) else r.json()['data']
    return r.text

  def query(self, triples, options=None):
    """
    Query the db

    >>> self.query([("?a", "b", "c"), ("?a", "c", "d")])
    [{"a": "string1"}, {"a": "string2"}]
    """
    req = {
      "graph": self.graph,
      "data": triples,
      'prefix': PREFIX,
     }
    if options:
      for k, v in options.iteritems():
        req[k] = v
    if self.debug:
      pprint.pprint(req)
    r = requests.post('http://' + self.host_port + '/v1/query', data=json.dumps(req))
    if r.status_code == requests.codes.ok:
      return r.json['data'] if isinstance(r.json, dict) else r.json()['data']
    return r.text

  def path(self, start, end, pred_name, pred_adj):
    """
    Find a path

    >>> self.path("a", "c", "name", "to")
    {graph: "graph", data: ["a", "to", "b", "c"]}
    """
    req = {
      "graph": self.graph,
      "start": start,
      "end": end,
      "predicateName": pred_name,
      "predicateAdjacent": pred_adj,
      'prefix': PREFIX,
    }
    if self.debug:
      pprint.pprint(req)
    r = requests.get('http://' + self.host_port + '/v1/path', params=req)
    if r.status_code == requests.codes.ok:
      return r.json['data'] if isinstance(r.json, dict) else r.json()['data']
    return r.text

  def inference(self, name):
    """
    Apply an inference
    >>> self.inference("geo")
    OK
    """
    req = {
      "graph": self.graph,
      "inference": name,
    }
    if self.debug:
      pprint.pprint(req)
    r = requests.put('http://' + self.host_port + '/v1/inference', data=req)
    return r.text

  def load(self, filename):
    """
    Load a csv file into the graph
    """
    f = open(filename, "rb")
    for sub, pred, obj in csv.reader(f):
      try:
        self.add(unicode(sub, "UTF-8"), unicode(pred, "UTF-8"), unicode(obj, "UTF-8"))
      except:
        print "couldn't add", sub, pred, obj
    f.close()

  def print_csv():
    for t in self.triples("", "", ""):
      csv.writer(sys.stdout).writerow(t) 

def example():
  pf = Pfft("127.0.0.1:9666", "user")
  print "add ",
  print pf.add([
    ("larry", "name", "Larry"),
    ("larry", "address", "1234 bluebird way, san francisco, ca"),
  ])

  print "inference ",
  print pf.inference("geo")

  print "triples ",
  print pf.triples("", "", "")

  print "query ",
  print pf.query([["?id", "address", "?address"]])

  print "remove ",
  print pf.remove([("larry", "address", ""),])

if __name__ == '__main__':
  db = Pfft("127.0.0.1:9666", "user")

