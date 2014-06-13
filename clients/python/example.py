import client
import pprint
db = client.Pfft("127.0.0.1:9666", "user")

db.add([
  ["foaf:Person", "rdf:type", "owl:SymmetricProperty"],

  ["user:1", "user:loggedIn", True],
  ["user:1", "foaf:mbox", "a@a.com"],
  ["user:1", "foaf:name", "Larry Lipshitz"],
  ["user:1", "user:facebook", "fb:1"],
  ["user:1", "user:pref", "_:1"],
  ["user:1", "rdf:type", "foaf:Person"],
  ["user:1", "address", "25 Lusk Street, San Francisco, CA 94107"],
  ["_:1", "item:id", "item:1"],
  ["_:1", "item:val", 1],
  ["user:1", "user:pref", "_:2"],
  ["_:2", "item:id", "item:2"],
  ["_:2", "item:val", 0],
  ["fb:1", "fb:email", "a@facebook.com"],
  ["fb:1", "fb:like", "resource1"],
  ["fb:1", "fb:like", "resource2"],
  ["fb:1", "fb:token", "someaccesstoken"],

  ["user:2", "rdf:type", "foaf:Person"],
  ["user:2", "foaf:knows", "user:1"],
  ["user:2", "address", "500 Howard Street, San Francisco, CA 94107"],
])


# show all triples
print db.triples("", "", "")

# show all addresses
print db.triples("", "address", "")

# show addresses with lat lon
print db.inference("geo")
print db.query([["?id", "address", "?address"], ["?id", "latitude", "?lat"], ["?id", "longitude", "?lon"]])

# get profile
print db.query([["/en/paul", "?pred", "?val"]])

# Find friends with name and address
print db.query([["/en/paul", "friends_with", "?friendid"], ["?friendid", "name", "?friendname"], ["?friendid", "address", "?address"]])

# Find friends of /en/paul with full profile
print db.query([["/en/paul", "friends_with", "?friendid"], ["?friendid", "?pred", "?value"]])

# city and name, notice because no ontology is defined you get states in countries as well
print db.query([["?cityid", "inside", "?biggerthing"], ["?cityid", "name", "?name"]])

# Find paths
print db.path("Val Kilmer", "Kevin Bacon", "name", "starring")
print db.path("Val Kilmer", "Tom Hanks", "name", "starring")
print db.path("Pavlos", "Winona Winone", "name", "friends_with")
print db.path("Larry Schmiegal", "Winona Winone", "name", "friends_with")
