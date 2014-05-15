import client
import pprint
db = client.Pfft("127.0.0.1:9666", "user")

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
