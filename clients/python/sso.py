import client
import pprint
db = client.Pfft("127.0.0.1:9666", "sso")

def user_triples(n):
  return [
		["_:mobageid%d" % n, "mobage:name", "parkles%d" % n],
		["_:mobageid%d"% n, "dena:deviceid", "_:deviceid%d1" % n],
		["_:mobageid%d"% n, "dena:deviceid", "_:deviceid%d2" % n],
		["_:facebookid%d"% n, "foaf:name", "sparkles%d" % n],
		["_:facebookid%d"% n, "dena:deviceid", "_:deviceid%d1" % n],
		["_:facebookid%d"% n, "dena:deviceid", "_:deviceid%d2" % n],
		["_:twitterid%d"% n, "foaf:name", "sparkles%d" % n],
		["_:twitterid%d"% n, "dena:deviceid", "_:deviceid%d1" % n],
		["_:twitterid%d"% n, "dena:deviceid", "_:deviceid%d3" % n],
  ]

def chunks(l, n):
  return [l[i:i+n] for i in range(0, len(l), n)]

def bloat():
  for chunk in chunks(range(10000000), 100000):
    triples = []
    for i in chunk:
      triples += user_triples(i)
    print db.add(triples)

bloat()

#print db.count("", "", None)

from time import time
starttime = time()
res = db.query([
  ["?accountid", "dena:deviceid", "_:deviceid193"],
  ["?accountid", "dena:deviceid", "?deviceid"],
  ["?otheraccountid", "dena:deviceid", "?deviceid"],
  ], {'select': ['otheraccountid'], 'distinct': True})
timetaken = time() - starttime
pprint.pprint(res)
print timetaken
