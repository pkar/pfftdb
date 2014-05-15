# Python client

### Requirements
```bash
python
```

### Example
```python
import client

pf = client.Pfft()
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
print pf.remove([("larry", "", ""),])
```
