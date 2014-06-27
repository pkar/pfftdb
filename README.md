# pfftdb

1. An expression of dismissal of another's comment. 
2. The sound of a silent and deadly rectal emission.

- Person 1 - You're an idiot.
- Person 2 - Pfft

---

[API](https://github.com/pkar/pfftdb/blob/master/API.md)

---

## Running the server

```bash
cd project
mkdir src
export GOPATH=`pwd`

#Note that the persistence layer is easily interchangeable, just implement the Driver interface. 
#Currently it is MongoDB.
mongod

go get github.com/pkar/pfftdb
go get labix.org/v2/mgo
go get github.com/golang/glog

go run src/github.com/pkar/pfftdb/example/full/main.go -logtostderr -webDir="$(pwd)/src/github.com/pkar/pfftdb/web/"

```

---

## Running example data load
```bash
cd src/github.com/pkar/pfftdb/clients/python/
python example.py

```

---

## Graph Vis
- View the graph at http://localhost:9666 if you run it with the option -webDir and default port which is 9666

![alt text](https://github.com/pkar/pfftdb/raw/master/web/static/images/dracula.png "dracula")

- View the graph at http://localhost:9666/exp.html - for a Three.js experimental version
![alt text](https://github.com/pkar/pfftdb/raw/master/web/static/images/threejs.png "threejs")
