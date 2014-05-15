package main

import (
	"flag"
	"os"
	"os/signal"

	log "github.com/golang/glog"
	"github.com/pkar/pfftdb"
)

func main() {
	httpApiPort := flag.String("httpApiPort", "6666", "port for the http api")
	env := flag.String("env", "development", "environment")
	webDir := flag.String("webDir", "", "web directory for graph visualization, src/web")
	dbType := flag.String("dbType", "mongo", "mongo or postgres(not currently implemented)")
	dbHosts := flag.String("dbHosts", "localhost", "hosts to db uri, comma seperated")
	dbName := flag.String("dbName", "eurisko", "db name")
	dbUser := flag.String("dbUser", "", "database user")
	dbPass := flag.String("dbPass", "", "database password")
	graphs := flag.String("graphs", "user", "comma seperated graph names")
	flag.Parse()

	log.Infof(`
		HttpAPIPort:%s 
		Env:%s 
		DBType:%s
		DBHosts:%s
		DBName:%s 
		DBUser:%s 
		DBPass:%s 
		WebDir:%s 
		Graphs:%v`,
		*httpApiPort,
		*env,
		*dbType,
		*dbHosts,
		*dbName,
		*dbUser,
		*dbPass,
		*webDir,
		*graphs,
	)

	store, err := pfftdb.New(
		*httpApiPort,
		*env, *dbType,
		*dbHosts,
		*dbName,
		*dbUser,
		*dbPass,
		*graphs,
		*webDir,
	)
	if err != nil {
		log.Fatal(err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	select {
	case sig := <-interrupt:
		store.Close()
		log.Infof("Captured %v, exiting...", sig)
	}

	log.Error("done.")
}
