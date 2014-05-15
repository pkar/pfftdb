package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/pkar/pfftdb"
)

func main() {
	httpApiPort := flag.String("httpApiPort", "9666", "port for the http api")
	env := flag.String("env", "development", "environment")
	webDir := flag.String("webDir", "", "web directory for graph visualization full path")
	dbType := flag.String("dbType", "mongo", "mongo or postgres(not currently implemented)")
	dbHosts := flag.String("dbHosts", "localhost", "hosts to db uri, comma seperated")
	dbName := flag.String("dbName", "eurisko", "db name")
	dbUser := flag.String("dbUser", "", "database user")
	dbPass := flag.String("dbPass", "", "database password")
	graphs := flag.String("graphs", "", "comma seperated graph names")
	maxProcs := flag.Int("maxProcs", runtime.NumCPU(), "number of process")
	profile := flag.Bool("profile", false, "enable profiling")
	flag.Parse()

	runtime.GOMAXPROCS(*maxProcs)

	if *profile {
		log.Info("cpuProfile enabled, writing to cpu.out and mem.dat")
		cpuFile, err := os.Create("cpu.out")
		if err != nil {
			log.Fatal(err)
		}
		defer cpuFile.Close()
		pprof.StartCPUProfile(cpuFile)
		defer pprof.StopCPUProfile()

		go func() {
			var stats runtime.MemStats
			for {
				log.Errorf("num goroutine %+v", runtime.NumGoroutine())
				runtime.ReadMemStats(&stats)
				log.Errorf("HeapSys:%+v HeapAlloc:%+v HeapIdle:%+v HeapReleased:%+v", stats.HeapSys, stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased)
				time.Sleep(500 * time.Millisecond)
			}
		}()

		go func() {
			memFile, err := os.Create("mem.dat")
			if err != nil {
				log.Fatal(err)
			}
			defer memFile.Close()
			memFile.WriteString("# Time\tHeapSys\tHeapAlloc\tHeapIdle\tHeapReleased\n")
			var stats runtime.MemStats
			start := time.Now().UnixNano()

			for {
				runtime.ReadMemStats(&stats)
				if memFile != nil {
					memFile.WriteString(
						fmt.Sprintf("%d\t%d\t%d\t%d\t%d\n",
							(time.Now().UnixNano()-start)/1000000,
							stats.HeapSys,
							stats.HeapAlloc,
							stats.HeapIdle,
							stats.HeapReleased,
						),
					)
					time.Sleep(time.Second)
				} else {
					break
				}
			}
		}()
	}

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
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	select {
	case sig := <-interrupt:
		store.Close()
		log.Infof("Captured %v, exiting...", sig)
	}

	log.Error("done.")
}
