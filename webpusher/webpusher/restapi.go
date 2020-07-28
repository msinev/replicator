package main

import (
	"fmt"
	"github.com/msinev/replicator/webpusher/reader"
	"net/http"
)

func reply(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<HTML><BODY><p>You've directly requested the root of <b>REDIS push server</b>. It is <b>NOT INTENDED USAGE</b>." +
		" Please use intended paths like <a href=\"/data\">/data</a> via "+
		"<a href=\"http://nginx.org\">nginx</a> or similar reverse proxy server with correspondent static content.")

}

func options(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "[]")
}

func sockets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "[]")
}

func delta(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "[]")
}


func InitReaders(so []reader.ServerOptions) ([]ScanReader, []reader.DeltaReceiver) {
	var DBDeltaReaders []reader.DeltaReceiver
	var DBScanReaders []ScanReader

	redisURL := *so[0].Global.RedisURL
	//onceDB.Do( func () {

	go reader.RedisExecutor(redisURL)

	ldbs := len(DBS)

	DBDeltaReaders = make([]reader.DeltaReceiver, ldbs)
	DBScanReaders = make([]ScanReader, ldbs)

	chandb := make([]chan<- string, ldbs)

	log.Info("Starting REDIS connectors")
	go reader.RedisExecutor(redisURL)
	log.Info("Init readers")

	//go ReadVersionDelta(start  <-chan string, out chan <- VersionData, db int )

	for r := range DBDeltaReaders {
		db := DBS[r]
		_, ok := DBSPlain[db]
		ver, dbver := reader.GetVersion(db)

		if !ok && !dbver {
			log.Errorf("Database %d missing version", db)
		} else if dbver {
			log.Infof("Database %d starting version %d", db, ver)
		}
		dbt := 0
		if ok || !dbver {
			log.Panic() // shall we panic ??
			// TODO fix subscription on plain DBs or missing versions
			dbt = 1
		} else {
			DBDeltaReaders[r].Init(so[r], dbt, ver)
			chandb[r] = DBDeltaReaders[r].NotifyVer
		}

	}

	go reader.RedisSubscriber(redisURL, DBS, DBSPlain, chandb)

	log.Info("Starting scan sunscribers")
	for k, _ := range DBS {
		DBScanReaders[k] = *InitScanReaderWithVersionCheck(so[k])
	}
	log.Info("Readers init done")
	//})
	return DBScanReaders, DBDeltaReaders
}
