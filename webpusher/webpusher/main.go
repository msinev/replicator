package main

import (
	"flag"
	"github.com/msinev/replicator/pool"
	"github.com/msinev/replicator/webpusher/reader"
	"github.com/op/go-logging"
)

const (
	//
	REDIS_HOST = "127.0.0.1"
	REDIS_PORT = "6379"
	//
	HTTP_HOST = "127.0.0.1"
	HTTP_PORT = "8079"
	//
	URL_BASE    = "/data"
	URL_SYNC    = URL_BASE + "/full"
	URL_SOCKET  = URL_BASE + "/socket"
	URL_OPTIONS = URL_BASE + "/options"
	URL_DELTA   = URL_BASE + "/delta"
)

var Sessions map[string]*WebClient

var strRedis = flag.String("redis", REDIS_HOST+":"+REDIS_PORT, "Redis address list")
var sentinelFlag = flag.Bool("sentinel", false, "Use sentinel")

//
var serverAddress = flag.String("http", HTTP_HOST+":"+HTTP_PORT, "HTTP Server address")
var urlSocket = flag.String("urlsocket", URL_SOCKET, "URL listen for sockets")
var urlSync = flag.String("urlsync", URL_SOCKET, "URL listen for sync")
var urlDelta = flag.String("urldelta", URL_SOCKET, "URL listen for deltas")
var dbparam = flag.String("databases", "0", "List of databases like 5,0,3,8")
var dbplain = flag.String("plaindb", "0", "List of version less databases like 3,8")
var log = logging.MustGetLogger("WEB.PUSH")

//var SessionCache *ttlcache.Cache
var DBS []int
var DBSPlain map[int]string

const maxDB = 16

func main() {
	flag.Parse()

	flag.VisitAll(func(f *flag.Flag) {
		log.Debugf("Flag %s ⇶ \"%s\" ⇷ \"%s\"", f.Name, f.Value, f.DefValue)
	})

	logRotate()

	if !dbCheck() {
		log.Critical("Database check failed.")
		return
	}

	log.Info("Starting REDIS pool")
	go pool.RedisExecutor(*strRedis)

	gopt := reader.GlobalOptions{
		RedisURL: strRedis,
		//ForceCheckVersion: !bGetOrElse(noVersion, false),
	}

	so := make([]reader.ServerOptions, len(DBS))
	for k, _ := range so {
		so[k] = reader.ServerOptions{Global: &gopt, Index: k, DB: DBS[k]}
	}

	mainServerHTTPLoop(so, *serverAddress, false)

}
