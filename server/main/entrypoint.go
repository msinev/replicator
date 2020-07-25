package main

import (
	"flag"
	"github.com/msinev/replicator/pool"
	"github.com/msinev/replicator/reader"
	"github.com/op/go-logging"
	"io"
	"sort"
	"strconv"
	"strings"
	//	"RedisReplica/Reader"
	"io/ioutil"
	"os"
	"os/user"
	"path"
)

const (
	//
	REDIS_HOST = "127.0.0.1"
	REDIS_PORT = "6379"
	//
	TLS_NAMES_CA     = "ca.pem"
	TLS_NAMES_CRTKEY = "certificate.crt,certificate.key"
	//
	CONN_HOST = ""
	CONN_PORT = "3333"
	//
	CONN_TYPE_TCP = "tcp"
	CONN_TYPE_TLS = "tls"
)

// ----
var strRedis = flag.String("redis", REDIS_HOST+":"+REDIS_PORT, "Redis address list")
var sentinelFlag = flag.Bool("sentinel", false, "Use sentinel")

//
var strTLSCA = flag.String("TLSCA", TLS_NAMES_CA, "Certificate authority")
var strTLSCRT = flag.String("TLSCRT", TLS_NAMES_CRTKEY, "X.509 Certificate")

//
var serverAddress = flag.String("hub", CONN_HOST+":"+CONN_PORT, "Server address")
var serverFlag = flag.Bool("server", false, "Listen for requests")
var dbparam = flag.String("databases", "", "List of databases like 5,0,3,8")
var dbplain = flag.String("plaindb", "", "List of version less databases like 3,8")
var serverNoWait = flag.Bool("nowait", false, "Don't wait for client requests termination")
var noVersion = flag.Bool("lv", false, "Ignore source version - create version chain locally")

//var dbLo = flag.Int("serverLo", 5, "Lo priority DB")
/*
func init() {
	// example with short version for long flag
	//flag.StringVar(strFlag, "l", CONN_HOST+":"+CONN_PORT, "Server")
}
*/

var log = logging.MustGetLogger("REPA")

var DBS []int
var DBSPlain map[int]string

const maxDB = 16

func bGetOrElse(option *bool, def bool) bool {
	if option == nil {
		return def
	}
	return *option
}

func sGetOrElse(option *string, def string) string {
	if option == nil {
		return def
	}
	return *option
}

func iGetOrElse(option *int, def int) int {
	if option == nil {
		return def
	}
	return *option
}

func dbCheck() bool {
	DBSPlain = make(map[int]string)

	var plainlist []int

	if dbplain != nil && strings.TrimSpace(*dbplain) != "" {

		dbparse := strings.Split(*dbplain, ",")

		ldbs := len(dbparse)
		if ldbs < 1 || ldbs > maxDB {
			log.Errorf("databases count wrong: %d\n", ldbs)
			return false
		}
		//DBS=make([]int, ldbs)
		for _, v := range dbparse {
			dbi, err := strconv.Atoi(v)
			DBSPlain[dbi] = "+"

			if err != nil {
				log.Error("Error parsing databases ", err)
				return false
			}

		}
		plainlist = append([]int(nil), DBS...)
		sort.Ints(plainlist)
		dbcheck := plainlist[0]

		for i := 1; i < ldbs; i++ {
			nextdb := plainlist[i]

			if nextdb == dbcheck {
				log.Error("Database dublicated: %d\n", dbcheck)
				return false
			}
			dbcheck = nextdb
		}
		//	log.Info("Databases: ", DBSPlain)
		log.Info("Databases in list: ", plainlist)

	} else {

		log.Warning("No plain databases defined")
		//return false
	}

	if dbparam != nil && strings.TrimSpace(*dbparam) != "" {

		dbparse := strings.Split(*dbparam, ",")

		ldbs := len(dbparse)
		if ldbs < 1 || ldbs > maxDB {
			log.Errorf("databases count wrong: %d\n", ldbs)
			return false
		}
		DBS = make([]int, ldbs)
		for i, v := range dbparse {
			dbi, err := strconv.Atoi(v)
			DBS[i] = dbi
			if err != nil {
				log.Error("Error parsing databases ", err)
				return false
			}
		}
		dbcopy := append([]int(nil), DBS...)

		sort.Ints(dbcopy)

		dbcheck := dbcopy[0]
		for i := 1; i < ldbs; i++ {
			nextdb := dbcopy[i]

			if nextdb == dbcheck {
				log.Error("Database dublicated: %d\n", dbcheck)
				return false
			}
			dbcheck = nextdb
		}

		log.Info("Databases: ", DBS)
		log.Info("Databases in list: ", dbcopy)

		for _, iv := range plainlist {
			ivp := sort.SearchInts(dbcopy, iv)
			if ivp >= ldbs || dbcopy[ivp] != iv {
				log.Error("Plain database not present in db list")
				return false
			}

		}
	} else {
		log.Error("No databases defined")
		return false
	}

	return true
}

func main() {
	serverMode := true
	flag.Parse()
	if serverFlag != nil {
		serverMode = *serverFlag
		//  log.Printf("server mode")
	}

	if serverMode {
		var format = logging.MustStringFormatter(
			`%{time:15:04:05.000} %{shortfile} %{callpath} ▶ %{level:.4s} %{message}`, // %{color} ...%{id:03x}%{color:reset}
		)

		var formatErr = logging.MustStringFormatter(
			`%{level:.6s} %{time:15:04:05.000} %{longfile} %{shortfunc} %{id:03x} ▶ %{message} ◀ %{callpath}`,
		)

		sLogName := "rrserver.log"
		sLogPrefix := sLogName

		eLogName := "rrerror.log"
		eLogPrefix := eLogName

		// For demo purposes, create two backend for os.Stderr.
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}

		slog := path.Join(usr.HomeDir, sLogName)

		var sLogFile *os.File
		var eLogFile *os.File
		var slogTmp *os.File
		var eLogTmp *os.File

		logInfo := make([]string, 0, 2)
		_, e0 := os.Stat(slog)
		if !os.IsNotExist(e0) { // Exists
			if e0 != nil {
				//println(e0)
				panic(e0)
			}
			// path/to/whatever exists
			var e1 error
			slogTmp, e1 = ioutil.TempFile(usr.HomeDir, sLogPrefix)
			if e1 != nil {
				panic(e1)
			}
			var e2 error
			sLogFile, e2 = os.Open(slog)
			if e2 != nil {
				panic(e2)
			}
			io.Copy(slogTmp, sLogFile)
			logInfo = append(logInfo, "Moved logs "+sLogFile.Name()+"->"+slogTmp.Name())
			slogTmp.Close()
			//		sLogFile.Seek(0,0)
			//		sLogFile.Truncate(0)
			sLogFile.Close()
			sLogFile, e2 = os.Create(slog)
		} else {
			var e2 error
			sLogFile, e2 = os.Create(slog)
			logInfo = append(logInfo, "Created log "+sLogFile.Name())
			if e2 != nil {
				panic(e2)
			}
		}

		elog := path.Join(usr.HomeDir, eLogName)
		s, e0 := os.Stat(elog)
		if !os.IsNotExist(e0) { //exists

			if e0 != nil {
				panic(e0)
			}

			var e2 error
			eLogFile, e2 = os.Open(elog)
			if e2 != nil {
				panic(e2)
			}
			if s.Size() < 1024*1024*1024 {
				logInfo = append(logInfo, "Reusing errlogs "+eLogFile.Name()+" on offset "+strconv.Itoa(int(s.Size())))
			} else {
				// path/to/whatever exists
				var e1 error
				eLogTmp, e1 = ioutil.TempFile(usr.HomeDir, eLogPrefix)
				if e1 != nil {
					panic(e1)
				}
				io.Copy(eLogTmp, eLogFile)
				logInfo = append(logInfo, "Moved error slogs "+eLogFile.Name()+"->"+eLogTmp.Name())

				eLogTmp.Close()
				//		eLogFile.Seek(0,0)
				//		eLogFile.Truncate(0)
				eLogFile.Close()
				eLogFile, e2 = os.Create(elog)
			}
		} else {

			var e2 error
			eLogFile, e2 = os.Create(elog)

			logInfo = append(logInfo, "Created error log "+sLogFile.Name())

			if e2 != nil {
				panic(e2)
			}

		}

		//f2, err := ioutil.TempFile(usr.HomeDir, "server.severe.log")

		//if(err!=nil) {
		//	println("Unable to open log file")
		//}
		//w := bufio.NewWriter(f)

		backendErr := logging.NewLogBackend(eLogFile, "", 0)
		backendStd := logging.NewLogBackend(sLogFile, "", 0)

		// For messages written to backend2 we want to add some additional
		// information to the output, including the used log level and the name of
		// the function.
		backendStdFormatter := logging.NewBackendFormatter(backendStd, format)
		backendErrFormatter := logging.NewBackendFormatter(backendErr, formatErr)

		// Only errors and more severe messages should be sent to backend1
		backendErrLeveled := logging.AddModuleLevel(backendErrFormatter)
		backendErrLeveled.SetLevel(logging.ERROR, "")

		// Set the backends to be used.
		logging.SetBackend(backendErrLeveled, backendStdFormatter)

		for _, v := range logInfo {
			log.Info(v)
			println("Debug log " + v)
		}

		println("Log: " + sLogFile.Name())
		println("Error log: " + eLogFile.Name())
	}

	flag.VisitAll(func(f *flag.Flag) {
		log.Debugf("Flag %s ⇶ \"%s\" ⇷ \"%s\"", f.Name, f.Value, f.DefValue)
	})

	if !dbCheck() {
		log.Critical("Database check failed.")
		return
	}
	wait := !bGetOrElse(serverNoWait, false)

	go pool.RedisExecutor(*strRedis)
	gopt := reader.GlobalOptions{
		LocalVersion: bGetOrElse(noVersion, false),
		RedisURL:     strRedis,
		//ForceCheckVersion: !bGetOrElse(noVersion, false),
	}
	gopt.Dump()
	if serverMode {
		so := make([]reader.ServerOptions, len(DBS))
		for k, _ := range so {
			so[k] = reader.ServerOptions{Global: &gopt, Index: k, DB: DBS[k]}
		}

		log.Info("Starting TCP server...")
		mainServerTCPLoop(so, *serverAddress, wait)

	} else { // Client mode

		if bGetOrElse(serverNoWait, false) {
			log.Error("Misconfiguration -nowait has no effect in client mode")
			return
		}

		log.Info("Starting TCP client...")
		mainClientTCP(*serverAddress, gopt)
		log.Info("TCP client terminated...")
	}
}
