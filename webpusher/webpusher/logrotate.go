package main

import (
	"github.com/op/go-logging"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"strconv"
)

func logRotate() {
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
