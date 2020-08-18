package main

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/msinev/replicator/webpusher/reader"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

var httpSrv *http.Server
var httpStoped sync.WaitGroup
var httpStopper sync.Once

func mainServerHTTPLoop(so []reader.ServerOptions, bind string, waitTermination bool) {
	addr := *serverAddress
	dbIndex := make(map[int]int, len(so))
	for idb := 0; idb < len(so); idb++ {
		dbIndex[so[idb].DB] = idb
	}
	log.Info("Listening http at " + addr)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	scansrc, deltasrc := InitReaders(so)
	r := mux.NewRouter()
	r.HandleFunc("/", reply)

	r.HandleFunc(URL_SYNC, func(w http.ResponseWriter, r *http.Request) {
		fullCopy(w, r, scansrc, deltasrc)
	})

	r.HandleFunc(URL_DELTA, func(w http.ResponseWriter, r *http.Request) {
		getDelta(w, r)
	})

	r.HandleFunc(URL_SYNC, func(w http.ResponseWriter, r *http.Request) {
		startSession(w, r, scansrc, deltasrc, so, dbIndex)
	})

	r.HandleFunc(URL_SOCKET, sockets)
	r.HandleFunc(URL_OPTIONS, options)

	srv := &http.Server{Addr: addr, Handler: r}

	go func() {
		for sig := range c {

			// sig is a ^C, handle it

			log.Fatal(sig.String())
			go func() {
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				srv.Shutdown(ctx)
			}()

		}
	}()

	log.Info("Starting server at %s\n", srv.Addr)
	err := srv.ListenAndServe() // start
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

/*
{
	// Listen for incoming connections.

	http.HandleFunc("/", reply)

	handler := server.NewRouter()
	srv := &http.Server{
		Handler: handler,
	}

	http.ListenAndServe(*serverAddress, nil)
	http.S

	var counter uint32
	l, err := net.Listen(CONN_TYPE_TCP, bind)

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.

	defer l.Close()

	sigs := make(chan os.Signal, 1)

	go func() {
		for {
			select {
			case sig, ok := <-sigs:

				if !ok {
					return
				}

				fmt.Println("Received signal")
				fmt.Println(sig)

				l.Close()
			}

		}
	}()
*/

/*
	fmt.Println("Listening on " + bind)
	var wg sync.WaitGroup

	//readers:=InitScanReadersWithVersionCheck()
	scan, delta := InitReaders(so)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		counter++
		wg.Add(1)

		if err != nil {
			log.Errorf("Socket returned error: %s", err.Error())
			if waitTermination {

				go func() {
					time.Sleep(time.Second * 10)
					os.Exit(2)
				}()

				wg.Wait()
			}
			os.Exit(1)
		}
		log.Infof("Connected to %s (connection %d)", conn.RemoteAddr(), counter)
		// Handle connections in a new goroutine
		go handleServer(conn, &wg, scan, delta)

	}
}
*/
