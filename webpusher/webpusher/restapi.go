package main

import (
	"fmt"
	"github.com/msinev/replicator/webpusher/reader"
	"net/http"
)

func reply(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<HTML><BODY><p>You've directly requested the root of <b>REDIS push server</b>. It is <b>NOT INTENDED USAGE</b>."+
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

func fullCopy(w http.ResponseWriter, r *http.Request, scan []ScanReader, delta []reader.DeltaReceiver) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "[]")
}
