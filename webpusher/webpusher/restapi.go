package main

import (
	"fmt"
	"github.com/msinev/replicator/jsonjackson"
	"github.com/msinev/replicator/webpusher/reader"
	"net/http"
)

func reply(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	fmt.Fprintf(w, "<HTML><BODY><p>You've directly requested the root of <b>REDIS push server</b>. It is <b>NOT INTENDED USAGE</b>."+
		" Please use intended paths like <a href=\"/data\">/data</a> via "+
		"<a href=\"http://nginx.org\">nginx</a> or similar reverse proxy server with correspondent static content.")
}

func options(w http.ResponseWriter, r *http.Request) {
	//
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()
	ids := r.Form.Get("id")
	filter := r.Form.Get("filter")

	jw := jsonjackson.NewBuilder(w)
	jw.BeginArray()
	jw.Str(ids)
	jw.Str(filter)

	defer jw.CloseAll()
	//
}

func sockets(w http.ResponseWriter, r *http.Request) {
	//
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()
	ids := r.Form.Get("id")

	jw := jsonjackson.NewBuilder(w)
	jw.BeginArray()
	jw.Str(ids)

	defer jw.CloseAll()
	//
}

func delta(w http.ResponseWriter, r *http.Request) {
	//
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()
	ids := r.Form.Get("id")

	jw := jsonjackson.NewBuilder(w)
	jw.BeginArray()
	jw.Str(ids)

	defer jw.CloseAll()
	//
}

func fullCopy(w http.ResponseWriter, r *http.Request, scan []ScanReader, delta []reader.DeltaReceiver) {
	//
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()
	ids := r.Form.Get("id")

	jw := jsonjackson.NewBuilder(w)
	jw.BeginArray()
	jw.Str(ids)

	defer jw.CloseAll()
	//
}
