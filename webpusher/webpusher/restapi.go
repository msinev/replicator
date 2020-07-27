package main

import (
	"fmt"
	"net/http"
)

func reply(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<HTML><BODY><p>You've directly requested the root of <b>REDIS push server</b>. It is <b>NOT INTENDED USAGE</b>. Please use intended paths like "+
		"<a href=\"/libility\">/liability</a> or <a href=\"/data\">/data</a> via "+
		"<a href=\"http://nginx.org\">nginx</a> or similar reverse proxy server with correspondent static content.")

}

func full(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "[]")
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
