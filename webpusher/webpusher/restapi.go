package main

import (
	"fmt"
	"github.com/codebear4/ttlcache"
	"github.com/msinev/replicator/webpusher/reader"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

var ClientCache ttlcache.Cache

func initTTLCache() {
	/*	newItemCallback := func(key string, value interface{}) {
		fmt.Printf("New key(%s) added\n", key)
	}*/
	/*
		checkExpirationCallback := func(key string, value interface{}) bool {
			if key == "key1" {
				// if the key equals "key1", the value
				// will not be allowed to expire
				return false
			}
			// all other values are allowed to expire
			return true
		}
	*/

	expirationCallback := func(key string, value interface{}) {
		fmt.Printf("Sesssion key(%s) has expired\n", key)
		close(value.(WebClient).Alive)
	}

	cache := ttlcache.NewCache()
	cache.SetTTL(time.Duration(90 * time.Second))
	cache.SetExpirationCallback(expirationCallback)

	cache.Set("key", "value")
	cache.SetWithTTL("keyWithTTL", "value", 10*time.Second)

	//	value, exists := cache.Get("key")
	//	count := cache.Count()
	//	result := cache.Remove("key")
}

var letterRunes = []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var rnd rand.Rand

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rnd.Intn(len(letterRunes))]
	}
	return string(b)
}

func init() {
	initTTLCache()
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

func reply(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	fmt.Fprintf(w, "<HTML><BODY><p>You've directly requested the root of <b>REDIS push server</b>. It is <b>NOT INTENDED USAGE</b>."+
		" Please use intended paths like <a href=\"/data\">/data</a> via "+
		"<a href=\"http://nginx.org\">nginx</a> or similar reverse proxy server with correspondent static content.")
}

func options(w http.ResponseWriter, r *http.Request) {
	//
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("Not supported yet"))
}

func sockets(w http.ResponseWriter, r *http.Request) {
	//
	r.ParseForm()
	ids := r.Form.Get("id")
	c, exist := ClientCache.Get(ids)
	if exist {
		se := &SyncRequest{}
		se.Release.Add(1)
		select {
		case c.(WebClient).ProcessAPI <- se:
			se.Release.Wait()
		default:
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte("Session ID " + ids + " already serving request wait or close"))
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Session ID " + ids + " not found"))
	}
	//
}

func getDelta(w http.ResponseWriter, r *http.Request) {
	//
	r.ParseForm()
	ids := r.Form.Get("id")

	c, exist := ClientCache.Get(ids)
	if exist {

		se := &SyncRequest{}
		se.Release.Add(1)

		select {
		case c.(WebClient).ProcessAPI <- se:
			se.Release.Wait()
		default:
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte("Session ID " + ids + " already serving request wait or close"))
		}

	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Session ID " + ids + " not found"))
	}
	/*
		jw := jsonjackson.NewBuilder(w)
		jw.BeginArray()
		jw.Str(ids)

		defer jw.CloseAll()

	*/
	//
}

func startSession(w http.ResponseWriter, r *http.Request) {
	//
	r.ParseForm()
	r.ParseForm()
	ids := RandStringRunes(16)
	log.Infof("New session %s started %s")
	newWC := &WebClient{
		Databases:      nil,
		Readers:        nil,
		Versions:       nil,
		SESSID:         ids,
		TerminateDone:  sync.Once{},
		TSStart:        time.Now(),
		Filter:         "",
		ConnWS:         nil,
		ProcessAPI:     nil,
		KVPartSink:     nil,
		Finished:       sync.WaitGroup{},
		Control:        nil,
		Alive:          nil,
		TSSynced:       nil,
		TSLatestUpdate: nil,
		Stats:          ClientStats{},
	}
	newWC.Init()
	ClientCache.Set(ids, newWC)

	se := &SyncRequest{}
	se.Release.Add(1)
	select {
	case newWC.ProcessAPI <- se:
		se.Release.Wait()
	default:
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("Session ID " + ids + " already serving request wait or close"))
	}
	//

	/*
		jw := jsonjackson.NewBuilder(w)
		jw.BeginArray()
		jw.Str(ids)

		defer jw.CloseAll()

	*/
	//
}

func closeClient(w http.ResponseWriter, r *http.Request, scan []ScanReader) {
	//
	r.ParseForm()
	ids := r.Form.Get("id")
	c, exist := ClientCache.Get(ids)
	if exist {
		close(c.(WebClient).ProcessAPI)
		ClientCache.Remove(ids)
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Session ID " + ids + " not found"))
	}
}

func fullCopy(w http.ResponseWriter, r *http.Request, scan []ScanReader, delta []reader.DeltaReceiver) {
	//
	r.ParseForm()
	ids := r.Form.Get("id")
	c, exist := ClientCache.Get(ids)
	if exist {
		se := &SyncRequest{}
		se.Release.Add(1)
		select {
		case c.(WebClient).ProcessAPI <- se:
			se.Release.Wait()
		default:
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte("Session ID " + ids + " already serving request wait or close"))
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Session ID " + ids + " not found"))
	}

	//
}
