package main

import (
	"github.com/msinev/replicator/jsonjackson"
	"github.com/msinev/replicator/webpusher/reader"
)

func writeJSONKV(msg reader.RedisKV, wrt *jsonjackson.JSONWriter) {
	msg.Write(wrt)

}
