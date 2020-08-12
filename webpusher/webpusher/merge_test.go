package main

import (
	"bytes"
	"github.com/msinev/replicator/jsonjackson"
	"github.com/msinev/replicator/webpusher/reader"
	"testing"
)

func pval(s string) *string {
	return &s
}

func TestJsonString(t *testing.T) {

	v := &reader.VersionData{
		DB:       0,
		Version:  10,
		DeltaFor: 0,
		VersionData: []reader.PKVData{&reader.KVData{
			Key:       "K1",
			Value:     pval("V1"),
			ListValue: nil,
			ListKeys:  nil,
			TTL:       nil,
		}},
	}

	bbuf := new(bytes.Buffer)
	jw := jsonjackson.NewBuilder(bbuf)
	jw.BeginArray()
	writeJSONKV(v, jw)
	jw.EndArray()
	//JSONVersionData{}
}
