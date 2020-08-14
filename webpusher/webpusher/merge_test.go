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

	ttl1 := uint32(1)
	ttl := uint32(1000)
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
		}, &reader.KVData{
			Key:       "K2",
			Value:     pval("V2"),
			ListValue: nil,
			ListKeys:  nil,
			TTL:       &ttl1,
		}},
	}

	v2 := &reader.VersionData{
		DB:       10,
		Version:  11,
		DeltaFor: 0,
		VersionData: []reader.PKVData{&reader.KVData{
			Key:       "K2",
			Value:     pval("V2-2"),
			ListValue: nil,
			ListKeys:  nil,
			TTL:       &ttl,
		}, &reader.KVData{
			Key:       "K3",
			Value:     pval("V3-2"),
			ListValue: nil,
			ListKeys:  nil,
		}},
	}

	bbuf := new(bytes.Buffer)

	jw := jsonjackson.NewBuilder(bbuf)
	jw.BeginArray()
	writeJSONKV(v, jw)
	jw.EndArray()

	cv1 := bbuf.String()
	if cv1 != "[{\"k\":\"K1\",\"v\":\"V1\"},{\"k\":\"K2\",\"v\":\"V2\",\"t\":1}]" {
		t.Fail()
		t.Logf("Failed V1 - : %s", cv1)
	}

	bbuf.Reset()
	jw.Reset()

	jw.BeginArray()
	writeJSONKV(v2, jw)
	jw.EndArray()

	cv2 := bbuf.String()
	ccv2 := "[{\"k\":\"K2\",\"v\":\"V2-2\",\"t\":1000},{\"k\":\"K3\",\"v\":\"V3-2\"}]"
	if cv2 != ccv2 {
		t.Fail()
		t.Logf("Failed V2 - : %s != %s", cv2, ccv2)
	}

	v3, mv := mergeDeltas(v2, v, 10)
	if mv != 11 {
		t.Fail()
		t.Log("Failed vers")
	}
	bbuf.Reset()
	jw.Reset()

	jw.BeginArray()
	writeJSONKV(v3, jw)
	jw.EndArray()

	cv3 := bbuf.String()
	ccv3 := "[{\"k\":\"K1\",\"v\":\"V1\"},{\"k\":\"K2\",\"v\":\"V2-2\",\"t\":1000},{\"k\":\"K3\",\"v\":\"V3-2\"}]"
	if cv3 != ccv3 {
		t.Fail()
		t.Logf("Failed V3 - : %s != %s", cv3, ccv3)
	}
	if !t.Failed() {
		t.Logf("\n%s\n +\n%s\n-----------------------\n%s", cv1, cv2, cv3)
	}

	//	t.Logf("V 2 - : %s", cv2)
	//JSONVersionData{}
}
