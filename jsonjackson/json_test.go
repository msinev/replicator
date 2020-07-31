package jsonjackson

import (
	"bytes"
	"testing"
)

func TestJsonString(t *testing.T) {

	cases := map[string]string{
		"hello world": "\"hello world\"",
		"hel\"lo":     "\"hel\\\"lo\"",
		"hel\\\"lo":   "\"hel\\\\\\\"lo\"",
		"hel\\i\"lo":  "\"hel\\\\i\\\"lo\"",
	}

	for k, v := range cases {
		var b bytes.Buffer

		WriteJSONString(&b, k)
		r := b.String()
		if r != v {
			t.Logf("%s not equal expected %s\n", r, v)
			t.Fail()
		} else {
			t.Logf("Pass %s to %s\n", k, v)
		}

	}
}
func TestJsonBuild1(t *testing.T) {
	var b bytes.Buffer
	jw := NewBuilder(&b)
	jw.BeginObject().Field("X").Int(10).Field("Y").Str("Z").BeginArrayField("A1").Int(1).Int(2)

	jw.CloseAll()
	v := "{\"X\":10,\"Y\":\"Z\",\"A1\":[1,2]}"
	r := b.String()
	if r != v {
		t.Logf("%s not equal expected %s\n", r, v)
		t.Fail()
	} else {
		t.Logf("Pass  %s\n", v)
	}
}

func TestJsonBuild2(t *testing.T) {
	var b bytes.Buffer
	jw := NewBuilder(&b)
	jw.BeginObject().
		IntField("X", 10).
		StrField("Y", "Z").
		BeginArrayField("A1").Int(1).Int(2).End().
		BeginArrayField("A2").
		BeginArray().Int(3).Int(1).Float(1.1).End().
		BeginArray().Int(1).Int(2).Float(7.2).End().
		End()

	jw.CloseAll()
	r := b.String()
	v := "{\"X\":10,\"Y\":\"Z\",\"A1\":[1,2],\"A2\":[[3,1,1.1],[1,2,7.2]]}"
	if r != v {
		t.Logf("%s not equal expected %s\n", r, v)
		t.Fail()
	} else {
		t.Logf("Pass  %s\n", v)
	}
}

func TestJsonBuild3(t *testing.T) {
	var b bytes.Buffer
	jw := NewBuilder(&b)
	jw.BeginObject().
		BoolField("X", false).
		StrField("Y", "Z").
		BeginArrayField("A1").Bool(true).Bool(false).End().
		BeginArrayField("A2").
		BeginArray().Int(3).Int(1).Float(1.1).End().
		BeginArray().Int(1).Int(2).Float(7.2).End().
		End()

	jw.CloseAll()
	r := b.String()
	v := "{\"X\":false,\"Y\":\"Z\",\"A1\":[true,false],\"A2\":[[3,1,1.1],[1,2,7.2]]}"
	if r != v {
		t.Logf("%s not equal expected %s\n", r, v)
		t.Fail()
	} else {
		t.Logf("Pass  %s\n", v)
	}
}
