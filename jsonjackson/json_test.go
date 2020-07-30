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
