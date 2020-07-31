package jsonjackson

import (
	"github.com/op/go-logging"
	"io"
	"strconv"
)

type JSONWriter struct {
	w     io.Writer
	close []byte
	next  bool
}

var escS = []byte("\\\\")
var escK = []byte("\\\"")

var preK = []byte("\"")

var preNull = []byte("null")
var preTrue = []byte("true")
var preFalse = []byte("false")

var log = logging.MustGetLogger("JSON.SAX")

func (w *JSONWriter) Write(wb []byte) {
	w.w.Write(wb)
}

func NewBuilder(wb io.Writer) (w *JSONWriter) {
	return &JSONWriter{w: wb, close: nil, next: false}

}

func (w *JSONWriter) Attach(wb io.Writer) {
	w.w = wb
}

func (w *JSONWriter) writeByte(b byte) *JSONWriter {
	var wb [1]byte
	wb[0] = b
	w.Write(wb[:])
	return w
}

func WriteJSONString(w io.Writer, s string) {
	sb := []byte(s)
	b0 := 0
	w.Write(preK)

	for i := 0; i < len(sb); i++ {
		si := sb[i]
		if si == '\\' {
			if b0 < i {
				w.Write(sb[b0:i])
			}
			b0 = i + 1
			w.Write(escS)
		} else if si == '"' {
			if b0 < i {
				w.Write(sb[b0:i])
			}
			b0 = i + 1
			w.Write(escK)
		}
	}

	if b0 < len(sb) {
		w.Write(sb[b0:])
	}
	w.Write(preK)
}

func (w *JSONWriter) writeStr(s string) *JSONWriter {
	WriteJSONString(w.w, s)
	return w
}

func (w *JSONWriter) BeginArray() *JSONWriter {
	return w.checknext(false).push(']').writeByte('[')
}

func (w *JSONWriter) checknext(nb bool) *JSONWriter {
	if w.next {
		w.writeByte(',')
	}
	w.next = nb
	return w
}

func (w *JSONWriter) push(nb byte) *JSONWriter {
	w.close = append(w.close, nb)
	return w
}

func (w *JSONWriter) CloseAll() {
	for i := len(w.close); i > 0; {
		i--
		w.writeByte(w.close[i])
	}
	w.close = nil
	w.next = false // no more shall be written anyway of json format would be broken
}

func (w *JSONWriter) Field(s string) *JSONWriter {
	w.checknext(false).writeStr(s).writeByte(':')
	return w
}

func (w *JSONWriter) EndArray() *JSONWriter {
	w.End()
	return w
}

func (w *JSONWriter) EndObject() *JSONWriter {
	return w.End()
}

func (w *JSONWriter) BeginObject() *JSONWriter {
	w.checknext(false).push('}').writeByte('{')
	return w
}

func (w *JSONWriter) End() *JSONWriter {
	p := len(w.close) - 1
	lc := w.close[p]
	w.close = w.close[:p]
	w.writeByte(lc)
	w.next = true
	return w
}

func (w *JSONWriter) Str(s string) *JSONWriter {
	return w.checknext(true).writeStr(s)
}

func (w *JSONWriter) Bool(s bool) *JSONWriter {
	w.checknext(true)
	if s {
		w.Write(preTrue)
	} else {
		w.Write(preFalse)
	}
	return w

}

func (w *JSONWriter) Nul() *JSONWriter {
	w.checknext(true).Write(preNull)
	return w
}

func (w *JSONWriter) NulStr(s *string) *JSONWriter {
	if s == nil {
		w.Nul()
	} else {
		w.Str(*s)
	}
	return w
}

func (w *JSONWriter) Int(i int64) *JSONWriter {
	w.checknext(true).Write([]byte(strconv.FormatInt(i, 10)))
	return w
}

func (w *JSONWriter) Float(i float64) *JSONWriter {
	w.checknext(true).Write([]byte(strconv.FormatFloat(i, 'G', -1, 64)))
	return w
}

func (w *JSONWriter) BeginArrayField(s string) *JSONWriter {
	return w.Field(s).BeginArray()
}

// convenience methods

func (w *JSONWriter) BeginObjectField(s string) *JSONWriter {
	return w.Field(s).BeginObject()
}

func (w *JSONWriter) IntField(s string, i int64) *JSONWriter {
	return w.Field(s).Int(i)
}

func (w *JSONWriter) StrField(s string, v string) *JSONWriter {
	return w.Field(s).Str(v)
}

func (w *JSONWriter) BoolField(s string, b bool) *JSONWriter {
	return w.Field(s).Bool(b)
}
