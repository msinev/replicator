package jsonjackson

import "io"

type JSONWriter struct {
	w     io.Writer
	close []byte
	next  bool
}

var escS = []byte("\\\\")
var escK = []byte("\\\"")

var preK = []byte("\"")

func (w *JSONWriter) Write(wb []byte) {
	w.Write(wb)
}

func (w *JSONWriter) WriteByte(b byte) {
	var wb [1]byte
	wb[0] = b
	w.Write(wb[:])
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

func (w *JSONWriter) getOpen() {

}

func (w *JSONWriter) assertOpen(c byte) *JSONWriter {

	return w
}

func (w *JSONWriter) BeginArray() *JSONWriter {
	return w
}

func (w *JSONWriter) EndArray() *JSONWriter {
	return w.End()
}

func (w *JSONWriter) EndObject() *JSONWriter {
	return w.End()
}

func (w *JSONWriter) BeginObject() *JSONWriter {
	return w
}

func (w *JSONWriter) End() *JSONWriter {
	return w
}

func (w *JSONWriter) Str(s string) *JSONWriter {
	return w
}

func (w *JSONWriter) Nul() *JSONWriter {
	return w
}

func (w *JSONWriter) NulStr(s string) *JSONWriter {
	return w
}

func (w *JSONWriter) Int(i int) *JSONWriter {
	return w
}

func (w *JSONWriter) BeginArrayField(s string) *JSONWriter {
	if w.next {
		w.WriteByte(',')
	}

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
