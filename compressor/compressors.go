package compressor

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"github.com/cxuhua/lzma"
)

func GZIPcompressor(inp []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(inp)
	w.Close()
	bout := b.Bytes()
	log.Debug("GZIP Comperessed " + hex.EncodeToString(bout[:10]) + "...")
	return bout
}

func LZMAcompressor(inp []byte) []byte {

	u, e := lzma.Compress(inp)
	if e != nil {
		log.Panic(e)
		return nil
	}
	log.Debug("LZMA Comperessed " + hex.EncodeToString(u[:10]) + "...")
	return u
}

func Zerocompressor(in []byte) []byte {
	log.Debug("STUB Comperessed " + hex.EncodeToString(in[:10]) + "...")
	return in
}

var Compressors = [...]func([]byte) []byte{
	Zerocompressor,
	LZMAcompressor,
	GZIPcompressor,
}
