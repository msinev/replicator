package Compressor

import (
	"bytes"
	"compress/gzip"
	"github.com/cxuhua/lzma"
	"encoding/hex"
)


func GZIPCompressor(inp []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(inp)
	w.Close()
	bout:=b.Bytes()
	log.Debug("GZIP Comperessed "+ hex.EncodeToString( bout[:10])+"..." )
	return bout
}

func LZMACompressor(inp []byte) []byte {

	u, e := lzma.Compress(inp)
	if (e != nil) {
		log.Panic(e)
		return nil
	}
	log.Debug("LZMA Comperessed "+ hex.EncodeToString( u[:10])+"..." )
	return u
}

func ZeroCompressor(in []byte) []byte {
	log.Debug("STUB Comperessed "+ hex.EncodeToString( in[:10])+"..." )
	return in
}

var Compressors = [...]func([]byte) []byte{
	ZeroCompressor,
	LZMACompressor,
	GZIPCompressor,
}
