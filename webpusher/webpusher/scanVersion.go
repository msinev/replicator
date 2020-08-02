package main

import (
	"github.com/msinev/replicator/protobuf/sendrecv"
	"io"
	//	"bytes"
	//	"encoding/binary"
	"github.com/golang/protobuf/proto"
	//	"github.com/gogo/protobuf/plugin/size"
	//	"log"
	"encoding/binary"
	"strconv"
)

func writeJSONKV(msg KVData, wrtr io.Writer) {

	var size uint64

	data, err := proto.Marshal(msg)

	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	size = uint64(len(data))
	wdata := make([]byte, 8)
	wsize := binary.PutUvarint(wdata, size)

	wrtr.Write(wdata[0:wsize])
	wrtr.Write(data)
}
