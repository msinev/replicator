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

type ProtobufReader interface {
	io.Reader
	io.ByteReader
}

func writePB(msg *sendrecv.Msg, wrtr io.Writer) {

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

func readPB(rdr ProtobufReader) *sendrecv.Msg {
	msg := new(sendrecv.Msg)
	size, err := binary.ReadUvarint(rdr)
	if err != nil {
		log.Panic(err)
	}

	data := make([]byte, size)
	//rdr.Read(data)

	actual, err := io.ReadFull(rdr, data)
	if actual != int(size) {
		log.Fatal("Error reading buffer " + strconv.Itoa(actual) + " != " + strconv.Itoa(int(size)))
		if err != nil {
			log.Panic(err)
		}
	}
	proto.Unmarshal(data, msg)
	return msg
}

/*

import (
	"github.com/garyburd/redigo/redis"
	"github.com/go-kit/kit/util/conn"
	"fmt"
)

func version() {
	// here we'll store our iterator value
	iter := 0

	// this will store the keys of each iteration
	var keys []string
	for {

		// we scan with our iter offset, starting at 0
		if arr, err := redis.MultiBulk(conn.Do("SCAN", iter)); err != nil {
			panic(err)
		} else {

			// now we get the iter and the keys from the multi-bulk reply
			iter, _ = redis.Int(arr[0], nil)
			keys, _ = redis.Strings(arr[1], nil)
		}

		fmt.Println(keys)

		// check if we need to stop...
		if iter == 0 {
			break
		}
	}
}

*/
