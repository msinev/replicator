package Compressor

import (
	"time"
	"fmt"
	"encoding/binary"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("REPA")


type CompressableData struct {
	Datatype uint16
	Data     []byte
}

type CompressedData struct {
	Header InnerHeader
	Data   []byte
}

const CommHeaderSize=16
const InnerHeaderSize=32

type TheMessage  []byte

type TheHeader struct {
	//
	BlockSize uint16 //0
	//
	Queue    uint8  // 2
	Reserved uint8  // 3
	Msg      uint32 // 4
	//
	Offset   uint32 // 8
	RestSize uint32 // 12
}

func (b TheMessage) String() string {
	p:=GetHeader(b)
	return fmt.Sprintf("{Pipe:%d, Size:%d/%d, At:%d}", p.Queue, p.BlockSize, p.RestSize, p.Offset)
}


func GetHeader(msg []byte) TheHeader {
	return TheHeader{
		BlockSize: binary.LittleEndian.Uint16(msg),
		Queue:     uint8(msg[2]),
		Reserved:  uint8(msg[3]),
		Msg:       binary.LittleEndian.Uint32(msg[4:]),
		Offset:    binary.LittleEndian.Uint32(msg[8:]),
		RestSize:  binary.LittleEndian.Uint32(msg[12:]),
	}
}

func SetHeader(block []byte, header TheHeader)  {
	binary.LittleEndian.PutUint16( block, uint16(header.BlockSize) )
	block[2]=byte(header.Queue)
	block[3]=byte(header.Reserved)
	binary.LittleEndian.PutUint32( block[4:], uint32(header.Msg) )
	binary.LittleEndian.PutUint32( block[8:], uint32(header.Offset) )
	binary.LittleEndian.PutUint32( block[12:], uint32(header.RestSize) )
}


type InnerHeader struct {
	Compression uint16 // 0
	//
	Datatype uint16 // 2
	//
	Uncompressed uint32 // 4
	HASH         uint64 // 8
	TS           uint64 // 16
	Reserved2    uint64 // 24
}

func JavaTSNow() uint64 {
	return uint64(time.Now().UnixNano()) / uint64(time.Millisecond)
}


func GetInnerHeader(msg []byte) (InnerHeader, []byte) {

	return InnerHeader{
		Compression:  binary.LittleEndian.Uint16(msg),
		Datatype:     binary.LittleEndian.Uint16(msg[2:]),
		Uncompressed: binary.LittleEndian.Uint32(msg[4:]),
		HASH:         binary.LittleEndian.Uint64(msg[8:]),
		TS:           binary.LittleEndian.Uint64(msg[16:]),
		Reserved2:    binary.LittleEndian.Uint64(msg[24:]),
	}, msg[InnerHeaderSize:]

}


func GetInnerCompression(msg []byte) uint16 {
	return binary.LittleEndian.Uint16(msg)
}

func SetInnerHeader(block []byte, header InnerHeader)  {
	binary.LittleEndian.PutUint16( block, header.Compression)
	binary.LittleEndian.PutUint16( block[2:], header.Datatype)
	binary.LittleEndian.PutUint32( block[4:], header.Uncompressed)
	binary.LittleEndian.PutUint64( block[8:], header.HASH )
	binary.LittleEndian.PutUint64( block[16:], header.TS )
	binary.LittleEndian.PutUint64( block[24:], header.Reserved2)
}

const CommDataBlockSize=4096


