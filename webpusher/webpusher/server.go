package main

import (
	"bufio"
	"encoding/json"
	"github.com/cespare/xxhash"
	"github.com/gorilla/websocket"
	"github.com/msinev/replicator/compressor"
	"github.com/msinev/replicator/control"
	"github.com/msinev/replicator/webpusher/reader"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClientStatsBin struct {
	Produced   []int64
	Compressed []int64
}

func (client *ClientStatsBin) Init(ldbs int) {
	//	client.SyncVolume = make([]int64, ldbs)

	client.Produced = make([]int64, ldbs)
	client.Compressed = make([]int64, ldbs)
	//	client.MSLatency = make([]int64, ldbs)

}

type Client struct {
	// static
	Databases []int
	Versions  []int64
	Conn      websocket.Conn
	Reader    *bufio.Reader
	Conntype  string
	ID        string

	//Info           map[string]string
	Handshake      map[string]string
	terminateDo    sync.Once
	TerminateChain func()

	TSStart time.Time

	//  static pipeline - no need to keep just for debugging
	//-- remove channels from client's structure after debugging
	//	KVFullScan   []chan []Reader.RedisKV
	KVPartSink []chan reader.VersionData
	//MsgSink     []chan compressor.CompressableData
	//BlockDrains []chan compressor.TheMessage

	//SocketDrain <-chan compressor.TheMessage
	//
	//VerSyncAlert []chan int64

	//	watchers
	// Control
	Finished sync.WaitGroup
	Control  chan control.ControlMessage
	Alive    chan int // never being sent just close on quit

	TSSynced       []time.Time
	TSLatestUpdate []time.Time
	//	SyncVolume []int64
	Stats ClientStatsBin
}

func (client *Client) DoneMsg() {
	client.Control <- control.ControlMessage{Message: control.DONE}
}

const VersionsHeader = "versions"

func (client *Client) ParseHeader(line string) {
	i := strings.Index(line, ":")
	if i > 0 {
		header := line[:i]
		hval := line[i+1:]
		log.Infof("Header  %s -> %s", header, hval)
		if strings.ToLower(header) == VersionsHeader {
			vs := strings.Split(hval, ",")
			client.Versions = make([]int64, len(vs))
			for k, v := range vs {
				if len(v) > 0 {
					i, err := strconv.ParseInt(v, 10, 64)
					if err == nil {
						client.Versions[k] = i
					}
				}
			}
		} else {
			log.Errorf("Wrong header %s", header)
		}
	} else {
		log.Errorf("Wrong header line %s", line)
	}
}

func (client *Client) Init(ldbs int) {
	client.Finished.Add(1)

	client.Control = make(chan control.ControlMessage)
	client.Alive = make(chan int)

	//client.DBReader.Add(ldbs)
	//client.update.RLock()
	client.TSStart = time.Now()

	// counters
	client.TSSynced = make([]time.Time, ldbs)
	client.TSLatestUpdate = make([]time.Time, ldbs)
	//	tsLatestUpdate	[]time.Time
	client.Stats.Init(ldbs)
	//

	//client.KVFullScan = make([]chan []Reader.RedisKV, ldbs)
	client.KVPartSink = make([]chan reader.VersionData, ldbs)
	//client.MsgSink = make([]chan compressor.CompressableData, ldbs)
	//client.DataBreakers = make([]chan compressor.CompressedData, ldbs)
	//client.BlockDrains = make([]chan compressor.TheMessage, ldbs)
	client.TerminateChain = func() {
		log.Info("Client %s terminating", client.ID)
		close(client.Alive)
		client.Finished.Done()
		log.Info("Client %s terminated", client.ID)
	}
}

func (client *Client) Terminate() {
	log.Info("Client %s termimate chain Do", client.ID)
	client.terminateDo.Do(client.TerminateChain)
}

func ServerDataCompressor(client *Client, pipe int, compression int,
	inCh <-chan compressor.CompressableData, outCh chan<- compressor.CompressedData) {

	defer log.Infof("ServerCompressionRouting %d %d Quiting", int(pipe), int(compression))
	defer close(outCh)

	for inmsg := range inCh {
		cd := compressor.CompressedData{
			Header: compressor.InnerHeader{
				Compression:  uint16(compression),
				Datatype:     inmsg.Datatype,
				Uncompressed: uint32(len(inmsg.Data)),
				TS:           compressor.JavaTSNow(),
				HASH:         xxhash.Sum64(inmsg.Data),
			},
			Data: compressor.Compressors[compression](inmsg.Data),
		}
		atomic.AddInt64(&client.Stats.Produced[pipe], int64(len(inmsg.Data)))
		atomic.AddInt64(&client.Stats.Compressed[pipe], int64(len(cd.Data)))

		b, err := json.Marshal(cd.Header)
		if err != nil {
			log.Debug(err)
		} else {
			log.Debugf("CompessorHeader: %s", string(b))
		}
		log.Noticef("compressor Pipe %d %d -> %d (%.2f)", pipe, client.Stats.Produced[pipe],
			client.Stats.Compressed[pipe], 100.0*float64(client.Stats.Compressed[pipe])/float64(client.Stats.Produced[pipe]))
		outCh <- cd
	}
}

func ServerDataStreaming(client *Client, pipe int, inCh <-chan compressor.CompressedData, outCh chan<- compressor.TheMessage) {
	msgN := uint32(0)
	defer log.Infof("DataStreaming Quiting pipe %d", pipe)
	defer close(outCh)
	for msg := range inCh {
		ldata := uint32(len(msg.Data))
		lall := compressor.InnerHeaderSize + ldata

		offset := uint32(0)
		dataoffset := uint32(0)

		msgNum := uint32(0)

		for offset < lall {

			restSize := lall - offset
			currentBlock := uint32(compressor.CommDataBlockSize)

			if restSize <= compressor.CommDataBlockSize {
				currentBlock = restSize
			}

			block := make([]byte, currentBlock+compressor.CommHeaderSize)
			var dataload uint32

			h := compressor.TheHeader{
				BlockSize: uint16(currentBlock),
				Queue:     uint8(pipe),
				Msg:       msgNum,
				Offset:    offset,
				RestSize:  restSize,
			}

			if offset == 0 {
				dataload = currentBlock - compressor.InnerHeaderSize

				copy(block[compressor.CommHeaderSize+compressor.InnerHeaderSize:], msg.Data[dataoffset:dataoffset+dataload])
				compressor.SetInnerHeader(block[compressor.CommHeaderSize:], msg.Header)
			} else {
				dataload = currentBlock
				copy(block[compressor.CommHeaderSize:], msg.Data[dataoffset:dataoffset+uint32(currentBlock)])
			}

			compressor.SetHeader(block, h)

			offset += currentBlock
			dataoffset += dataload
			log.Noticef("Sending pipe %d %d bytes data block", pipe, dataload)
			outCh <- block
		}
		msgN++
	}
}
