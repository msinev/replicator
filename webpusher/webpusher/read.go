package main

import (
	"github.com/gorilla/websocket"
	"github.com/msinev/replicator/webpusher/reader"
	"sync"
	"time"
)

type ClientStats struct {
	Keys  []int64
	Bytes []int64
}

type WebClient struct {
	// static global options
	Databases     []reader.ServerOptions
	ScanReader    []ScanReader
	DeltaReceiver []reader.DeltaReceiver
	DBIndex       map[int]int
	//
	//Readers   []chan<- *DrainRequest
	//Versions  []int64
	SESSID string

	//Info      map[string]string
	//Handshake map[string]string

	TerminateDone sync.Once
	//TerminateChain func()

	TSStart time.Time
	Filter  string

	ConnWS *websocket.Conn
	//StopChan <-chan int

	//  static pipeline - no need to keep just for debugging
	//-- remove channels from client's structure after debugging
	//	KVFullScan   []chan []Reader.PKVData
	//KVPartSink []chan reader.VersionData

	Drains     []chan *DrainRequest
	ProcessAPI chan *SyncRequest

	//
	//VerSyncAlert []chan int64

	//	watchers
	// Control
	Finished sync.WaitGroup
	//Control  chan control.ControlMessage

	Alive chan int // never being sent just close on quit

	TSSynced       []time.Time
	TSLatestUpdate []time.Time
	//	SyncVolume []int64
	Stats ClientStats
}

func (client *WebClient) DrainProcess() {
	ldrains := len(client.Drains)
	crq := make(chan *reader.VersionData, ldrains)
	for rq := range client.ProcessAPI {
		sendNext(client, rq, crq)
	}
}

func (client *WebClient) Done() {
	defer close(client.Alive)
	defer close(client.ProcessAPI)
}

func (client *WebClient) Init() {
	client.Alive = make(chan int)
	client.Drains = make([]chan *DrainRequest, len(client.Databases))
	go client.DrainProcess()

	for rk, rv := range client.Databases {
		//	if (DBSPlain[rv] == "+") {
		log.Debugf("Starting DB %d -> chan", rv)
		//req := client.ScanReader[rk].data
		//controlStop := make(chan int64)
		stage2 := make(chan ScanRequest)
		stage2a := client.DeltaReceiver[rk].SubscribeVersions(client.SESSID) // Subscribe to updates

		//		client.KVFullScan[rk] = stage1   // for debug
		//client.KVPartSink[rk] = stage2   // for debug
		//client.MsgSink[rk] = stage3      // for debug
		//client.DataBreakers[rk] = stage4 // for debug
		//client.BlockDrains[rk] = stage5  // for debug
		//qosDrains[rk] = stage5
		/*
		   func KVMerger(db int, stop <-chan int64, outrqc chan<- <-chan Reader.VersionData,
		   	ind chan Reader.VersionData, outc chan Compressor.CompressableData) {
		   			rqVersion
		*/
		//rqVersion := int64(0)
		KVPullMerger(rv.DB, client.Alive, stage2, stage2a, client.Drains[rk], client.SESSID)

		//go Reader.Scan(rv, stage2, &client.DBReader) moved to Init
		//		go Reader.KVScanAccumulator(rk, stage1,  stage2)

		//go clientserver.ServerDataCompressor(client, rk, GZIPCompression, stage3, stage4)
		//go clientserver.ServerDataStreaming(client, rk, stage4, stage5)
		//		} else {

		//	}
		//		go scan(rv, client.kvPartSink[rk])

	}

}
