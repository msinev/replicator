package main

import (
	"github.com/msinev/ML/pushweb/reader"
	"github.com/msinev/replicator/control"
	"sync"
	"time"
)

type ClientStats struct {
	Keys  []int64
	Bytes []int64
}

type WebClient struct {
	// static
	Databases []int
	Versions  []int64
	SESSID    string

	Info      map[string]string
	Handshake map[string]string

	terminateDo    sync.Once
	TerminateChain func()

	TSStart time.Time

	//  static pipeline - no need to keep just for debugging
	//-- remove channels from client's structure after debugging
	//	KVFullScan   []chan []Reader.RedisKV
	KVPartSink []chan reader.VersionData

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
	Stats ClientStats
}