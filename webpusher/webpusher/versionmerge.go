package main

import (
	"bytes"

	"github.com/msinev/replicator/compressor"
	"github.com/msinev/replicator/webpusher/reader"
	"sort"
	"time"
)

const initialSendScan,
	initialWaitScan,
	initialWaitScanWithDeltas,
	activeSendWaitDeltas,
	activeWaitDeltas,
	//
	//
	brokenFuseScan,
	brokenInitialScanDelta,
	brokenInitialScanDeltaRecovery,
	stopWaitScan int = 0, 2, 3, 4, 5,
	19, 20, 21, 55

//
//

var stateNames = [...]string{"initialSendScan",
	"initialWaitScan",
	"initialWaitScanWithDeltas",
	"activeSendWaitDeltas",
	"activeWaitDeltas",
	"brokenFuseScan",
	"brokenInitialScanDelta",
	"brokenInitialScanDeltaRecovery",
	"stopWaitScan"}

var stateIDs = [...]int{0, 2, 3, 4, 5, 19, 20, 21, 55}

var stateMap map[int]string

func init() {
	stateMap = make(map[int]string, len(stateIDs))
	for k, v := range stateIDs {
		stateMap[v] = stateNames[k]
	}
}

//
//
// Mytype must implement Less func.
func InitVersionCollection(f reader.VersionData) []reader.VersionData {
	return append([]reader.VersionData(nil), f)
}

func ResetVersionCollection(v []reader.VersionData) []reader.VersionData {
	log.Debugf("Resetting version collection of %d elemets", len(v))
	return make([]reader.VersionData, 0, 3)
}

func AppendVersionWithSort(s []reader.VersionData, f reader.VersionData) []reader.VersionData {
	l := len(s)
	if l == 0 {
		return append([]reader.VersionData(nil), f)
	}

	if s[l-1].Version <= f.Version { // new value is the biggest
		return append(s[0:l], f)
	}

	i := sort.Search(l, func(i int) bool { return s[i].Version < f.Version })

	if i == l { // not found = new value is the smallest
		return append(append([]reader.VersionData(nil), f), s...)
	}

	if i == l-1 { // new value is the biggest ???
		log.Error("Might be smthg not right... should never get here!")
		return append(s[0:l], f)
	}

	return append(append(s[0:i], f), s[i+1:]...)
}

//mergeData
//vdata reader.VersionData
type JSONVersionData struct {
	Version  uint64
	DeltaFor uint64
	JSONData []byte
}

func getCompressableSnapshot(vdata reader.VersionData, isent uint64) (*JSONVersionData, uint64) {
	if vdata.Version <= isent {
		return nil, isent
	}

	kvbuf := make([]RedisKV, 0, 20000)

	for _, c := range vdata.VersionData {
		kvbuf = append(kvbuf, c)
	}

	bbuf := new(bytes.Buffer)
	writeJSONKV(&vMsg, bbuf)

	log.Infof("Uncompressed length %d!", bbuf.Len())

	return &JSONVersionData{
		Version:  0,
		DeltaFor: 1,
		JSONData: bbuf.Bytes(),
	}, vdata.Version
}

var TFALSE = false

func getCompressableDelta(deltas []reader.VersionData,
	prev *compressor.CompressableData,
	isent uint64, iprepared uint64) (*compressor.CompressableData,
	uint64,
	[]reader.VersionData) {

	log.Debugf("getCompressableDelta data length %d (%d -> %d)", len(deltas), isent, iprepared)
	sent := isent

	// TODO: make it merge deltas for optimisation

	istart := -1
	for i, c := range deltas {
		if sent > 0 && c.Version <= sent {
			continue
		}

		if istart < 0 || c.DeltaFor == 0 {
			istart = i
			sent = c.Version
		}
	}

	if istart < 0 || len(deltas) <= istart-1 {
		log.Critical("empty data set for sending - possibly internal algorithm error")
		return prev, iprepared, deltas
	}

	actualRange := deltas[istart:]

	if len(actualRange) == 1 {
		kvbuf := make([]*sendrecv.Msg_SendValues, 0, len(actualRange[0].VersionData))
		for _, c := range actualRange[0].VersionData {
			kvbuf = append(kvbuf, c)
		}
		var vMsg sendrecv.Msg

		if actualRange[0].DeltaFor == 0 {
			vMsg = sendrecv.Msg{Vals: kvbuf}
		} else {
			vMsg = sendrecv.Msg{Vals: kvbuf, Reset_: &TFALSE}
		}

		bbuf := new(bytes.Buffer)
		writePB(&vMsg, bbuf)
		log.Debugf("Uncompressed length %d for %d keys in a group", bbuf.Len(), len(kvbuf))

		return &compressor.CompressableData{
			Datatype: 0,
			Data:     bbuf.Bytes(),
		}, actualRange[0].Version, deltas
	}

	log.Infof("Merging %d deltas", len(actualRange))
	mmap := make(map[string]*sendrecv.Msg_SendValues)

	var version uint64
	plainsum := 0
	for _, arv := range actualRange {
		version = arv.Version
		plainsum = plainsum + len(arv.VersionData)
		for _, c := range arv.VersionData {
			mmap[*c.Key] = c
		}
	}

	kvbuf := make([]*sendrecv.Msg_SendValues, 0, len(mmap))

	for _, c := range mmap {
		kvbuf = append(kvbuf, c)
	}

	vMsg := sendrecv.Msg{Vals: kvbuf}
	bbuf := new(bytes.Buffer)
	writePB(&vMsg, bbuf)

	log.Debugf("Uncompressed length %d for %d/%d keys in %d groups", bbuf.Len(), plainsum, len(kvbuf), len(actualRange))

	return &compressor.CompressableData{
		Datatype: 0,
		Data:     bbuf.Bytes(),
	}, version, deltas
}

func KVMerger(db int, stop <-chan int, outrqc chan<- ScanRequest,
	ind <-chan reader.VersionData, outc chan compressor.CompressableData, clientID string, rqVersion int64) {
	// Merging
	//state:=0 -- version not defined
	// received scan
	// received delta

	/*
		IN:
		stop
		ind
		vscan
		OUT:
		 outrqc
		 outc
	*/
	log.Infof("KVMerger dbid: %d started with RQVersion: %d, client: %s", db, rqVersion, clientID)

	var timeFuse <-chan time.Time

	var mapDeltaAll []reader.VersionData

	state := initialSendScan

	var versionSent uint64
	var versionPrepared uint64

	var reliableDelta uint64

	vscan := make(chan reader.VersionData)
	scanrq := ScanRequest{Reply: vscan, Version: rqVersion}
	var ready2Send *compressor.CompressableData
	scanRepeat := 0

	for {

		log.Infof("Merger DB %d in state %s/%d", db, stateMap[state], state)

		switch state {

		case initialSendScan: // Wait to send initial scan request.. ignore deltas unless those are snapshots
			select {
			case v, ok := <-stop: // stop is allways stop
				log.Infof("Merger initialSendScan Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			case outrqc <- scanrq: // succcsesfully sending scan request
				log.Infof("Merger initialSendScan DB %d sent scan request", db)
				state = initialWaitScan

			case v := <-ind: // Received delta
				log.Infof("Merger initialSendScan DB %d receive delta", db)
				if v.DeltaFor == 0 { // Received not delta but snapshot
					// Shortcut - not needing to wait scan
					state = activeSendWaitDeltas
					mapDeltaAll = InitVersionCollection(v)

					ready2Send, versionPrepared = getCompressableSnapshot(v, versionSent)
				}
			}

		case initialWaitScan: // request sent and we are waiting for the reply with snapshot
			select {
			case v, ok := <-stop:
				log.Infof("Merger initialWaitScan Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				state = stopWaitScan
			case v := <-ind: // Received delta
				log.Infof("Merger initialWaitScan DB %d receive delta", db)
				if v.DeltaFor == 0 { // Received not delta but snapshot
					state = brokenInitialScanDelta
					reliableDelta = 0
					ready2Send, versionPrepared = getCompressableSnapshot(v, versionSent)
				} else {
					state = initialWaitScanWithDeltas
					if reliableDelta < v.Version {
						mapDeltaAll = AppendVersionWithSort(mapDeltaAll, v)
						ready2Send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2Send, versionSent, versionPrepared)
					}
				}

			case s, ok := <-vscan:
				log.Infof("Merger initialWaitScan DB %d scan received", db)
				if ok {
					//close(vscan)
					scanRepeat = 0
					state = activeSendWaitDeltas
					mapDeltaAll = append([]reader.VersionData(nil), s)
					ready2Send, versionPrepared = getCompressableSnapshot(s, versionSent)

				} else {
					log.Infof("Merger initialWaitScan DB %d scan not ok aborting", db)
					scanRepeat++
					if scanRepeat > 30 {
						log.Criticalf("Merger initialWaitScan DB %d scan not ok FINAL fuse blow up", db)
						break
					} else {
						log.Criticalf("Merger initialWaitScan DB %d scan not ok retry in %d second", db, scanRepeat)
						timeFuse = time.After(time.Second * time.Duration(scanRepeat))
						state = brokenFuseScan
					}
				}
			}

		case stopWaitScan: // request sent and we are waiting for the reply with snapshot
			select {
			//			case v := <-stop:
			//				Stop alreay received but should receive scan
			case <-ind: // Received delta
				log.Infof("stopWaitScan should ignore deltas", db)

			case <-vscan:
				return

			}
		case brokenFuseScan: // request sent and we are waiting for the reply with snapshot
			select {
			case v, ok := <-stop:
				log.Infof("Merger brokenFuseScan Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			case v := <-ind: // Received delta
				log.Infof("Merger brokenFuseScan DB %d receive delta", db)
				if v.DeltaFor == 0 { // Received not delta but snapshot
					state = brokenInitialScanDelta
					reliableDelta = 0
					ready2Send, versionPrepared = getCompressableSnapshot(v, versionSent)
				} else {
					state = initialWaitScanWithDeltas
					if reliableDelta < v.Version {
						mapDeltaAll = AppendVersionWithSort(mapDeltaAll, v)
						ready2Send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2Send, versionSent, versionPrepared)
					}
				}
			case _ = <-timeFuse:
				log.Criticalf("Merger brokenFuseScan DB %d starting retry", db)
				vscan = make(chan reader.VersionData)
				state = initialSendScan
			}
		case initialWaitScanWithDeltas:
			select {
			case v, ok := <-stop:
				log.Infof("Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			case v := <-ind:
				mapDeltaAll = AppendVersionWithSort(mapDeltaAll, v)
			case s := <-vscan:
				mapDeltaAll = AppendVersionWithSort(mapDeltaAll, s)
				state = activeSendWaitDeltas
				//close(vscan)
			}

		case activeSendWaitDeltas:
			select {
			case v, ok := <-stop:
				log.Infof("activeSendWaitDeltas Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			case outc <- *ready2Send:
				log.Infof("activeSendWaitDeltas DB %d Sent data for version %d -> %d", db, versionSent, versionPrepared)
				state = activeWaitDeltas
				versionSent = versionPrepared
				mapDeltaAll = ResetVersionCollection(mapDeltaAll)
			case v := <-ind: // Received delta
				log.Infof("DB activeSendWaitDeltas/%d Received delta for version %d -> %d", db, v.DeltaFor, v.Version)
				if v.DeltaFor == 0 { // Received not delta but snapshot
					mapDeltaAll = append([]reader.VersionData(nil), v)
					state = brokenInitialScanDelta
					reliableDelta = 0
					ready2Send, versionPrepared = getCompressableSnapshot(v, versionSent)
					mapDeltaAll = nil

				} else {
					state = initialWaitScanWithDeltas
					if versionPrepared < v.Version {
						mapDeltaAll = append(mapDeltaAll, v)
						ready2Send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2Send, versionSent, versionPrepared)
					}
				}

			}
		case activeWaitDeltas:
			select {
			case v, ok := <-stop:
				log.Infof("Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			case v := <-ind: // Received delta
				log.Infof("DB activeWaitDeltas %d Received delta for version %d -> %d", db, v.DeltaFor, v.Version)
				if v.DeltaFor == 0 { // Received not delta but snapshot

					// log.Infof("DB %d full DB as delta version", state, db, ok, v)
					// no need to scan
					state = activeSendWaitDeltas
					mapDeltaAll = append([]reader.VersionData(nil), v)
					ready2Send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2Send, versionSent, versionPrepared)
				} else {
					state = initialWaitScanWithDeltas
					if versionPrepared < v.Version {
						mapDeltaAll = append(mapDeltaAll, v)
						ready2Send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2Send, versionSent, versionPrepared)
					} else {
						log.Warningf("DB %d Ignoring delta update %d -> %d for prepared version %d", db, v.DeltaFor, v.Version, versionPrepared)
					}
				}
			}

		case brokenInitialScanDeltaRecovery: // break during delta while scaning
			select {
			case v, ok := <-stop:
				log.Infof("Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			}

		case brokenInitialScanDelta: // break during delta while scaning
			select {
			case v, ok := <-stop:
				log.Infof("Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			case v := <-ind:
				mapDeltaAll = append(mapDeltaAll, v)
			case outc <- *ready2Send:

			case s, ok := <-vscan:
				if ok {
					//					close(vscan)
					state = activeSendWaitDeltas
					mapDeltaAll = append([]reader.VersionData(nil), s)
					ready2Send, versionPrepared = getCompressableSnapshot(s, versionSent)

				}
			}
		default:
			log.Panicf("Undefined state %d for db %d", state, db)
			panic("Invalid state")
		}

		log.Debugf("Merger state %d For DB: %d", state, db)

	}
	//inc := client.kvPartSink[db] -- remove channels from client's structure after debugging
	//outc :=client.msgSink[db]

	defer close(outc)
	defer log.Infof("Closing merge channel for %d pipe", db)
}
