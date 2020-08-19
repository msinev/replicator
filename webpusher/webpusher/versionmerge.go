package main

import (
	"github.com/msinev/replicator/jsonjackson"
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
func InitVersionCollection(f *reader.VersionData) []*reader.VersionData {
	return append([]*reader.VersionData(nil), f)
}

func ResetVersionCollection(v []reader.VersionData) []reader.VersionData {
	log.Debugf("Resetting version collection of %d elemets", len(v))
	return make([]reader.VersionData, 0, 3)
}

func AppendVersionWithSort(s []*reader.VersionData, f *reader.VersionData) []*reader.VersionData {
	l := len(s)
	if l == 0 {
		return append([]*reader.VersionData(nil), f)
	}

	if s[l-1].Version <= f.Version { // new value is the biggest
		return append(s[0:l], f)
	}

	i := sort.Search(l, func(i int) bool { return s[i].Version < f.Version })

	if i == l { // not found = new value is the smallest
		return append(append([]*reader.VersionData(nil), f), s...)
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

func writeJSONKV(vdata *reader.VersionData, wrt *jsonjackson.JSONWriter) {
	for _, msg := range vdata.VersionData {
		msg.Write(wrt)
	}
}

func sendVersionSnapshot(cli WebClient, request *SyncRequest) {
	/*	if vdata.Version <= isent {
			return nil, isent
		}
	*/
	defer request.Release.Done()
	//bbuf := new(bytes.Buffer)
	jw := jsonjackson.NewBuilder(request.Wr)
	vdata := make(chan reader.VersionData, len(DBS))

	drq := &DrainRequest{vdata}
	jw.BeginArray()

	for _, v := range cli.Drains {
		v <- drq
	}

	for i := 0; i < len(cli.Drains); i++ {
		vi := <-vdata
		writeJSONKV(&vi, jw)
	}

	jw.CloseAll() // All just in case

	//log.Infof("Uncompressed length %d!", bbuf.Len())
	/*&JSONVersionData{
		Version:  0,
		DeltaFor: 1,
		JSONData: bbuf.Bytes(),
	}*/

	//return vdata.Version
}

/*
func sendVersionCompression(rq *DrainRequest) uint64 {
	if vdata.Version <= isent {
		return nil, isent
	}

	bbuf := new(bytes.Buffer)
	jw := jsonjackson.NewBuilder(bbuf)

	jw.BeginArray()
	writeJSONKV(vdata, jw)
	jw.CloseAll()  // just in case

	log.Infof("Uncompressed length %d!", bbuf.Len())
	&JSONVersionData{
		Version:  0,
		DeltaFor: 1,
		JSONData: bbuf.Bytes(),
	},
	return vdata.Version
}
*/

var TFALSE = false

func mergeDeltas(newDeltas *reader.VersionData, prevDelta *reader.VersionData, preparedVersion uint64) (*reader.VersionData, uint64) {
	c := 0

	if newDeltas != nil {
		c += len(newDeltas.VersionData)
	}

	if prevDelta != nil {
		c += len(prevDelta.VersionData)
	}

	if c == 0 {
		return prevDelta, preparedVersion
	}

	pkmap := make(map[string]reader.PKVData, c)
	if prevDelta != nil {
		for _, v := range prevDelta.VersionData {
			pkmap[v.Key] = v
		}
	}

	if newDeltas != nil {
		for _, v := range newDeltas.VersionData {
			pkmap[v.Key] = v
		}
	}

	newdata := make([]reader.PKVData, len(pkmap))
	n := 0
	for _, v := range pkmap {
		newdata[n] = v
		n++
	}
	sort.Slice(newdata, func(i, j int) bool {
		return newdata[i].Key < newdata[j].Key
	})

	return &reader.VersionData{
		DB:          prevDelta.DB,
		Version:     prevDelta.Version,
		DeltaFor:    newDeltas.Version,
		VersionData: newdata,
	}, newDeltas.Version
}

func updateLatestVersion(vdata *reader.VersionData, isent uint64) (*reader.VersionData, uint64) {
	if vdata.Version <= isent {
		return nil, isent
	}

	return vdata, vdata.Version
}

func KVPullMerger(db int, stop <-chan int,
	outrqc chan<- ScanRequest,
	ind <-chan *reader.VersionData,
	ssr <-chan *DrainRequest,
	clientID string) {

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

	///log.Infof("KVMerger dbid: %d started with RQVersion: %d, client: %s", db, rqVersion, client.SESSID)

	var timeFuse <-chan time.Time
	var outc *DrainRequest

	//var mapDeltaAll []*reader.VersionData

	state := initialSendScan

	log.Info("Getting initial request")
	outc = <-ssr
	log.Info("Got initial request")

	//var versionSent uint64
	var versionPrepared uint64
	var reliableDelta uint64
	var ready2send *reader.VersionData

	vscan := make(chan reader.VersionData)
	scanrq := ScanRequest{Reply: vscan, Version: 0}
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
					//mapDeltaAll = InitVersionCollection(v)
					//
					//v
					ready2send, versionPrepared = updateLatestVersion(v, versionPrepared)
					//outc=nil
					//					ready2send, versionPrepared = updateLatestVersion(v, versionSent)
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

					ready2send, versionPrepared = updateLatestVersion(v, versionPrepared)
				} else {
					state = initialWaitScanWithDeltas
					if reliableDelta < v.Version {
						//mapDeltaAll = AppendVersionWithSort(mapDeltaAll, v)
						ready2send, versionPrepared = mergeDeltas(v, ready2send, versionPrepared)
					}
				}

			case s, ok := <-vscan:
				log.Infof("Merger initialWaitScan DB %d scan received", db)
				if ok {
					//close(vscan)
					scanRepeat = 0
					state = activeSendWaitDeltas
					//mapDeltaAll = append([]reader.VersionData(nil), s)
					ready2send, versionPrepared = updateLatestVersion(&s, versionPrepared)

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
					ready2send, versionPrepared = updateLatestVersion(v, versionPrepared)
				} else {
					state = initialWaitScanWithDeltas
					if reliableDelta < v.Version {
						//mapDeltaAll = AppendVersionWithSort(mapDeltaAll, v)
						ready2send, versionPrepared = mergeDeltas(v, ready2send, versionPrepared)
						//ready2send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2send, versionSent, versionPrepared)
					}
				}
			case _ = <-timeFuse:
				log.Criticalf("Merger brokenFuseScan DB %d starting retry", db)
				vscan = make(chan reader.VersionData)
				state = initialSendScan
				ready2send, versionPrepared = nil, 0
				//ready2send, versionPrepared = mergeDeltas(v, ready2send, versionPrepared)
			}
		case initialWaitScanWithDeltas:
			select {
			case v, ok := <-stop:
				log.Infof("Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
				return
			case v := <-ind:
				ready2send, versionPrepared = mergeDeltas(v, ready2send, versionPrepared)
				//mapDeltaAll = AppendVersionWithSort(mapDeltaAll, v)
			case s := <-vscan:
				ready2send, versionPrepared = mergeDeltas(ready2send, &s, versionPrepared)
				//				mapDeltaAll = AppendVersionWithSort(mapDeltaAll, s)
				state = activeSendWaitDeltas
				//close(vscan)
			}

		case activeSendWaitDeltas:
			if outc == nil {
				select {
				case v, ok := <-stop:
					log.Infof("activeSendWaitDeltas Stop requested in state %d in DB %d - %t : %d", state, db, ok, v)
					return
				case outc = <-ssr:
					log.Infof("activeSendWaitDeltas DB %d Sent data for version %d", db, versionPrepared)
					state = activeWaitDeltas
					//versionSent = versionPrepared
					//mapDeltaAll = ResetVersionCollection(mapDeltaAll)
				case v := <-ind: // Received delta
					log.Infof("DB activeSendWaitDeltas/%d Received delta for version %d -> %d", db, v.DeltaFor, v.Version)
					if v.DeltaFor == 0 { // Received not delta but snapshot
						//mapDeltaAll = append([]reader.VersionData(nil), v)
						state = brokenInitialScanDelta
						reliableDelta = 0
						ready2send, versionPrepared = updateLatestVersion(v, versionPrepared)
						//mapDeltaAll = nil

					} else {
						state = initialWaitScanWithDeltas
						if versionPrepared < v.Version {
							//						mapDeltaAll = append(mapDeltaAll, v)
							//						ready2send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2send, versionSent, versionPrepared)
							ready2send, versionPrepared = mergeDeltas(ready2send, v, versionPrepared)
						}
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
					//					mapDeltaAll = append([]reader.VersionData(nil), v)
					ready2send, versionPrepared = updateLatestVersion(v, versionPrepared)
					//					ready2send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2send, versionSent, versionPrepared)
				} else {
					state = initialWaitScanWithDeltas
					if versionPrepared < v.Version {
						//						mapDeltaAll = append(mapDeltaAll, v)
						//						ready2send, versionPrepared, mapDeltaAll = getCompressableDelta(mapDeltaAll, ready2send, versionSent, versionPrepared)
						ready2send, versionPrepared = mergeDeltas(ready2send, v, versionPrepared)

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
				ready2send, versionPrepared = mergeDeltas(ready2send, v, versionPrepared)
			//case outc <- *ready2send:

			case s, ok := <-vscan:
				if ok {
					//					close(vscan)
					state = activeSendWaitDeltas
					//mapDeltaAll = append([]reader.VersionData(nil), s)
					ready2send, versionPrepared = updateLatestVersion(&s, versionPrepared)

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

	//	defer close(outc)
	defer log.Infof("Closing merge channel for %d pipe", db)
}
