package main

import (
	"net"
	"bufio"
	"sync"
	"fmt"
	"os"
	"time"
	"strconv"
	"RedisReplica/ClientServer"
	"RedisReplica/Compressor"
	"RedisReplica/Reader"
	"sync/atomic"

//	"RedisReplica/BlockerChan"
//	"net/http"
)

/*
const initialSendScan=0
const initialWaitScan=initialSendScan+1
const initialWaitScanWithDeltas=initialWaitScan+1
const activeSendWaitDeltas=initialWaitScanWithDeltas+1
const activeWaitDeltas=activeSendWaitDeltas+1

const brokenInitialScanDelta=(activeWaitDeltas+19)%10
const brokenInitialScanDeltaRecovery=brokenInitialScanDelta+1

 */

/*
func KVMergerPlain(db int, inc chan Reader.VersionData, ind chan Reader.VersionData,
	chs chan<- int64, chd chan<- int64, chok <-chan int64, outc chan Compressor.CompressableData) {

	//inc := client.kvPartSink[db] -- remove channels from client's structure after debugging
	//outc :=client.msgSink[db]

	defer close(outc)
	defer log.Infof("Closing merge channel for %d pipe", db)
	kvbuf := make([]*sendrecv.Msg_SendValues, 0, 20000)

	for vdata := range inc {
	}
}
*/
/*
func loadMsg(redis int) (*sendrecv.Msg) {

	lzmab, _:=ioutil.ReadFile("/home/max/lzma.redis."+strconv.Itoa(redis))
	log.Infof("Uncompressing %d \n", len(lzmab))

	bufpb,_:=lzma.Uncompress(lzmab)
	log.Infof("Uncompressed %d \n", len(bufpb) )
	rdr:=bytes.NewReader(bufpb)

	msg:=readPB(rdr)
	return msg
}
*/

const GZIPCompression = 2
const LZMACompression = 1
const NoCompression = 0


func SocketWriter(client *ClientServer.Client, inCh <-chan Compressor.TheMessage) {

	defer log.Info("Socket writer exiting")

	defer client.DoneMsg()
	defer client.Conn.Close()


	count := 0
	for {
		log.Info("Waitig block for socket!")
		select {
		case <-client.Alive:
			log.Infof("Exiting socket Writer as chan Alive closed")
		case block, ok := <-inCh:
		if (!ok) {
			log.Info("merged qos channel closed")
			return
		}

		ldata := len(block)
		count++
		log.Infof("Block %d %d for socket!", count, ldata)

		for ldata > 0 {
			n, err := client.Conn.Write(block)
			if (err != nil) {
				log.Error("error writing socket", err)
				return
			}
			if (n >= ldata) {
				break
			}
			block = block[n:]
			ldata -= n
		}
		}

	}

}

func ControlMessageLoopForClient(client *ClientServer.Client) { // Kinda client servant main actor
	defer client.Terminate()
	for {
		select {
		case message, ok := <-client.Control:
			if (ok) {
				log.Infof("Control Message %s for ", message.Message)
				if (message.Message == "done") {
					close(client.Alive)
					message.Done("ok")
					return

				} else if (message.Message == "sync.sent") {
					message.Done("ok")
				} else {
					message.Done("unknown")
				}
				if (message.Finished != nil) {
					message.Finished.Done()
				}
			} else {
				log.Error("Error in control loop")
				close(client.Alive)
				return
			}
		}
	}
}

const serverChannelBuffer = 10
const serverBlockBuffer = 3
var clietID uint64 =0

func handleServer(tcpconn net.Conn, w *sync.WaitGroup, scan []ScanReader, delta []Reader.DeltaReceiver) {

	//defer tcpconn.Close()
	if (w != nil) {
		defer w.Done()
	}

	ldbs := len(DBS)

	id:=atomic.AddUint64(&clietID, 1)

	remote:=tcpconn.RemoteAddr().String()
	clientid:=remote+":"+strconv.FormatUint(id, 16)
	log.Infof("Serving connection from %s", remote)

	client := new(ClientServer.Client)
	client.Init(ldbs)
	client.Conn = tcpconn
	client.ID = tcpconn.RemoteAddr().String()
	client.Conntype = CONN_TYPE_TCP
	client.Databases = DBS
	//

	go ControlMessageLoopForClient(client)

	qosDrains:= client.BlockDrains

	for rk, rv := range client.Databases {
	//	if (DBSPlain[rv] == "+") {
			log.Debugf("Starting DB %d -> chan", rv)
			req:= scan[rk].data
			//controlStop := make(chan int64)
			stage2 := make(chan Reader.VersionData)
			stage2a := delta[rk].SubscribeVersions(clientid) // Subscribe to updates
			stage3 := make(chan Compressor.CompressableData, serverChannelBuffer)
			stage4 := make(chan Compressor.CompressedData)
			stage5 := make(chan Compressor.TheMessage, serverBlockBuffer)

			//		client.KVFullScan[rk] = stage1   // for debug
			client.KVPartSink[rk] = stage2   // for debug
			client.MsgSink[rk] = stage3      // for debug
			client.DataBreakers[rk] = stage4 // for debug
			client.BlockDrains[rk] = stage5  // for debug
			qosDrains[rk] = stage5
/*
func KVMerger(db int, stop <-chan int64, outrqc chan<- <-chan Reader.VersionData,
	ind chan Reader.VersionData, outc chan Compressor.CompressableData) {
			rqVersion
 */
		rqVersion := int64(0)
		if (client.Versions != nil) {
			rqVersion = client.Versions[rk];
		}

		go KVMerger(rk, client.Alive, req, stage2a, stage3, clientid, rqVersion)

			//go Reader.Scan(rv, stage2, &client.DBReader) moved to Init
			//		go Reader.KVScanAccumulator(rk, stage1,  stage2)

		go ClientServer.ServerDataCompressor(client, rk, GZIPCompression, stage3, stage4)
			go ClientServer.ServerDataStreaming(client, rk, stage4, stage5)
//		} else {


	//	}
		//		go scan(rv, client.kvPartSink[rk])

	}

	qosMerged := QOSBuilder(qosDrains)
	client.SocketDrain = qosMerged // for debug

	//go ServerWriter(client, client.socketDrain)
	const hello ="RRC version 1.0\n\n"
	//
	log.Debugf("Writing hello %s", hello)
	r := bufio.NewReader(tcpconn)

	n, err := tcpconn.Write([]byte(hello))
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			log.Error(n, err)
			return
		}
		lline := len(line)
		if lline == 0 || lline == 1 && line[0] == '\n' {
			break
		}
		sline:=string(line)
		log.Info("Server reading header: " + string(sline))
		client.ParseHeader(sline);
	}
	log.Debug("Starting socket writer")

	go SocketWriter(client, qosMerged) // send data from socketDrain into socket

	if err != nil {
		log.Error(n, err)
		return
	}
}

type ScanRequest struct {
	Version int64
	Reply   chan<- Reader.VersionData
}

type  ScanReader struct {
	data chan<- ScanRequest
}

//var onceDB sync.Once
const RedisBuf=10

func InitReaders(so []Reader.ServerOptions) ([]ScanReader, []Reader.DeltaReceiver) {
	var DBDeltaReaders []Reader.DeltaReceiver
	var DBScanReaders []ScanReader

	redisURL := *so[0].Global.RedisURL
	//onceDB.Do( func () {

	go Reader.RedisExecutor(redisURL)

	ldbs := len(DBS)

	DBDeltaReaders = make([]Reader.DeltaReceiver, ldbs)
	DBScanReaders = make([]ScanReader, ldbs)

	chandb := make([]chan<- string, ldbs)

	log.Info("Starting REDIS connectors")
	go Reader.RedisExecutor(redisURL)
	log.Info("Init readers")

	//go ReadVersionDelta(start  <-chan string, out chan <- VersionData, db int )

	for r := range DBDeltaReaders {
		db := DBS[r]
		_, ok := DBSPlain[db]
		ver, dbver := Reader.GetVersion(db)

		if (!ok && !dbver) {
			log.Errorf("Database %d missing version", db)
		} else if (dbver) {
			log.Infof("Database %d starting version %d", db, ver)
		}
		dbt := 0
		if (ok || !dbver) {
			log.Panic() // shall we panic ??
			// TODO fix subscription on plain DBs or missing versions
			dbt = 1
		} else {
			DBDeltaReaders[r].Init(so[r], dbt, ver)
			chandb[r] = DBDeltaReaders[r].NotifyVer
		}

	}

	go Reader.RedisSubscriber(redisURL, DBS, DBSPlain, chandb)

	log.Info("Starting scan sunscribers")
	for k, _ := range DBS {
		DBScanReaders[k] = *InitScanReaderWithVersionCheck(so[k])
	}
	log.Info("Readers init done")
	//})
return DBScanReaders, DBDeltaReaders
}

/*
func InitScanReaderWithNoVersionCheck(dbindex int) (<-chan Reader.VersionData, chan<- uint64) {
	log.Info("Initializing readers")

	InitReaders()

	//chanblockdb:=make([]chan []string, ldbs)
	//DBDeltaReaders=make([]Reader.DeltaReceiver, ldbs)

	versionchan := make(chan Reader.VersionData)
	controlchan := make(chan uint64)
	keyblock := make(chan []string)
	kvdata := make(chan []Reader.RedisKV)
	//for k,_:= range DBS {
	rs := DBDeltaReaders[dbindex].SubscribeKeys(remote)

//		chandb[k]=make(chan string, RedisBuf)
//		chanblockdb[k]=make(chan []string, RedisBuf)
	go BlockerChan.UniqueBlocker(rs, keyblock)
	//	go

	go Reader.KVVersionGenerator(dbindex, kvdata, versionchan)
	go Reader.KeyReader(dbindex, keyblock, kvdata)

	// go Reader.KVVersionGenerator(dbindex, kvdata, versionchan)

	// go Reader.KVScanDeltaAccumulator(rk, readers[rk].Subscribe(remote), stage2a)

	//go KeyUpdatePublisher(db, listener , out );



	log.Info("Initializing readers initialized")
	return versionchan, controlchan
  }
*/
func scanReaderWithVersionCheckProcessor(so Reader.ServerOptions, data <-chan ScanRequest) {
	db := so.DB
 	defer log.Debugf("Exiting scan for dbID %d", db)
 	for rq:= range data {
 		log.Debugf("Performing scan for DB %d for %d", db, rq.Version)
		if rq.Version > 0 {
			Reader.Scan(so, rq.Reply)
		} else {
			Reader.ScanVersion(so, uint64(rq.Version), rq.Reply)
		}
		//log.Debugf("Scan for DB %d complete", db)
	}
 }

func InitScanReaderWithVersionCheck(so Reader.ServerOptions) *ScanReader {

	//	dbnumber:=so.DB
	//ldbs := len(DBS)
	//chandb:=make([]chan string, ldbs)
	//chanblockdb:=make([]chan []string, ldbs)

	//go BlockerChan.UniqueBlocker(chandb[k], chanblockdb[k])

	dataChan := make(chan ScanRequest)
	go scanReaderWithVersionCheckProcessor(so, dataChan)
//	rqChan:=make(chan<- uint64)

	log.Info("Initializing readers initialized")

	return &ScanReader{data: dataChan}

}


/* InitReader dublicate
func InitVersionScanReaders() []Reader.DeltaReceiver {
	log.Info("Initializing readers")

	ldbs := len(DBS)
	chandb:=make([]chan string, ldbs)
	chanblockdb:=make([]chan []string, ldbs)
	for k,_:= range DBS {
		chandb[k]=make(chan string, RedisBuf)
		chanblockdb[k]=make(chan []string, RedisBuf)
		go BlockerChan.UniqueBlocker(chandb[k], chanblockdb[k] )

	}

	onceDelta.Do(func () {
		go Reader.RedisExecutor()
		//	go Reader.RedisSubscriber(DBS, chandb)
	})

	go Reader.RedisSubscriber(DBS, chandb)

	//go KeyUpdatePublisher(db, listener , out );

	DBDeltaReaders =make([]Reader.DeltaReceiver, ldbs)

	for r:=range DBDeltaReaders {
		DBDeltaReaders[r].Init(DBS[r], )
	}

	log.Info("Initializing readers initialized")
	return DBDeltaReaders
}
*/

func mainServerTCPLoop(so []Reader.ServerOptions, bind string, waitTermination bool) {
	// Listen for incoming connections.
	var counter uint32
	l, err := net.Listen(CONN_TYPE_TCP, bind)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.

	defer l.Close()

	sigs := make(chan os.Signal, 1)

	go func() {
		for {
			select {
			case sig, ok := <-sigs:

				if (!ok) {
					return
				}

				fmt.Println("Received signal")
				fmt.Println(sig)

				l.Close()
			}

		}
	}()


	fmt.Println("Listening on " + bind)
	var wg sync.WaitGroup

	//readers:=InitScanReadersWithVersionCheck()
	scan, delta := InitReaders(so);
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		counter++
		wg.Add(1)

		if err != nil {
			log.Errorf("Socket returned error: %s", err.Error())
			if (waitTermination) {

				go func() {
					time.Sleep(time.Second * 10)
					os.Exit(2)
				}()

				wg.Wait()
			}
			os.Exit(1)
		}
		log.Infof("Connected to %s (connection %d)", conn.RemoteAddr(), counter)
		// Handle connections in a new goroutine
  		go handleServer(conn, &wg, scan,delta)

	}

}
