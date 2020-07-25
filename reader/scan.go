package reader

import (
	"github.com/garyburd/redigo/redis"
	"github.com/msinev/replicator/pool"

	"github.com/msinev/replicator/protobuf/sendrecv"
	"strconv"
	"strings"
	"sync"
	//	"github.com/go-kit/kit/util/conn"
	"time"
)

/* -- DEBUG


func mainSave() {

var wg1 sync.WaitGroup
wg1.Add(1)
var consumeReplica=make(chan RedisKV, 100)
go pool.RedisExecutor();

dbnum, _:=strconv.Atoi(pool.GetEnvDefault("DB", "0"))

go kvConsumerSender(dbnum, consumeReplica, &wg1)

scan(dbnum, consumeReplica, nil);

wg1.Wait()


	r:=DeltaReceiver{queue:nil, keyMap:nil}

	r.Init()
	r.Add("1", "77")
	r.Add("2", "88")
	r.Add("4", "88498")
	r.Add("5", "8876")
	r.Add("6", "8876")


	json, err := r.queue.ToJSON()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Dump r.queue: \n", string(json), "\n")

DEBUG -- */

/* --

	for r.Consume(buf, 2)>0 {
		stdoutDumper := hex.Dumper(os.Stdout)

		defer stdoutDumper.Close()
		stdoutDumper.Write(buf.Bytes())

		fmt.Println("\n==")
		buf = new(bytes.Buffer)
	}

-- */
/*

QOSIdle=make(chan TheMessage)
QOSLo=make(chan TheMessage)
QOSHi=make(chan TheMessage)
QOSRT=make(chan TheMessage)
OutBound=make(chan TheMessage)

go QOSScheduler()
*/
/* --
	rw := bytes.NewBuffer(nil)
	wu:=[]byte("Hello world!Hello world!Hello world!Hello world!Hello world!Hello worl"+
		"Hello world!Hello world!Hello world!Hello world!Hello world!Hello "+
		"Hello world!Hello world!Hello world!Hello world!Hello world!Hello "+"Hello world!Hello world!sHello world!Hello world!Hello dworld!Hello "+"Hello world!Hello world!Hello world!Hello world!Hello world!Hello "+"Hello world!Hello world!Hello world!Hello world!Hello world!Hello "+"Hello world!Hello world!Hello world!Hello world!Hello world!Hello ")


	w := lz4.NewWriter(rw)
	w.Write( wu )
	w.Close()
	outma,_:=lzma.Compress(wu)

	fmt.Println(rw.Len())
	fmt.Println(len(outma))
	fmt.Println(len(wu))
}

-- */

type VersionData struct {
	Version     uint64
	DeltaFor    uint64
	VersionData []RedisKV
}

func KVVersionGenerator(so ServerOptions, inc <-chan []RedisKV, outc chan<- VersionData) {

	defer log.Infof("KVScanAccumulator %d terminated", so.DB)
	defer close(outc)

	//outc := client.kvPartSink[db]  -- remove channels from client's structure after debugging
	//inc :=client.kvFullScan[db] -- remove channels from client's structure after debugging

	//kvbuf:=make([]RedisKV, 0, 20000)
	ver := time.Now().Unix() * 10000

	for kva := range inc {

		//var err error
		log.Noticef("DB %d KVVersionGenerator received %d Version %d ", so.DB, len(kva), ver)

		ver++
		outc <- VersionData{
			Version:     uint64(ver),
			DeltaFor:    uint64(ver - 1),
			VersionData: kva,
		}

	}

}

/*
func reader(conn redis.Conn) {
	var (
		cursor int64
		items []string
	)

    cursor=0
	results := make([]string, 0)

	for {
		values, err := redis.Values( conn.Do("SSCAN", cursor) )
		if err != nil {
			return
		}

		values, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return
		}

		results = append(results, items...)

		if cursor == 0 {
			break
		}
	}
}
*/

type RedisKV = *sendrecv.Msg_SendValues
type RetriveOperation func(string, redis.Conn) error
type ReadOperation func(int64, string, redis.Conn) (error, RedisKV)

type TypeConverter struct {
	init RetriveOperation
	read ReadOperation
	tag  string
}

const maxrange = 1000000

func uselessTTL(ttl int64) bool {
	return ttl == -2 || ttl >= 0 && ttl < 100
}

const versionKey = "version"
const versionKeyList = "version:"

func ReadVersionDelta(so ServerOptions, start <-chan string, out chan<- VersionData, initVersion uint64) { //Delta reader
	db := so.DB
	/*
		if(nextVersion==nowVersion) {
			select {
			case out <- VersionData {
				Version:     nowVersion,
				DeltaFor:    fromVersion,
				VersionData: existingKeys,
			}:	{
				existingKeyIndexes = make(map[string]int)
				existingKeys = make([] RedisKV, 200)
				fromVersion=nowVersion
			}

			case nextVersion = <-start:
			}

			//			return true, nowVersion, nil
		}
	*/

	fromVersion := initVersion
	checkStart := false

	for {

		log.Debugf("DB %d started delta loop version %d", db, fromVersion)
		if !checkStart {
			sT := <-start
			log.Debugf("!! DB  %d started delta after receiving mark %s ", db, sT)
		} else {
			log.Debugf("!! DB %d started delta repeat", db)
		}
		checkStart = false

		chdone := make(chan uint64)

		//
		//		--- exec begin --
		//
		pool.ExecutorGo <- func(conn redis.Conn) error {
			nowVersion := fromVersion
			//initVersion

			defer close(chdone)
			log.Noticef("Retrive version starting from %d", nowVersion)
			defer log.Noticef("Retrive version complete %d to %d", fromVersion, nowVersion)

			_, err := conn.Do("SELECT", db)
			if err != nil {
				return err
			}

			addVersionKeys := !so.Global.LocalVersion

			nextVersion, err := redis.Uint64(conn.Do("GET", versionKey))
			log.Noticef("Current next version %d", nextVersion)
			if nowVersion < nextVersion {
				newKeys := make(map[string]uint64)
				existingKeyIndexes := make(map[string]int)
				existingKeys := make([]RedisKV, 0)
				for nowVersion < nextVersion {
					//existingKeysCount:=len(existingKeyIndexes)
					if addVersionKeys {
						newKeys[versionKey] = nextVersion
					}
					versionListTTL, err := redis.Int(conn.Do("TTL", versionKeyList+strconv.FormatUint(fromVersion, 10)))
					if err != nil {
						return err
					}

					if versionListTTL == -2 {
						return nil
					}

					for i := nowVersion + 1; i <= nextVersion; i++ {
						lrKey := versionKeyList + strconv.FormatUint(i, 10)
						if addVersionKeys {
							newKeys[lrKey] = i
						}
						err := conn.Send("LRANGE", lrKey, 0, 10000)
						log.Debugf("Send get keys for version %d", i)
						if err != nil {
							return err
						}
					}

					conn.Flush()

					for i := nowVersion + 1; i <= nextVersion; i++ {
						rk, err := redis.Strings(conn.Receive())
						if err != nil {
							return err
						} else {
							for _, vv := range rk {
								newKeys[vv] = i
							}
							log.Debugf("Got %d keys reply totaling %d pairs for version %d", len(rk), len(newKeys), i)
						}

					}

					addKeys := make([]string, len(newKeys))
					for key, _ := range newKeys {
						addKeys = append(addKeys, key)
					}

					existingKeyIndexes, existingKeys, err = retriveKeys(existingKeyIndexes, existingKeys, addKeys, conn)
					if err != nil {
						return err
					}

					nextVersionCheck, err := redis.Uint64(conn.Do("GET", versionKey)) // check that no changes during retriving
					if err != nil {
						return err
					}

					if nextVersionCheck == nextVersion {
						break
					}
					//
					log.Infof("Version %d => %d changed while getting data to %d", nowVersion, nextVersion, nextVersionCheck)
					nowVersion = nextVersion
					nextVersion = nextVersionCheck

				} // for
				out <- VersionData{DeltaFor: fromVersion, Version: nextVersion, VersionData: existingKeys}
				log.Infof("Sent version update %d -> %d %d pairs", fromVersion, nextVersion, len(existingKeys))
			} else {
				log.Infof("No changes in version %d", nextVersion)
			}

			chdone <- nextVersion

			return nil
		} // function

		//
		//		--- exec end --
		//

		doLoop := true
		for doLoop {
			ok := false
			select {
			case sT := <-start:
				checkStart = true
				log.Debugf("DB %d receive mark %s", db, sT)
				break
			case fromVersion, ok = <-chdone:
				if ok {
					log.Debugf("Shifting db %d current version to %d", db, fromVersion)
				} else {
					log.Debugf("Delta db %d failed", db)
				}
				doLoop = false

			}
		} // Wait completed

	} // for

}

func retriveKeys(existingKeyIndexes map[string]int, existingKeys []RedisKV, keys []string,
	conn redis.Conn) (map[string]int, []RedisKV, error) {
	lkeys := len(keys)

	pttl := make([]int64, lkeys)
	var err error
	for _, val := range keys {
		//	  log.Println("Sending info " + val)
		err := conn.Send("PTTL", val)
		if err != nil {
			log.Error(err)
			return existingKeyIndexes, existingKeys, err
		}
	}

	conn.Flush()

	for key, val := range keys {
		//	  log.Println("Sending info " + val)

		pttl[key], err = redis.Int64(conn.Receive())
		if err != nil {
			log.Error(err)
			return existingKeyIndexes, existingKeys, err
		}
		if !uselessTTL(pttl[key]) {
			err := conn.Send("TYPE", val)
			if err != nil {
				return existingKeyIndexes, existingKeys, err
			}
		}
	}
	conn.Flush()
	cnv := make([]*TypeConverter, len(keys))

	for keysKey, keysVal := range keys {
		if !uselessTTL(pttl[keysKey]) {
			keyType, err := redis.String(conn.Receive())
			if err != nil {
				return existingKeyIndexes, existingKeys, err
			}
			//		log.Println("Received type " + keyType + " for "+ keysVal)
			if err != nil {
				return existingKeyIndexes, existingKeys, err
			}
			cnv[keysKey] = retriveFunctions[keyType]
			cnv[keysKey].init(keysVal, conn)
		}
	}

	conn.Flush()
	lnewX := len(existingKeys)
	for key, val := range keys {
		ttk := pttl[key]
		if !uselessTTL(ttk) {
			err, data := cnv[key].read(ttk, val, conn)
			if err != nil {
				return existingKeyIndexes, existingKeys, err
			}
			ind, ok := existingKeyIndexes[val]
			if ok {
				existingKeys[ind] = data
			} else {
				existingKeys = append(existingKeys, data)
				existingKeyIndexes[val] = lnewX
				lnewX++
			}

		}
	}

	return existingKeyIndexes, existingKeys, nil
}

func retrive(keys []string, conn redis.Conn) (error, []RedisKV) { // for KeyReader

	log.Notice("Retrive starting")
	defer log.Notice("Retrive complete")

	pttl := make([]int64, len(keys))
	var err error
	for _, val := range keys {
		//	  log.Println("Sending info " + val)
		err := conn.Send("PTTL", val)
		if err != nil {
			log.Error(err)
			return err, nil
		}

	}
	conn.Flush()

	for key, val := range keys {
		//	  log.Println("Sending info " + val)

		pttl[key], err = redis.Int64(conn.Receive())
		if err != nil {
			log.Error(err)
			return err, nil
		}
		if !uselessTTL(pttl[key]) {
			err := conn.Send("TYPE", val)
			if err != nil {
				return err, nil
			}
		}
	}
	conn.Flush()
	cnv := make([]*TypeConverter, len(keys))

	for keysKey, keysVal := range keys {
		if !uselessTTL(pttl[keysKey]) {
			keyType, err := redis.String(conn.Receive())
			if err != nil {
				return err, nil
			}
			//		log.Println("Received type " + keyType + " for "+ keysVal)
			if err != nil {
				return err, nil
			}
			cnv[keysKey] = retriveFunctions[keyType]
			cnv[keysKey].init(keysVal, conn)
		}
	}

	conn.Flush()
	databuf := make([]RedisKV, len(keys))
	for key, val := range keys {
		ttk := pttl[key]
		if !uselessTTL(ttk) {
			err, data := cnv[key].read(ttk, val, conn)
			if err != nil {
				return err, nil
			}
			databuf[key] = data

		}
	}

	return nil, databuf

}

func GetVersion(db int) (uint64, bool) {
	var s sync.WaitGroup
	s.Add(1)
	var version uint64
	var ok bool

	pool.ExecutorGo <- func(conn redis.Conn) error {
		ok = false
		defer s.Done()

		_, err := conn.Do("SELECT", db)
		if err != nil {
			return err
		}

		version, err = redis.Uint64(conn.Do("GET", versionKey))
		if err != nil {
			return nil
		}
		ok = true
		return nil
	}
	s.Wait()

	return version, ok
}

func KeyReader(db int, inp <-chan []string, out chan<- []RedisKV) {

	//	go pollLoop(&wg1)
	defer close(out)
	defer log.Infof("Exiting KeyReader function in db %d. Closing channel", db)
	//}

	log.Infof("Starting keyReader function in db %d", db)
	for sa := range inp {
		var s sync.WaitGroup
		s.Add(1)

		pool.ExecutorGo <- func(conn redis.Conn) error {
			defer s.Done()

			_, err := conn.Do("SELECT", db)
			if err != nil {
				return err
			}

			err, data := retrive(sa, conn)
			if err != nil {
				log.Error(err)
				return err
			}
			out <- data
			return nil
		}

		s.Wait()
	}

}

func ScanVersion(so ServerOptions, version uint64, out chan<- VersionData) {
	var wg sync.WaitGroup
	db := so.DB
	wg.Add(1)
	fallback := true

	fromVersion := version

	pool.ExecutorGo <- func(conn redis.Conn) error {
		defer wg.Done()
		nowVersion := fromVersion
		//initVersion

		//defer close(chdone)
		log.Noticef("Retrive version starting from %d", nowVersion)
		defer log.Noticef("Retrive version complete %d to %d", fromVersion, nowVersion)

		_, err := conn.Do("SELECT", db)
		if err != nil {
			return err
		}

		addVersionKeys := !so.Global.LocalVersion

		nextVersion, err := redis.Uint64(conn.Do("GET", versionKey))
		log.Noticef("Current next version %d", nextVersion)
		if nowVersion < nextVersion {
			//nextVersion, err := redis.Uint64(conn.Do("EXISTS", versionKey))
			newKeys := make(map[string]uint64)
			existingKeyIndexes := make(map[string]int)
			existingKeys := make([]RedisKV, 0)
			for nowVersion < nextVersion {
				//existingKeysCount:=len(existingKeyIndexes)
				if addVersionKeys {
					newKeys[versionKey] = nextVersion
				}

				vlKey := versionKeyList + strconv.FormatUint(fromVersion, 10)
				versionListTTL, err := redis.Int(conn.Do("TTL", vlKey))
				if err != nil {
					return err
				}

				if versionListTTL < 0 {
					log.Errorf("Invalid  %s TTL %d, doing fallback", vlKey, versionListTTL)
					return nil
				}

				for i := nowVersion + 1; i <= nextVersion; i++ {
					lrKey := versionKeyList + strconv.FormatUint(i, 10)
					if addVersionKeys {
						newKeys[lrKey] = i
					}
					err := conn.Send("LRANGE", lrKey, 0, 10000)
					log.Debugf("Send get keys for version %d", i)
					if err != nil {
						return err
					}
				}

				conn.Flush()
				fail := uint64(0)
				for i := nowVersion + 1; i <= nextVersion; i++ {
					rk, err := redis.Strings(conn.Receive())
					if err != nil {
						return err
					} else if len(rk) == 0 {
						fail = i
						break
					} else {
						for _, vv := range rk {
							newKeys[vv] = i
						}
						log.Debugf("Got %d keys reply totaling %d pairs for version %d", len(rk), len(newKeys), i)
					}

				}
				if fail != 0 {
					log.Errorf("Missing version %d list, doing fallback", fail)
					return nil
				}
				addKeys := make([]string, len(newKeys))
				for key, _ := range newKeys {
					addKeys = append(addKeys, key)
				}

				existingKeyIndexes, existingKeys, err = retriveKeys(existingKeyIndexes, existingKeys, addKeys, conn)
				if err != nil {
					return err
				}

				nextVersionCheck, err := redis.Uint64(conn.Do("GET", versionKey)) // check that no changes during retriving
				if err != nil {
					return err
				}

				if nextVersionCheck == nextVersion {
					break
				}
				//
				log.Infof("Version %d => %d changed while getting data to %d", nowVersion, nextVersion, nextVersionCheck)
				nowVersion = nextVersion
				nextVersion = nextVersionCheck

			} // for
			out <- VersionData{DeltaFor: fromVersion, Version: nextVersion, VersionData: existingKeys}
			log.Infof("Sent version update %d -> %d %d pairs", fromVersion, nextVersion, len(existingKeys))
			fallback = false
		} else if nowVersion == nextVersion {
			out <- VersionData{DeltaFor: fromVersion, Version: nextVersion, VersionData: make([]RedisKV, 0)}
			fallback = false
			log.Infof("No changes in version %d", nextVersion)
		}

		//			chdone <- nextVersion

		return nil
	} // function

	//
	//		--- exec end --
	//
	wg.Wait()
	if fallback {
		Scan(so, out)
	}

}

func Scan(so ServerOptions, out chan<- VersionData) {

	db := so.DB
	log.Infof("Starting scan function on db %d", db)

	//out:=<-call
	// TODO: In case of REDIS preformance bottleneck make a batch reading to send same scan to all of them

	pool.ExecutorGo <- func(conn redis.Conn) error {

		defer close(out)
		defer log.Infof("Exiting scan function in db %d. Closing channel", db)
		//}

		_, err := conn.Do("SELECT", db)
		if err != nil {
			return err
		}

		ver, err := redis.Int64(conn.Do("GET", versionKey))
		if err != nil {
			log.Criticalf("Unable to get root %s key in db %d. Aborting scan", versionKey, db)
			return nil
		}

		iter := 0
		var data []RedisKV

		filterPrefix := versionKey
		flp := len(filterPrefix)

		for {

			//log.Printf("Sending scan %d \n" , iter)
			arr, err := redis.MultiBulk(conn.Do("SCAN", iter))
			if err != nil {
				log.Error(err)
				return err
			} else {

				// now we get the iter and the keys from the multi-bulk reply
				olditer := iter
				iter, _ = redis.Int(arr[0], nil)
				keys, _ := redis.Strings(arr[1], nil)
				lkeys := len(keys)
				log.Debugf("SCAN DB %d ITER %d -> %d KEYS %d", db, olditer, iter, lkeys)
				rkeys := keys
				if so.Global.LocalVersion {
					rkeys = make([]string, 0, lkeys)
					for _, v := range keys {
						if !(strings.HasPrefix(v, filterPrefix) && (len(v) == flp || v[flp] == ':')) {
							rkeys = append(rkeys, v)
						}
					}
					log.Warningf("DB %s scan filered version keys %d -> %d", db, lkeys, len(rkeys))

				}

				err, nextdata := retrive(rkeys, conn)
				data = append(data, nextdata...)
				if err != nil {
					log.Error(err)
					return err
				}
				defer log.Infof("Sent scan result on db %d. Version %d with %d keys", db, ver, len(data))
				//	scan(iter)
			}

			if iter == 0 {
				out <- VersionData{
					Version:     uint64(ver),
					DeltaFor:    0,
					VersionData: data,
				}

				return nil
			}

		}
	}
}

func writeSV(conn redis.Conn, vv *sendrecv.Msg_SendValues, reset bool) (int, error) {
	Reply := 0
	ttls := int(vv.GetTTL()/1000) + 1
	switch vv.Value.(type) {
	case *sendrecv.Msg_SendValues_Strvalue:
		//log.Printf("SET %s -> %s", vv.GetKey(), vv.GetStrvalue())
		if vv.TTL != nil {

			err := conn.Send("SETEX", vv.GetKey(), ttls, vv.GetStrvalue())
			return 1, err

		} else {
			err := conn.Send("SET", vv.GetKey(), vv.GetStrvalue())
			return 1, err

		}
	case *sendrecv.Msg_SendValues_Listvalue:
		list := vv.GetListvalue()
		//log.Printf("SETLIST %s -> %d", vv.GetKey(), len(list.Value))
		if !reset {
			err := conn.Send("DEL", vv.GetKey())
			if err != nil {
				return Reply, err
			}
			Reply++
		}

		for _, lv := range list.Value {

			err := conn.Send("RPUSH", vv.GetKey(), lv)
			if err != nil {
				return Reply, err
			}
			Reply++

		}

		if vv.TTL != nil {
			err := conn.Send("EXPIRE", vv.GetKey(), ttls)
			if err != nil {
				return Reply, err
			}
			Reply++
		}

		return Reply, nil

	}

	return Reply, nil

}

/*
func mainRead() {

	var wg1 sync.WaitGroup
	wg1.Add(1)
	//var consumeReplica=make(chan RedisKV, 100)

	go pool.RedisExecutor()

 	dbnum, _:=strconv.Atoi(pool.GetEnvDefault("DB", "5"))
	dbout, _:=strconv.Atoi(pool.GetEnvDefault("DBOUT", "7"))

	msg := loadMsg(dbnum)

    writeKeys(dbout,msg,&wg1)

//	go kvConsumerSender(dbnum, consumeReplica, &wg1)

//	scan(dbnum, consumeReplica, nil);

	wg1.Wait()


}


func mainLoad() {

	log.SetFlags(log.Lshortfile)

	cer, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		log.Println(err)
		return
	}

	config := &tls.Config{Certificates: []tls.Certificate{cer}}
	ln, err := tls.Listen("tcp", ":443", config)
	if err != nil {
		log.Println(err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		msg, err := r.ReadString('\n')
		if err != nil {
			log.Println(err)
			return
		}

		println(msg)

		n, err := conn.Write([]byte("world\n"))
		if err != nil {
			log.Println(n, err)
			return
		}
	}
}
*/
// https://github.com/fatih/pool/blob/v2.0.0/channel.go

// Factory is a function to create new connections.

/*
const RedisExchangeKey="mapping"
var ExecuteLoadRequest chan bool

func InitRedisScan() {

	var (
		cursor int64
		items []string
	)

	results := make([]string, 0)

	for {
		values, err := redis.Values(conn.Do("SSCAN", key, cursor))
		if err != nil {
			return
		}

		values, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return
		}

		results = append(results, items)

		if cursor == 0 {
			break
		}
	}

}
*/
