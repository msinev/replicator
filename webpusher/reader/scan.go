package reader

import (
	"github.com/garyburd/redigo/redis"
	"github.com/msinev/replicator/jsonjackson"
	"github.com/msinev/replicator/pool"

	"strconv"
	"sync"
	//	"github.com/go-kit/kit/util/conn"
	"time"
)

type GlobalOptions struct {
	RedisURL *string
}

type ServerOptions struct {
	DB    int
	Index int
	//InitVersion int64
	Plain  bool
	Global *GlobalOptions
}

type VersionData struct {
	DB          int
	Version     uint64
	DeltaFor    uint64
	VersionData []PKVData
}

func KVVersionGenerator(so ServerOptions, inc <-chan []PKVData, outc chan<- VersionData) {

	defer log.Infof("KVScanAccumulator %d terminated", so.DB)
	defer close(outc)

	//outc := client.kvPartSink[db]  -- remove channels from client's structure after debugging
	//inc :=client.kvFullScan[db] -- remove channels from client's structure after debugging

	//kvbuf:=make([]PKVData, 0, 20000)
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
type KVData struct {
	Key       string
	Value     *string
	ListValue []string
	ListKeys  []string
	TTL       *uint32
}

type PKVData = *KVData
type RetriveOperation func(string, redis.Conn) error
type ReadOperation func(int64, string, redis.Conn) (error, PKVData)

type TypeConverter struct {
	init RetriveOperation
	read ReadOperation
	tag  string
}

const maxrange = 1000000

func uselessTTL(ttl int64) bool {
	return ttl == -2 || ttl >= 0 && ttl < 100
}

func KVSplitter(Subscriber <-chan chan<- *KVData, Source <-chan *KVData, Abort chan<- int) {
	subscribers := make(map[int]chan<- *KVData)

	defer close(Abort)
	count := 0
	loopDone := false

	for !loopDone {
		select {
		case newMsg, ok := <-Source:
			{
				if !ok {
					loopDone = true
					break
				}

				var closemeset []int
				for k, v := range subscribers {
					select {
					case v <- newMsg:
						log.Infof("Sending messages to %d subscriber", k)
					default:
						closemeset = append(closemeset, k)
					}
				}

				for _, v := range closemeset {
					close(subscribers[v])
					delete(subscribers, v)
					log.Infof("Closing %d subscriber", v)
				}
				closemeset = nil

			}
		case newConsumer, ok := <-Subscriber:
			if !ok {
				loopDone = true
				break
			}
			count++
			log.Infof("Appending new %d consumer", count)
			subscribers[count] = newConsumer
		}

	}
	log.Warningf("Closing %d subscribers from splittter", len(subscribers))
	for _, v := range subscribers {
		close(v)
	}
}

const versionKey = "version"
const versionKeyList = "version:"

func (kv *KVData) Write(i *jsonjackson.JSONWriter) {
	i.BeginObject().StrField("k", kv.Key).Field("v")
	if kv.ListValue == nil {
		if kv.ListKeys == nil {
			i.NulStr(kv.Value)
		} else {
			i.BeginObject().StrField("k", kv.Key).Field("v").BeginArray()
			for _, v := range kv.ListKeys {
				i.Str(v)
			}
			i.EndArray().BoolField("s", true)
		}
	} else {
		if kv.ListKeys == nil {
			i.BeginObject().StrField("k", kv.Key).Field("v").BeginArray()
			for _, v := range kv.ListValue {
				i.Str(v)
			}
			i.EndArray().BoolField("s", false)

		} else {
			i.BeginObject().StrField("k", kv.Key).Field("v").BeginArray()
			for ind, v := range kv.ListKeys {
				i.BeginArray().Str(v).Str(kv.ListValue[ind]).EndArray()
			}

		}
	}

	if kv.TTL != nil {
		i.IntField("t", int64(*kv.TTL))
	}

	i.EndObject()
}

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
				existingKeys = make([] PKVData, 200)
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

			nextVersion, err := redis.Uint64(conn.Do("GET", versionKey))
			log.Noticef("Current next version %d", nextVersion)
			if nowVersion < nextVersion {
				newKeys := make(map[string]uint64)
				existingKeyIndexes := make(map[string]int)
				existingKeys := make([]PKVData, 0)
				for nowVersion < nextVersion {
					//existingKeysCount:=len(existingKeyIndexes)
					versionListTTL, err := redis.Int(conn.Do("TTL", versionKeyList+strconv.FormatUint(fromVersion, 10)))
					if err != nil {
						return err
					}

					if versionListTTL == -2 {
						return nil
					}

					for i := nowVersion + 1; i <= nextVersion; i++ {
						lrKey := versionKeyList + strconv.FormatUint(i, 10)
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

func retriveKeys(existingKeyIndexes map[string]int, existingKeys []PKVData, keys []string,
	conn redis.Conn) (map[string]int, []PKVData, error) {
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

func retrive(keys []string, conn redis.Conn) (error, []PKVData) { // for KeyReader

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
	databuf := make([]PKVData, len(keys))
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

func KeyReader(db int, inp <-chan []string, out chan<- []PKVData) {

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

		nextVersion, err := redis.Uint64(conn.Do("GET", versionKey))
		log.Noticef("Current next version %d", nextVersion)
		if nowVersion < nextVersion {
			//nextVersion, err := redis.Uint64(conn.Do("EXISTS", versionKey))
			newKeys := make(map[string]uint64)
			existingKeyIndexes := make(map[string]int)
			existingKeys := make([]PKVData, 0)
			for nowVersion < nextVersion {
				//existingKeysCount:=len(existingKeyIndexes)

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
			out <- VersionData{DeltaFor: fromVersion, Version: nextVersion, VersionData: make([]PKVData, 0)}
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
		var data []PKVData

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
