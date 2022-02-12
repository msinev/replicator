package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/msinev/replicator/pool"
	"net/http"
	"log"
	"strconv"
	"sync"
)
import "github.com/msinev/replicator/webpusher/reader"

/*
{ "key": "k1", "sval": "sval", slval: ["sval1", "val2"], "ttl": int }
 */

type Kv struct {
	Key   string      `json:"k"`
	Val  string       `json:"v"`
}

/* Atomic API */
type RKey struct {
	Key   string      `json:"key"`
	Sval  string      `json:"sv,omitempty"`
	Slval []string    `json:"lv,omitempty"`
	Hval  []*Kv       `json:"hv,omitempty"`
	Ttl   *uint32 	  `json:"ttl,omitempty"`
}

type APIResult struct {
	Rc    int         `json:"code"`
	Error  string     `json:"err,omitempty"`
	Keys   []RKey 	  `json:"keys,omitempty"`
}
func Reader2APIVal(data reader.KVData) *RKey {
	if data.Value!=nil {
		return &RKey{
			Key:   data.Key,
			Slval: data.ListValue,
			Ttl:   data.TTL,
		}
	} else if len(data.ListKeys)>0 {
		return &RKey{
			Key:   data.Key,
			Slval: data.ListValue,
			Ttl:   data.TTL,
		}
	} else {
		return &RKey{
			Key:   data.Key,
			Slval: data.ListValue,
			Ttl:   data.TTL,
		}
	}
}

func sendRequest(operation pool.RedisOperation) {

	pool.ExecutorGo <- operation

}

func GetKey(w http.ResponseWriter, r *http.Request) {


}

func CheckAndSet(w http.ResponseWriter, r *http.Request) {

}

func CheckAndSetBatch(w http.ResponseWriter, r *http.Request) {

}


func SetAKey(w http.ResponseWriter, r *http.Request) {

	key:=r.FormValue("key");
	if(key=="") {
		log.Println("Error key must be provided")
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("422 - not parsable"))
		return
	}

	db:=r.FormValue("db");
	if(db=="") {
		db="0"
		}

	versionKey:=r.FormValue("vk");

	if(versionKey=="") {
		versionKey="version"
		}
	var group *sync.WaitGroup
	val:=r.FormValue("val");

	sendRequest( func(conn redis.Conn) error {
		var v int64
		var versionKeyTTL=300
		var versionListKeyTTL=versionKeyTTL*3

		if (group != nil) {
			defer group.Done()
			}

		_, err := conn.Do("SELECT", db)
		if err != nil {
			log.Error(err)
			return err
		}

		//log.Warningf("Clearing database %d", db)
		_, err = conn.Do("WATCH", versionKey)
		//EXISTS key1
		x,err :=redis.Int64(conn.Do("EXISTS", versionKey))
		if err!=nil {
			log.Infof( "REDIS error getting existance of %s %s", versionKey,  err.Error())
			return err
		}

		if err != nil {
			log.Error(err)
			return err
		}
		var nextVersion int64

		if(x==1) {
			v, err = redis.Int64(conn.Do("GET", versionKey))

			log.Infof("Writing to DB %d version %d", db, v)
			if err != nil {
				log.Error(err)
				return err
			}
			nextVersion=v+1
		} else {
			log.Infof("Writing to DB %d starting new version chain", db)
			nextVersion=1
		}

		versionListKey:=versionKey+":"+strconv.FormatInt(nextVersion,10)
		err = conn.Send("MULTI")

		if err != nil {
			log.Error(err)
			return err
		}		gcount := 0

		missingVersionKey := true
		for _, vv := range msg.GetVals() {
			if *vv.Key == versionKey {
				missingVersionKey = false
			}
			count, err := writeSV(conn, vv, reset)

			//			log.Debugf( "REDIS PUSH %s %s", versionListKey,  *vv.Key)
			if err != nil {
				return err
			}
			gcount += count + 1
		}

		if missingVersionKey {
			log.Errorf("Error in %d - update without version - deleting", db)
			err = conn.Send("DEL", versionKey)
			if err != nil {
				log.Error(err)
				return err
			}
		}

		log.Noticef("Flushing %d operations", gcount)
		_, err = conn.Do("EXEC")
		if err != nil {
			log.Error(err)
			return err
		}

		log.Infof("Completed DB %d", db)
		return nil
	})

}

func SetSetKey(w http.ResponseWriter, r *http.Request) {

}

func SetSetKey(w http.ResponseWriter, r *http.Request) {

}
