package reader

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/msinev/replicator/pool"
	"github.com/msinev/replicator/sendrecv"
	"strconv"
	"sync"
)

type GlobalOptions struct {
	LocalVersion bool
	RedisURL     *string
	//	ForceCheckVersion bool
	//PlainDBS          *map[int]bool

}

type ClientOptions struct {
	DB int
	//InitVersion int64
	Plain  bool
	Global *GlobalOptions
}

type ServerOptions struct {
	DB    int
	Index int
	//InitVersion int64
	Plain  bool
	Global *GlobalOptions
}

func (o *GlobalOptions) Dump() {
	b, err := json.Marshal(o)
	if err != nil {
		log.Debug(err)
	} else {
		log.Debugf("GlobalOptions: %s", string(b))
	}
}

func WriteKeysWithVersionSquashDB(co ClientOptions, msg *sendrecv.Msg, group *sync.WaitGroup) {
	db := co.DB
	log.Infof("Squashing %d Values in db %d", len(msg.Vals), db)
	pool.ExecutorGo <- func(conn redis.Conn) error {
		var v int64
		var versionKeyTTL = 300
		var versionListKeyTTL = versionKeyTTL * 3
		if group != nil {
			defer group.Done()
		}
		_, err := conn.Do("SELECT", db)
		if err != nil {
			log.Error(err)
			return err
		}

		reset := msg.GetReset_()
		if reset {
			log.Warningf("Clearing database %d", db)
			_, err = conn.Do("FLUSHDB")
		}

		//log.Warningf("Clearing database %d", db)
		_, err = conn.Do("WATCH", versionKey)
		//EXISTS key1
		x, err := redis.Int64(conn.Do("EXISTS", versionKey))
		if err != nil {
			log.Infof("REDIS error getting existance of %s %s", versionKey, err.Error())
			return err
		}

		if err != nil {
			log.Error(err)
			return err
		}
		var nextVersion int64

		if x == 1 {
			v, err = redis.Int64(conn.Do("GET", versionKey))

			log.Infof("Writing to DB %d version %d", db, v)
			if err != nil {
				log.Error(err)
				return err
			}
			nextVersion = v + 1
		} else {
			log.Infof("Writing to DB %d starting new version chain", db)
			nextVersion = 1
		}

		versionListKey := versionKey + ":" + strconv.FormatInt(nextVersion, 10)
		err = conn.Send("MULTI")

		if err != nil {
			log.Error(err)
			return err
		}
		gcount := 0
		conn.Send("DEL", versionListKey)
		for _, vv := range msg.GetVals() {
			count, err := writeSV(conn, vv, reset)
			conn.Send("RPUSH", versionListKey, *vv.Key)
			//			log.Debugf( "REDIS PUSH %s %s", versionListKey,  *vv.Key)
			if err != nil {
				return err
			}
			gcount += count + 1
		}
		conn.Send("SETEX", versionKey, versionKeyTTL, nextVersion)
		conn.Send("EXPIRE", versionListKey, versionListKeyTTL)

		//v, err = redis.Int64(conn.Do("GET", versionKey))
		log.Noticef("Flushing %d operations", gcount)
		_, err = conn.Do("EXEC")
		if err != nil {
			log.Error(err)
			return err
		}

		/*
			err=conn.Flush()

			if err != nil {
				return err
			}

			for i:=0; i<gcount; i++ {
				_,err=conn.Receive()
				if err != nil {
					return err
				}
			}
		*/
		log.Infof("Synced DB %d", db)
		return nil
	}
}

func WriteKeysWithVersionDB(co ClientOptions, msg *sendrecv.Msg, group *sync.WaitGroup) {
	db := co.DB

	log.Infof("ValuesCount %d in db %d", len(msg.Vals), db)
	pool.ExecutorGo <- func(conn redis.Conn) error {
		if group != nil {
			defer group.Done()
		}
		_, err := conn.Do("SELECT", db)
		if err != nil {
			log.Error(err)
			return err
		}

		reset := msg.GetReset_()
		if reset {
			log.Warningf("Clearing database %d", db)
			_, err = conn.Do("FLUSHDB")
		}

		//log.Warningf("Clearing database %d", db)
		_, err = conn.Do("WATCH", versionKey)
		//EXISTS key1

		if err != nil {
			log.Error(err)
			return err
		}

		err = conn.Send("MULTI")

		if err != nil {
			log.Error(err)
			return err
		}
		gcount := 0

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
	}
}
