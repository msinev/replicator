package reader

// https://github.com/fatih/pool/blob/v2.0.0/channel.go
import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/msinev/replicator/pool"
	"strconv"
	"sync"
	"time"
)

// Factory is a function to create new connections.
type RedisOperation func(redis.Conn) error

type RedisUpdates struct {
	keys []string
	//ops  []byte
}

var ExecutorGo chan RedisOperation

const RedisExchangeKey = "version"

//var ExecuteLoadRequest chan bool

func mapDBSubscribe(v int) []string {
	var ss = []string{"__keyspace@" + strconv.Itoa(v) + "__:" + RedisExchangeKey}
	return ss
}

func mapDBEventSubscribe(v int) []string {
	mdb := make([]string, len(events))
	for ie, ev := range events {
		sdbk := "__keyevent@" + strconv.Itoa(v) + "__:" + ev
		mdb[ie] = sdbk
	}
	return mdb
}

func pollLoop(addr string, wg *sync.WaitGroup, dbselect []int, dbtype map[int]string, listeners map[int]chan<- string) {
	defer wg.Done()
	//	ExecuteLoadRequest <- true

	c, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Error("REDIS error connecting to " + addr + ":" + err.Error())
		return
	}
	defer c.Close()

	defer log.Warning("Message poll Loop terminated")

	_, err = c.Do("SELECT", dbselect[0])
	if err != nil {
		log.Error("REDIS error in select " + err.Error())
		return
	}

	_, err = c.Do("config", "set", "notify-keyspace-events", "KAE")
	log.Debug("Set notify-keyspace-events KAE")
	if err != nil {
		log.Error("REDIS error in config " + err.Error())
		return
	}

	log.Info("Subscribing to REDIS")

	psc := redis.PubSubConn{c}
	//psc.Subscribe( mapDBSubscribe(dbselect)... )

	//	vsm := make([]interface{}, len(dbselect))

	var dbs []interface{} // keys to subscribe

	processors := make(map[string]func([]byte))
	mdb := make(map[string]int)

	for _, db := range dbselect {
		{
			_, ok := dbtype[db]
			chanDB := listeners[db]
			dbid := strconv.Itoa(db)
			if !ok { // versionable database
				v := mapDBSubscribe(db)
				proc := func(s []byte) {
					log.Debugf("Sending note to %s", dbid)
					chanDB <- dbid
					log.Debugf("Sent note to %s", dbid)
				}
				for _, s := range v {
					dbs = append(dbs, s)
					mdb[s] = db
					processors[s] = proc
				}
			} else {
				proc := func(b []byte) {
					s := string(b)
					log.Debugf("Sending note %s to %s ", s, dbid)
					chanDB <- s
					log.Debugf("Sent note %s to %s ", s, dbid)
				}
				v := mapDBEventSubscribe(db)
				for _, s := range v {
					dbs = append(dbs, s)
					mdb[s] = db
					processors[s] = proc
				}
			}
		}

	}

	psc.Subscribe(dbs...)

	log.Notice("Starting polling loop")

	for {
		switch n := psc.Receive().(type) {
		case redis.Message:
			log.Debugf("DB: %d Message: %s Data: %s\n", mdb[n.Channel], n.Channel, n.Data)
			//rup:=RedisUpdates{keys}
			processors[n.Channel](n.Data)
			//ExecuteLoadRequest <- true
		case redis.PMessage:
			log.Debugf("PMessage: %s %s %s\n", n.Pattern, n.Channel, n.Data)
		case redis.Subscription:
			log.Debugf("Subscription: %s %s %d\n", n.Kind, n.Channel, n.Count)
			if n.Count == 0 {
				return
			}
		case error:
			fmt.Printf("error: %v\n", n)
			return
		}
	}
}

const RedisBuf = 10

func RedisSubscriber(URL string, dbs []int, dbtype map[int]string, listeners []chan<- string) {

	defer panic("Subscriber crashed")
	rate := time.Second * 10
	throttle := time.Tick(rate)

	vsm := make(map[int]chan<- string)
	for i, v := range dbs {
		vsm[v] = listeners[i]
	}

	for {

		var wg1 sync.WaitGroup
		wg1.Add(1)
		go pollLoop(URL, &wg1, dbs, dbtype, vsm)
		wg1.Wait()
		log.Info("Subscribtion connection terminated")

		<-throttle // rate limit of our Reconnections
	}
}

func KeyUpdatePublisher(db int, listener <-chan RedisUpdates, out chan<- []PKVData) {

	defer panic("Subscriber crashed")
	rate := time.Second * 10
	throttle := time.Tick(rate)

	//vsm := make( map[int] chan<-RedisUpdates )
	var wg sync.WaitGroup
	wg.Add(1)
	for v := range listener {
		pool.ExecutorGo <- func(conn redis.Conn) error {
			defer wg.Done()
			defer log.Infof("Exiting scan function in db %d. Wait group notification.", db)
			//}
			keys := v.keys

			_, err := conn.Do("SELECT", db)
			if err != nil {
				return err
			}

			err, data := retrive(keys, conn)
			if err != nil {
				log.Error(err)
				return err
			}
			out <- data
			return nil
		}

		wg.Wait()

		log.Info("Subscribtion connection terminated")

		<-throttle // rate limit of our Reconnections
	}
}

func RedisExecutor(URL string) {
	defer panic("Executor crashed")
	rate := time.Second * 2
	throttle := time.Tick(rate)
	for {
		var wg1 sync.WaitGroup
		wg1.Add(1)

		go redisExecutorWorker(URL, &wg1)

		wg1.Wait()
		log.Info("Execution connection terminated")

		<-throttle // rate limit of our Reconnections
	}
}

func redisExecutorWorker(URL string, wg *sync.WaitGroup) {
	defer wg.Done()
	//	dbselect, _ := strconv.Atoi(Config.GetEnvDefault("DBREDIS", "103"))
	c, err := redis.Dial("tcp", URL)
	//	c.Do("SELECT", dbselect)
	if err != nil {
		log.Error("REDIS error")
		return
	}

	log.Info("Connected to REDIS")

	defer c.Close()

	for {
		foo, ok := <-ExecutorGo
		if !ok {
			log.Critical("Channel closed")
			panic("Unrecoverable channel closed")
			return
		}

		err = foo(c)
		if err != nil {
			log.Error(err)
			return
		}
	}
}
