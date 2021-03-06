package reader

import (
	"github.com/garyburd/redigo/redis"
	"github.com/msinev/replicator/protobuf/sendrecv"
)

func retriveString(keys string, conn redis.Conn) error {
	return conn.Send("GET", keys)
}

func readString(ttl int64, keys string, conn redis.Conn) (error, RedisKV) {
	//	log.Println("Reading value of key " + keys)
	reply, err := redis.String(conn.Receive())

	if err == nil {
		var ttlcode *uint32
		if ttl > 0 {
			uttl := uint32(ttl)
			ttlcode = &uttl
		} else {
			ttlcode = nil
		}

		//		conn.Send("GETTTL", keys)
		return nil, &sendrecv.Msg_SendValues{Value: &sendrecv.Msg_SendValues_Strvalue{reply}, Key: &keys, TTL: ttlcode}
	}

	return err, nil
}

func retriveList(keys string, conn redis.Conn) error {
	return conn.Send("LRANGE", keys, 0, maxrange)
}

func retriveSet(keys string, conn redis.Conn) error {
	return conn.Send("SMEMBERS", keys)
}

func retriveZSet(keys string, conn redis.Conn) error {
	return conn.Send("ZRANGE", keys, 0, -1)
}

func retriveHash(keys string, conn redis.Conn) error {
	return conn.Send("HGETALL", keys)
}

func readList(ttl int64, keys string, conn redis.Conn) (error, RedisKV) {
	//log.Println("Reading list " + keys)
	reply, err := redis.Strings(conn.Receive())

	if err == nil {
		var ttlcode *uint32
		if ttl > 0 {
			uttl := uint32(ttl)
			ttlcode = &uttl
		} else {
			ttlcode = nil
		}

		//		conn.Send("GETTTL", keys)
		atrue := true
		vset := sendrecv.Msg_DataSet{Value: reply, Ordered: &atrue}
		return nil, &sendrecv.Msg_SendValues{Value: &sendrecv.Msg_SendValues_Listvalue{&vset}, Key: &keys, TTL: ttlcode}
	}

	return err, nil
}

func readSet(ttl int64, keys string, conn redis.Conn) (error, RedisKV) {
	log.Notice("Reading set " + keys)
	reply, err := redis.Strings(conn.Receive())
	setOrdered := false

	if err == nil {
		var ttlcode *uint32
		if ttl > 0 {
			uttl := uint32(ttl)
			ttlcode = &uttl
		} else {
			ttlcode = nil
		}

		//		conn.Send("GETTTL", keys)
		vset := sendrecv.Msg_DataSet{Value: reply, Ordered: &setOrdered}
		return nil, &sendrecv.Msg_SendValues{Value: &sendrecv.Msg_SendValues_Setvalue{&vset}, Key: &keys, TTL: ttlcode}
	}
	return err, nil
}

func readZSet(ttl int64, keys string, conn redis.Conn) (error, RedisKV) {
	log.Notice("Reading zset " + keys)
	reply, err := redis.Strings(conn.Receive())
	setOrdered := true

	if err == nil {
		var ttlcode *uint32
		if ttl > 0 {
			uttl := uint32(ttl)
			ttlcode = &uttl
		} else {
			ttlcode = nil
		}

		//		conn.Send("GETTTL", keys)
		vset := sendrecv.Msg_DataSet{Value: reply, Ordered: &setOrdered}
		return nil, &sendrecv.Msg_SendValues{Value: &sendrecv.Msg_SendValues_Setvalue{&vset}, Key: &keys, TTL: ttlcode}
	}
	return err, nil
}

func readHash(ttl int64, keys string, conn redis.Conn) (error, RedisKV) {
	reply, err := redis.Strings(conn.Receive())
	lmap := len(reply) / 2
	mapvals := make([]string, lmap)
	mapkeys := make([]string, lmap)
	for k := 0; k < lmap; k++ {
		mapvals[k] = reply[k*2+1]
		mapkeys[k] = reply[k*2]
	}

	if err == nil {
		var ttlcode *uint32
		if ttl > 0 {
			uttl := uint32(ttl)
			ttlcode = &uttl
		} else {
			ttlcode = nil
		}

		//		conn.Send("GETTTL", keys)

		vset := sendrecv.Msg_KeyValueMap{Keys: mapkeys, Values: mapvals}
		return nil, &sendrecv.Msg_SendValues{Value: &sendrecv.Msg_SendValues_Mapvalue{&vset}, Key: &keys, TTL: ttlcode}
	}

	return err, nil

}

var retriveFunctions = map[string]*TypeConverter{
	"string": &TypeConverter{retriveString, readString, "string"},
	"list":   &TypeConverter{retriveList, readList, "list"},
	"set":    &TypeConverter{retriveSet, readSet, "set"},
	"zset":   &TypeConverter{retriveZSet, readZSet, "zset"},
	"hash":   &TypeConverter{retriveHash, readHash, "hash"},
}
