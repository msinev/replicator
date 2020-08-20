package reader

import (
	"github.com/emirpasic/gods/maps/treemap"

	"github.com/cespare/xxhash"
	"strings"

	"bytes"
	"encoding/binary"
	"github.com/emirpasic/gods/sets/treeset"
	"strconv"
)

/*
type KV struct {
	Key string
	value string
}
*/

type KHV struct {
	Key  string
	Hash uint64
	Size uint32
}

type ReceiverData struct {
	queue      *treeset.Set
	keyMap     *treemap.Map
	keyRemoved *treeset.Set
}

type DeltaReceiver struct {
	DB           int
	DBtype       int
	StartVersion uint64
	//
	DrainVer <-chan *VersionData // for final database replication
	//
	//
	NotifyVer chan<- string // for versionable database replication
	pubsubver *PubSubVersion
	//
	//
	pubsubstr   *PubSubStr
	DrainString chan<- string // for non-versionless databases replication
}

func (r *DeltaReceiver) SubscribeVersions(sname string) <-chan *VersionData {

	ch := make(chan *VersionData, 1)
	r.pubsubver.AddSubscriber(ch, sname)

	return ch

}

func (r *DeltaReceiver) SubscribeKeys(sname string) <-chan string {
	ch := make(chan string)
	r.pubsubstr.AddSubscriber(ch, sname)
	return ch
}

const srQueue = 10

func (r *DeltaReceiver) Init(so ServerOptions, dbt int, ver uint64) {
	DB := so.DB
	r.DB = DB
	r.DBtype = dbt
	r.StartVersion = ver

	ch2 := make(chan string)
	ch1 := make(chan *VersionData)
	r.DrainVer = ch1

	r.pubsubver = &PubSubVersion{}
	r.pubsubver.InitPubSub(r.DrainVer, "RV "+strconv.Itoa(DB))

	if dbt == 0 {
		r.NotifyVer = ch2
		go ReadVersionDelta(so, ch2, ch1, ver) // start delta readers

	} else {

		r.pubsubstr = &PubSubStr{}
		r.pubsubstr.InitPubSub(ch2, "RS "+strconv.Itoa(DB))
		r.DrainString = ch2

	}
}

func (r *DeltaReceiver) InitPlain(so ServerOptions, dbt int, RedisURL string) {
	DB := so.DB
	r.DB = DB
	r.DBtype = dbt
	r.StartVersion = 0

	ch2 := make(chan string)
	ch1 := make(chan *VersionData)
	r.DrainVer = ch1

	r.pubsubver = &PubSubVersion{}
	r.pubsubver.InitPubSub(r.DrainVer, "RV "+strconv.Itoa(DB))

	if dbt == 0 {
		r.NotifyVer = ch2
		go ReadPlainDelta(so, ch2, ch1) // start delta readers

	} else {

		r.pubsubstr = &PubSubStr{}
		r.pubsubstr.InitPubSub(ch2, "RS "+strconv.Itoa(DB))
		r.DrainString = ch2

	}
}

func bySizeAndKey(a, b interface{}) int {
	// Type assertion, program will panic if this is not respected
	c1 := a.(KHV)
	c2 := b.(KHV)

	switch {
	case c1.Size < c2.Size:
		return 1
	case c1.Size > c2.Size:
		return -1
	case c1.Hash < c2.Hash:
		return 1
	case c1.Hash > c2.Hash:
		return -1
	default:
		return strings.Compare(c1.Key, c2.Key)
	}
}

func (r *ReceiverData) Init(DB int) {
	r.queue = treeset.NewWith(bySizeAndKey)
	r.keyMap = treemap.NewWithStringComparator()
	r.keyRemoved = treeset.NewWithStringComparator()
}

func (r *ReceiverData) Add(k string, v string) {
	if r.keyRemoved.Contains(k) {
		r.keyRemoved.Remove(k)
	} else {
		pk := KHV{Key: k, Hash: xxhash.Sum64String(v), Size: uint32(len(v))}
		r.queue.Add(pk)
	}
}

func (r *ReceiverData) Delete(k string) {
	pkv, found := r.keyMap.Get(k)
	if found {
		r.queue.Remove(pkv)
	} else {
		r.keyRemoved.Remove(k)
	}
}

func (r *ReceiverData) Consume(buf *bytes.Buffer, maxChunk int) uint32 {

	da := make([]*KHV, 0)
	qi := r.queue.Iterator()

	var c uint32
	c = 0
	maxc := uint32(maxChunk)

	for qi.Next() && c < maxc {
		value := qi.Value().(KHV)
		da = append(da, &value)
		c++
	}

	binary.Write(buf, binary.BigEndian, c)

	for _, v := range da {
		//
		//		msg:=sendrecv.Msg{}
		binary.Write(buf, binary.BigEndian, v.Key)
		binary.Write(buf, binary.BigEndian, v.Hash)
		//
		r.keyMap.Remove(v.Key)
		r.queue.Remove(*v)
		//
	}

	return c
}
