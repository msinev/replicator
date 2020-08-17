package reader

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func Drain(C <-chan PKVData, t *testing.T, id string, w *sync.WaitGroup) {
	if w != nil {
		defer w.Done()
	}
	n := 0
	for v := range C {
		n++
		t.Logf("Drain %s received %s", id, *v.Value)
	}
	t.Logf("Drain %s count %d", id, n)
}

func RunSrc(C chan<- PKVData, t *testing.T, nMax int, id string, w *sync.WaitGroup) {
	if w != nil {
		defer w.Done()
	}
	for v := 0; v < nMax; {
		v++
		t.Logf("Sending %s count %d", id, v)
		val := strconv.Itoa(v)
		C <- &KVData{
			Key:       val,
			Value:     &val,
			ListValue: nil,
			ListKeys:  nil,
			TTL:       nil,
		}
		t.Logf("Sent %s count %d", id, v)
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSplit(t *testing.T) {
	var wgDst sync.WaitGroup
	Src := make(chan PKVData)
	Abort := make(chan int)
	Subscribers := make(chan chan<- PKVData)

	go KVSplitter(Subscribers, Src, Abort)
	for i := 0; i < 30; i++ {
		wgDst.Add(1)
		D1 := make(chan PKVData, 5)
		go Drain(D1, t, "Dst-"+strconv.Itoa(i), &wgDst)
		Subscribers <- D1
	}

	RunSrc(Src, t, 20, "S1", nil)

	close(Src)

	<-Abort

	wgDst.Wait()

}
