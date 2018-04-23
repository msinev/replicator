package Compressor

import (
	"sync"
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"github.com/cxuhua/lzma"
)

func GZIPDecompressor(inCh <-chan []byte, out chan<- []byte, wg *sync.WaitGroup) {

	offset := int64(0)
	for rd:= range inCh {
		h,d:= GetInnerHeader(rd)
		ld := len(d)
		offset = offset + int64(ld)
		log.Infof("GZIP Data Latency %d Offset %d + %d ", int(JavaTSNow()-h.TS), offset, ld)

		br := bytes.NewReader(d)
		r, e := gzip.NewReader(br)

		if(e!=nil) {
			log.Panic(e)
			defer wg.Done()
			return
		}
		u,e:=ioutil.ReadAll(r)
		r.Close()
		if(e!=nil) {
			log.Panic(e)
			defer wg.Done()
			return
		} else {
			out<-u
			wg.Done()
		}
	}
}



func LZMADecompressor(inCh <-chan []byte, out chan<- []byte, wg *sync.WaitGroup) {

	for rd:= range inCh {
		h,d:= GetInnerHeader(rd)
		log.Infof("LZMA Data Latency %d ms", int(JavaTSNow()-h.TS))
		u,e:=lzma.Uncompress(d)
		if(e!=nil) {
			log.Panic(e)
			wg.Done()
			return
		} else {
			out<-u
			wg.Done()
		}
	}
}

func ZeroDecompressor(inCh <-chan []byte, out chan<- []byte, wg *sync.WaitGroup) {
	for rd:= range inCh {
		h,d:= GetInnerHeader(rd)
		log.Infof("ZeroComp Data Latency %d", int(JavaTSNow()-h.TS))

		out<-d
		wg.Done()
	}
}


var Decompressors = [...]func( <-chan []byte, chan<- []byte, *sync.WaitGroup ) {
	ZeroDecompressor,
	LZMADecompressor,
	GZIPDecompressor,
}

func CloseByteChan(out []chan []byte, name string) {
	log.Info(name)
	for _,vout := range out {
		close(vout)
	}
}

func ClientBlockAssembly(inCh <-chan []byte, out []chan []byte, wg *sync.WaitGroup, db int, voidCh chan []byte ) {
	//defer CloseByteChan(out, "Exiting assembly DB:"+ strconv.Itoa(db))
	defer func() {
		close(voidCh)
	}()

	for {

		block, ok := <-inCh
		if(!ok) {
			log.Infof("Channel %d assembly terminating at correct place", db)
			return
		}
		header0:=GetHeader(block)
		//log.Noticef("Starting Assembling %d bytes of %d message to %d router (Offset %d for %d remaining message)",
		//header0.BlockSize, header0.Msg, header0.Queue, header0.Offset,  header0.RestSize)

		if(header0.Offset !=0) {
			log.Panic("Initial Offset not zero") // not for UDP
		}



		assembling:=make([]byte, header0.RestSize)
		remaining:=int(header0.RestSize)

		header:=header0
		for {


			copy(assembling[header.Offset:], block[CommHeaderSize:header.BlockSize+CommHeaderSize])
			remaining-=int(header.BlockSize)
			//log.Noticef("Assembling %d bytes of %d message to %d router (Offset %d for %d/%d remaining message)",
			//	header0.BlockSize, header0.Msg, header0.Queue, header0.Offset,  header0.RestSize, remaining)

			if(remaining>0) {
				block, ok = <-inCh
				header=GetHeader(block)
				if(!ok) {
					log.Errorf("Channel %d assembly terminating at incorrect place", db)
					return
				}

			} else {
				break;
			}
		}

		compression:=GetInnerCompression(assembling)

		wg.Add(1)
		log.Noticef("Sending DB/I %d/%d for decompression\n", db, int(header0.Queue))
		out[compression] <- assembling
		wg.Wait()
		log.Noticef("DB/I %d/%d for decompression done\n", db, int(header0.Queue))
	}
}

