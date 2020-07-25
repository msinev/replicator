package clientserver

import (
	"bufio"

	"encoding/binary"
	"github.com/msinev/replicator/compressor"
	"io"
	"sync"
)

func ClientBlockReader(reader *bufio.Reader, routers []chan []byte, termimator *sync.WaitGroup) {
	defer compressor.CloseByteChan(routers, "Closing routers")
	defer termimator.Done()

	lrouters := uint(len(routers))
	bytecount := uint64(0)

	for {
		block := make([]byte, compressor.CommDataBlockSize+compressor.CommHeaderSize)
		c, err := io.ReadFull(reader, block[:compressor.CommHeaderSize])

		if err != nil {
			if err == io.EOF {
				log.Info("Connection Reader closed")
				break
			}
			log.Panic(err)
			return
		} else if c != compressor.CommHeaderSize {
			log.Panic("Header not readable")
			return
		}
		currentBlock := binary.LittleEndian.Uint16(block)
		pipe := uint(block[2])

		if currentBlock > compressor.CommDataBlockSize {
			log.Fatal("Block size beyond defined")

			return
		}

		cb, err := io.ReadFull(reader, block[compressor.CommHeaderSize:compressor.CommHeaderSize+currentBlock])

		if err != nil {
			log.Fatal(err)
			return
		} else if cb != int(currentBlock) {
			log.Fatal("Data not readable")
			return
		}

		bytecount += uint64(currentBlock)

		if pipe > lrouters {
			log.Warning("Pipe data beyond defined routers, ignoring message")
			continue
		}
		/*
			log.Noticef("Routing %d bytes of %d message to %d router (offset %d for %d remaining message)",
				int(currentBlock), int(block[3]), int(pipe),
				int(binary.LittleEndian.Uint32( block[4:] )), int(binary.LittleEndian.Uint32( block[8:] )))
		*/
		//log.Debugf(" -->> !! Routing %d bytes in pipe %d after reading %d ", currentBlock,  pipe , bytecount)

		routers[pipe] <- block[:compressor.CommHeaderSize+currentBlock]

		//		msg:=block[3]
	}

}
