package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/msinev/replicator/clientserver"
	"github.com/msinev/replicator/compressor"
	"github.com/msinev/replicator/reader"
	"net"
	"os"
	"strconv"
	"sync"
)

func mainClientTCP(remote string, co reader.GlobalOptions) {
	// Listen for incoming connections.
	conn, err := net.Dial("tcp", remote)
	if err != nil {
		fmt.Println("Error connecting:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.

	defer conn.Close()
	fmt.Println("Connected to " + remote)
	//	var wg sync.WaitGroup
	//wg.Add(1)
	// Handle connections in a new goroutine.
	handleClient(conn, co)
}

func ClientDBMsgRouting(inCh <-chan []byte, dbout int, co reader.ClientOptions, term *sync.WaitGroup) {
	defer term.Done()
	for msgbuf := range inCh {
		var wg1 sync.WaitGroup

		wg1.Add(1)
		rdr := bytes.NewReader(msgbuf)

		msg := readPB(rdr)
		msgbuf = nil
		if co.Global.LocalVersion {
			reader.WriteKeysWithVersionSquashDB(co, msg, &wg1)
		} else {
			reader.WriteKeysWithVersionDB(co, msg, &wg1)
		}

		wg1.Wait()
	}
}

const DBQ = 1000

func handleClient(conn net.Conn, gco reader.GlobalOptions) {
	defer conn.Close()

	ldbs := len(DBS)
	var terminator sync.WaitGroup

	/*
		Prepare infrastructure pipes for pipelining
	*/

	decompressed := make([]chan []byte, ldbs)
	assembled := make([]chan []byte, ldbs)
	received := make([]chan []byte, ldbs)

	for rk := range DBS {
		decompressed[rk] = make(chan []byte)
		assembled[rk] = make(chan []byte)
		received[rk] = make(chan []byte, DBQ)
	}

	newDecompressSync := make([]sync.WaitGroup, ldbs)

	/*
	   Commutate workers
	*/
	buffer := make([]byte, 0, len(DBS)*16)
	buffer = append(buffer, clientserver.VersionsHeader...)
	buffer = append(buffer, ':')
	for rk, rv := range DBS {
		ver, dbver := reader.GetVersion(rv)
		_, ok := DBSPlain[rv]
		if !ok && !dbver {
			log.Errorf("Database %d missing version", rv)
		} else if dbver {
			log.Infof("Database %d starting version %d", rv, ver)
		}
		//!!!
		if rk != 1 {
			buffer = append(buffer, ',')
		}
		buffer = strconv.AppendInt(buffer, int64(ver), 10)
		co := reader.ClientOptions{Global: &gco, DB: rv}
		decompressSync := &newDecompressSync[rk]

		newDecompressChans := make([]chan []byte, len(compressor.Decompressors))
		for di, decompressorFunc := range compressor.Decompressors {
			newDecompressChans[di] = make(chan []byte)
			go decompressorFunc(newDecompressChans[di], decompressed[rk], decompressSync)
		}

		go compressor.ClientBlockAssembly(received[rk], newDecompressChans, decompressSync, rv, decompressed[rk])
		terminator.Add(1)
		go ClientDBMsgRouting(decompressed[rk], rv, co, &terminator) // ClientDBMsgRouting(inCh <- chan []byte, dbout int,
		// co Reader.ClientOptions, term *sync.WaitGroup)
	}
	buffer = append(buffer, '\n')
	buffer = append(buffer, '\n')

	r := bufio.NewReader(conn)

	go func() {
		n, err := conn.Write([]byte("RR version 1.0\n"))
		if err != nil {
			log.Error(n, err)
		}
		n2, err := conn.Write(buffer)
		if err != nil {
			log.Error(n2, err)
		}
	}()

	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			log.Error(err)
			return
		}
		if len(line) == 1 && line[0] == '\n' {
			break
		}
		log.Debug(string(line))
	}
	terminator.Add(1)
	clientserver.ClientBlockReader(r, received, &terminator)
	log.Info("Waiting for terminator")
	terminator.Wait()
	log.Info("Terminating... ")
}
