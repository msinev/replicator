package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

func mainTLS() {
//	log.SetFlags(log.Lshortfile)

	cer, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		log.Error(err)
		return
	}

	capool := x509.NewCertPool()
	cacert, err := ioutil.ReadFile("ca.pem")
	//checkError(err, "loadCACert")
	capool.AppendCertsFromPEM(cacert)

	config := &tls.Config{
		Certificates: []tls.Certificate{cer},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: capool,
		//ClientCAs *x509.CertPool
	}
	ln, err := tls.Listen("tcp", ":8443", config)
	if err != nil {
		log.Error(err)
		return
	}
	defer ln.Close()

	for {
		theconn, err := ln.Accept()
		conn, ok := theconn.(*tls.Conn)
		if(ok) {
			state:= conn.ConnectionState()
			if(len(state.PeerCertificates)>0) {
				log.Info(state.PeerCertificates[0].Subject.CommonName)
			}
			if err != nil {
				log.Error(err)
				continue
			}
	//		go handleServer(conn, nil)
		}
	}
}

func mainTLSClient() {
	//log.SetFlags(log.Lshortfile)

	conf := &tls.Config{
		InsecureSkipVerify: true,
	}


	conn, err := tls.Dial("tcp", "127.0.0.1:443", conf)
	if err != nil {
		log.Error(err)
		return
	}
	defer conn.Close()

	n, err := conn.Write([]byte("hello\n"))
	if err != nil {
		log.Error(n, err)
		return
	}

	buf := make([]byte, 100)
	n, err = conn.Read(buf)
	if err != nil {
		log.Error(n, err)
		return
	}

	println(string(buf[:n]))
}