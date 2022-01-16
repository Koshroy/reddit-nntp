package main

import (
	"flag"
	"log"
	"net"
	"net/textproto"
	"time"

	"github.com/Koshroy/reddit-nntp/nntp"
	"github.com/Koshroy/reddit-nntp/spool"
)

func main() {
	initFlag := flag.Bool("init", false, "initialize the database")
	flag.Parse()

	spool, err := spool.New("/tmp/spool.db")
	if err != nil {
		log.Fatalln("Could not open spool:", err)
	}
	if *initFlag {
		err = spool.Init(time.Now().Add(-24 * 7 * time.Hour))
		if err != nil {
			log.Fatalln("Could not initialize spool:", err)
		}
		log.Println("Initialized database")
		return
	}

	readerListener, err := net.Listen("tcp", "0.0.0.0:1119")
	if err != nil {
		log.Fatalln("Could not open reader listener")
	}
	defer readerListener.Close()

	log.Println("Listening on :1119")

	//go acceptorLoop(transitListener, TRANSIT_LISTENER)
	acceptorLoop(readerListener)
}

func acceptorLoop(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting incoming connection: %v\n", err)
		}
		log.Println("Client connected")
		nc := textproto.NewConn(c)
		s := nntp.NewServer(nc)
		go s.Process()
	}
}
