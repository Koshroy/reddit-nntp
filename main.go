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
	subs := flag.Bool("subs", false, "get subreddits")
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

	if *subs {
		log.Println("Populating spool with subs")
		startDate, err := spool.StartDate()
		if err != nil {
			log.Fatalln("Could not fetch start date:", err)
		}
		err = spool.FetchSubreddit("VOIP", *startDate)
		if err != nil {
			log.Fatalln("Could not fetch sub:", err)
		}
		log.Println("Populated spool with sub")
		return
	}

	readerListener, err := net.Listen("tcp", "0.0.0.0:1119")
	if err != nil {
		log.Fatalln("Could not open reader listener")
	}
	defer readerListener.Close()

	log.Println("Listening on :1119")

	//go acceptorLoop(transitListener, TRANSIT_LISTENER)
	acceptorLoop(readerListener, spool)
}

func acceptorLoop(l net.Listener, spool *spool.Spool) {
	for {
		c, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting incoming connection: %v\n", err)
		}
		log.Println("Client connected")
		nc := textproto.NewConn(c)
		s := nntp.NewServer(nc, spool)
		go s.Process()
	}
}
