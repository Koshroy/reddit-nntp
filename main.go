package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"time"

	"github.com/Koshroy/reddit-nntp/config"
	"github.com/Koshroy/reddit-nntp/nntp"
	"github.com/Koshroy/reddit-nntp/spool"
)

func main() {
	var defaultSpool string
	var defaultConfig string
	home, err := os.UserHomeDir()
	if err == nil {
		defaultSpool = filepath.Join(home, ".config", "reddit-nntp", "spool.db")
		defaultConfig = filepath.Join(home, ".config", "reddit-nntp", "config.toml")
	}

	initFlag := flag.Bool("init", false, "initialize the database")
	prefix := flag.String("prefix", "reddit", "prefix used on spool initialization")
	dbPath := flag.String("db", defaultSpool, "path to sqlite database")
	configPath := flag.String("conf", defaultConfig, "path to config file")
	subs := flag.Bool("subs", false, "get subreddits")
	flag.Parse()

	if *configPath == "" || *dbPath == "" {
		log.Fatalln("config path and spool path must be specified")
	}

	cfg, err := config.ParseFile(*configPath)
	if err != nil {
		log.Fatalln("could not parse config file:", err)
	}

	spool, err := spool.New(*dbPath, cfg.ConcurrencyLimit)
	if err != nil {
		log.Fatalln("Could not open spool:", err)
	}
	defer spool.Close()
	if *initFlag {
		err = spool.Init(time.Now().Add(-24*7*time.Hour), *prefix)
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

	readerListener, err := net.Listen("tcp", cfg.Listener)
	if err != nil {
		log.Fatalln("Could not open reader listener")
	}
	defer readerListener.Close()

	log.Println("Listening on", cfg.Listener)

	//go acceptorLoop(ctx, transitListener, TRANSIT_LISTENER)
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
		go s.Process(context.Background())
	}
}
