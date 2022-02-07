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
	updateFlag := flag.Int("update", 0, "update spool with contents of last n hours")
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

	spool, err := spool.New(*dbPath, cfg.ConcurrencyLimit, &spool.Credentials{
		ID:       cfg.BotCredentials.ID,
		Secret:   cfg.BotCredentials.Secret,
		Username: cfg.BotCredentials.Username,
		Password: cfg.BotCredentials.Password,
	})
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

	if *subs || (*updateFlag > 0) {
		if *subs && (*updateFlag > 0) {
			log.Fatalln("Cannot init and update at the same time")
		}

		if cfg.PageFetchLimit == 0 {
			log.Fatalln("PageFetchLimit not set in config, exiting.")
		}

		var fetchStart time.Time
		if *subs {
			log.Println("Populating spool with subs")
			start, err := spool.StartDate()
			if err != nil {
				log.Fatalln("Could not fetch start date:", err)
			}
			fetchStart = *start
		} else {
			log.Println("Updating spool for last", *updateFlag, "hours")
			now := time.Now()
			fetchStart = now.Add(time.Duration(-1**updateFlag) * time.Hour)
		}
		for _, sub := range cfg.Subreddits {
			log.Println("Fetching sub", sub)
			err = spool.FetchSubreddit(sub, fetchStart, cfg.PageFetchLimit, cfg.IgnoreTick)
			if err != nil {
				log.Fatalln("Could not fetch sub:", err)
			}
			err = spool.AddGroupMetadata(sub, time.Now(), 30)
			if err != nil {
				log.Fatalln("Could not add group metadata for sub", sub, ":", err)
			}
			log.Println("Finished populating subreddit", sub)
		}
		log.Println("Finished populating spool")
		return
	}

	count, err := spool.ArticleCount()
	if err != nil {
		log.Fatalln("error: spool is probably empty:", err)
	} else if count == 0 {
		log.Fatalln("spool has no articles, exiting")
	}

	readerListener, err := net.Listen("tcp", cfg.Listener)
	if err != nil {
		log.Fatalln("Could not open reader listener")
	}
	defer readerListener.Close()

	log.Println("Listening on", cfg.Listener)

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
