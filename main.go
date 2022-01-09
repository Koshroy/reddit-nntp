package main

import (
	"context"
	"log"
	"net"
	"net/textproto"

	"github.com/Koshroy/reddit-nntp/nntp"
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

func main() {
	readerListener, err := net.Listen("tcp", "0.0.0.0:1119")
	if err != nil {
		log.Fatalln("Could not open reader listener")
	}
	defer readerListener.Close()

	log.Println("Listening on :1119")

	//go acceptorLoop(transitListener, TRANSIT_LISTENER)
	acceptorLoop(readerListener)

	client, err := reddit.NewReadonlyClient()
	if err != nil {
		log.Fatalln("Could not initialize reddit client")
	}

	posts, _, err := client.Subreddit.TopPosts(context.Background(), "golang", &reddit.ListPostOptions{
		ListOptions: reddit.ListOptions{
			Limit: 5,
		},
		Time: "all",
	})
	if err != nil {
		log.Fatalln("Received error when getting sub", err)
	}

	log.Printf("Received %d posts.\n", len(posts))
	for _, p := range posts {
		log.Printf("[%d] - %s\n", p.Score, p.Title)
	}
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
