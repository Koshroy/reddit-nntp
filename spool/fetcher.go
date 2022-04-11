package spool

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vartanbeno/go-reddit/v2/reddit"
)

func (s *Spool) FetchSubreddit(subreddit string, startDateTime time.Time, pageFetchLimit uint, ignoreTick bool) error {
	allPosts := make([]*reddit.Post, 0)
	results := false

	ticker := time.Tick(1 * time.Second)
	for i := uint(0); i < pageFetchLimit; i++ {
		if !ignoreTick {
			<-ticker
		}

		posts, resp, err := s.client.Subreddit.NewPosts(
			context.Background(),
			subreddit,
			&reddit.ListOptions{
				Limit: 100, // max limit
			},
		)
		if len(posts) > 0 {
			allPosts = append(allPosts, posts...)
		}
		if !results {
			results = len(allPosts) > 0
		}
		if err != nil {
			if !results {
				if resp != nil {
					log.Println("got rate limit:", resp.Rate.Remaining)
				}
				return fmt.Errorf("could not fetch any posts from %s: %w", subreddit, err)
			}
			break
		}
		log.Println("Rate limit remaining:", resp.Rate.Remaining)
		if len(posts) == 0 {
			break
		}
		log.Println("Fetched", len(posts), "posts")

		minTime := posts[0].Created
		for _, p := range posts {
			if p.Created.Before(minTime.Time) {
				minTime = p.Created
			}
		}
		if startDateTime.After(minTime.Time) {
			break
		}
	}

	var wg sync.WaitGroup
	var spoolWg sync.WaitGroup
	pChan := make(chan *reddit.PostAndComments)
	spoolPCChan := make(chan *reddit.PostAndComments)
	limiter := make(chan bool, s.concLimit)
	go s.addPostAndComments(spoolPCChan, &spoolWg)
	wg.Add(len(allPosts))
	for _, p := range allPosts {
		go fetchComments(
			context.Background(),
			s.client, p, pChan, limiter,
			ticker, ignoreTick, &wg,
		)
	}
	go func() {
		wg.Wait()
		close(pChan)
	}()

	for pc := range pChan {
		spoolWg.Add(1)
		spoolPCChan <- pc
	}

	spoolWg.Wait()
	close(spoolPCChan)
	return nil
}

func fetchComments(
	ctx context.Context,
	client *reddit.Client,
	post *reddit.Post,
	pChan chan<- *reddit.PostAndComments,
	limiter chan bool,
	ticker <-chan time.Time,
	ignoreTick bool,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer func() {
		<-limiter
	}()

	limiter <- true
	if !ignoreTick {
		<-ticker
	}

	pc, _, err := client.Post.Get(ctx, post.ID)
	if err != nil {
		log.Println("Error fetching comments for post ID", post.ID, ":", err)
		return
	}
	for i := 0; i < 900; i++ {
		if pc.HasMore() {
			if !ignoreTick {
				<-ticker
			}

			_, err := client.Post.LoadMoreComments(ctx, pc)
			if err != nil {
				log.Printf("Error fetching more comments: %s\n", err)
				return
			}
		}
	}

	if pc != nil {
		log.Println("Fetched", len(pc.Comments), "comments for post ID:", post.ID)
		pChan <- pc
	}
}

func (s *Spool) addPostAndComments(pcChan chan *reddit.PostAndComments, wg *sync.WaitGroup) {
	prefix, err := s.Prefix()
	noPrefix := false
	if err != nil {
		log.Println("error getting prefix:", err)
		noPrefix = true
	}

	for pc := range pcChan {
		if noPrefix {
			wg.Done()
			continue
		}

		a := postToArticle(pc.Post, prefix)
		err = s.db.InsertArticleRecord(&a)
		if err != nil {
			log.Println("error adding reddit post to spool:", err)
			wg.Done()
			continue
		}

		commentStack := make([]*reddit.Comment, len(pc.Comments))
		copy(commentStack, pc.Comments)
		for true {
			if len(commentStack) == 0 {
				break
			}
			c := commentStack[0]
			commentStack = commentStack[1:]
			for _, c := range c.Replies.Comments {
				commentStack = append(commentStack, c)
			}
			cA := commentToArticle(c, a.Subject, prefix)
			err := s.db.InsertArticleRecord(&cA)
			if err != nil {
				log.Println("error adding reddit comment to spool:", err)
				break
			}
		}

		wg.Done()
	}
}
