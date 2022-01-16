package spool

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vartanbeno/go-reddit/v2/reddit"

	"github.com/Koshroy/reddit-nntp/spool/store"
)

type Spool struct {
	db           *store.DB
	client       *reddit.Client
}

func New(fname string) (*Spool, error) {
	db, err := store.Open(fname)
	if err != nil {
		return nil, fmt.Errorf("could not open DB: %w", err)
	}

	client, err := reddit.NewReadonlyClient()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("could not open Reddit client: %w", err)
	}

	return &Spool{
		db: db,
		client: client,
	}, nil
}

func (s *Spool) Close() error {
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("error closing reddit spool: %w", s.db.Close())
	}
	return nil
}

// func (r *Spool) GetArticleByNum(subreddit string, articleNum) {

// }

func (s *Spool) Init(startDate time.Time) error {
	err := s.db.CreateNewSpool(startDate)
	if err != nil {
		return fmt.Errorf("Error initializing spool: %w", err)
	}
	return nil
}

func (s *Spool) FetchSubreddit(subreddit string, startDateTime time.Time) error {
	allPosts := make([]*reddit.Post, 0)
	results := false
	for {
		posts, _, err := s.client.Subreddit.NewPosts(
			context.Background(),
			subreddit,
			&reddit.ListOptions{
				Limit: 100, // max limit
			},
		)
		allPosts = append(allPosts, posts...)
		if !results {
			results = len(allPosts) > 0
		}
		if err != nil {
			if !results {
				return fmt.Errorf("could not fetch any posts from %s: %w", subreddit, err)
			}
			break
		}
		if len(posts) == 0 {
			break
		}

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

	postComments := make([]*reddit.PostAndComments, len(allPosts))
	for _, p := range allPosts {
		pc, _, err := s.client.Post.Get(context.Background(), p.ID)
		if err != nil {
			log.Printf("Error fetching comments for post ID %s\n", p.ID)
			continue
		}

		for i := 0; i < 900; i++ {
			if pc.HasMore() {
				_, err := s.client.Post.LoadMoreComments(context.Background(), pc)
				if err != nil {
					log.Printf("Error fetching more comments: %s\n", err)
					break
				}
			}
		}

		postComments = append(postComments, pc)
	}

	for _, pc := range postComments {
		err := s.db.AddPostAndComments(pc)
		if err != nil {
			log.Printf("error adding postcomments to spool: %v\n", err)
		}
	}

	return nil
}
