package spool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/vartanbeno/go-reddit/v2/reddit"

	"github.com/Koshroy/reddit-nntp/spool/store"
)

type Spool struct {
	db          *store.DB
	client      *reddit.Client
	startDate   *time.Time
	timeFetched bool
}

const nntpTimeFormat = "02 Jan 2006 15:04 -0700"

type Header struct {
	PostedAt  time.Time
	Newsgroup string
	Subject   string
	Author    string
	MsgID     string
	References  []string
}

func (h Header) Bytes() bytes.Buffer {
	var buf bytes.Buffer

	buf.WriteString("Path: reddit!not-for-mail\n")
	buf.WriteString("From: ")
	buf.WriteString(h.Author)
	buf.WriteRune('\n')
	buf.WriteString("Newsgroups: ")
	buf.WriteString(h.Newsgroup)
	buf.WriteRune('\n')
	buf.WriteString("Subject: ")
	buf.WriteString(h.Subject)
	buf.WriteRune('\n')
	buf.WriteString("Date: ")
	buf.WriteString(h.PostedAt.Format(nntpTimeFormat))
	buf.WriteRune('\n')
	buf.WriteString("Message-ID: ")
	buf.WriteString(h.MsgID)
	buf.WriteRune('\n')
	if len(h.References) > 0 {
		buf.WriteString("References: ")
		for i, ref := range h.References {
			if i > 0 {
				buf.WriteString(",")	
			}
			buf.WriteString(ref)
		}
	}
	buf.WriteRune('\n')

	return buf
}

type Article struct {
	Header Header
	Body   []byte
}

func (a Article) Bytes() bytes.Buffer {
	var buf bytes.Buffer

	hdrBytes := a.Header.Bytes()
	buf.ReadFrom(&hdrBytes)
	buf.WriteRune('\n')
	buf.Write(a.Body)

	return buf
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

	now := time.Now()
	return &Spool{
		db:          db,
		client:      client,
		startDate:   &now,
		timeFetched: false,
	}, nil
}

func (s *Spool) Close() error {
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("error closing reddit spool: %w", s.db.Close())
	}
	return nil
}

func (s *Spool) Init(startDate time.Time) error {
	err := s.db.CreateNewSpool(startDate)
	if err != nil {
		return fmt.Errorf("Error initializing spool: %w", err)
	}
	return nil
}

func (s *Spool) StartDate() (*time.Time, error) {
	if s.timeFetched {
		return s.startDate, nil
	}

	t, err := s.db.GetStartDate()
	if err != nil {
		return nil, fmt.Errorf("error fetching start date: %w", err)
	}
	s.startDate = t
	s.timeFetched = true

	return t, nil
}

func (s *Spool) addPostAndComments(pc *reddit.PostAndComments) error {
	a := postToArticle(pc.Post)
	err := s.db.InsertArticleRecord(&a)
	if err != nil {
		return fmt.Errorf("error adding reddit post to spool: %w", err)
	}

	for _, c := range pc.Comments {
		cA := commentToArticle(c, a.Subject)
		err := s.db.InsertArticleRecord(&cA)
		if err != nil {
			return fmt.Errorf("error adding reddit comment to spool: %w", err)
		}
	}
	return nil
}

func postToArticle(p *reddit.Post) store.ArticleRecord {
	return store.ArticleRecord{
		PostedAt:  p.Created.Time,
		Newsgroup: "reddit." + strings.ToLower(p.SubredditName),
		Subject:   p.Title,
		Author:    p.Author + " <" + p.Author + "@reddit" + ">",
		MsgID:     "<" + p.ID + ".reddit.nntp>",
		ParentID:  "",
		Body:      p.Body,
	}
}

func commentToArticle(c *reddit.Comment, title string) store.ArticleRecord {
	return store.ArticleRecord{
		PostedAt:  c.Created.Time,
		Newsgroup: "reddit." + strings.ToLower(c.SubredditName),
		Subject:   "Re: " + title,
		Author:    c.Author + " <" + c.Author + "@reddit" + ">",
		MsgID:     "<" + c.ID + ".reddit.nntp>",
		ParentID:  "<" + c.ParentID + ".reddit.nntp>",
		Body:      c.Body,
	}
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
			} else {
				break
			}
		}

		postComments = append(postComments, pc)
	}

	if len(postComments) == 0 {
		return errors.New("could not fetch any posts")
	}

	for _, pc := range postComments {
		if pc == nil {
			log.Println("Skipping empty postcomment")
			continue
		}
		err := s.addPostAndComments(pc)
		if err != nil {
			log.Printf("error adding postcomments to spool: %v\n", err)
		}
	}

	return nil
}

func (s *Spool) Newsgroups() ([]string, error) {
	var empty []string
	groups, err := s.db.FetchNewsgroups()
	if err != nil {
		return empty, fmt.Errorf("error getting newsgroups in spool: %w", err)
	}

	return groups, nil
}

func (s *Spool) GroupArticleCount(group string) (int, error) {
	count, err := s.db.GroupArticleCount(group)
	if err != nil {
		return 0, fmt.Errorf("error getting article count for group %s: %w", group, err)
	}
	return count, nil
}

func (s *Spool) GetHeaderByNGNum(group string, articleNum uint) (*Header, error) {
	rowIDs, err := s.db.GetRowIDs(group, articleNum)
	if err != nil {
		return nil, fmt.Errorf("error getting row ID for header request: %w", err)
	}

	if len(rowIDs) == 0 {
		return nil, fmt.Errorf("no headers found for group %s", group)
	}

	last := rowIDs[len(rowIDs)-1]
	dbHeader, err := s.db.GetHeaderByRowID(last)
	if dbHeader == nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error fetching headers for row ID %d: %w", last, err)
	}

	postedAt, err := store.FromDbTime(dbHeader.PostedAt)
	if err != nil {
		postedAt = time.UnixMilli(0)
	}
	header := &Header{
		PostedAt:  postedAt,
		Newsgroup: dbHeader.Newsgroup,
		Subject:   dbHeader.Subject,
		Author:    dbHeader.Author,
		MsgID:     dbHeader.MsgID,
		References: []string{dbHeader.ParentID},
	}
	return header, nil
}

func (s *Spool) GetArticleByNGNum(group string, articleNum uint) (*Article, error) {
	rowIDs, err := s.db.GetRowIDs(group, articleNum)
	if err != nil {
		return nil, fmt.Errorf("error getting row ID for header request: %w", err)
	}

	if len(rowIDs) == 0 {
		return nil, fmt.Errorf("no headers found for group %s", group)
	}

	last := rowIDs[len(rowIDs)-1]
	dbArticle, err := s.db.GetArticleByRowID(last)
	if dbArticle == nil {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("error fetching headers for row ID %d: %w", last, err)
	}

	postedAt, err := store.FromDbTime(dbArticle.Header.PostedAt)
	if err != nil {
		postedAt = time.UnixMilli(0)
	}
	article := &Article{
		Header: Header{
			PostedAt:  postedAt,
			Newsgroup: dbArticle.Header.Newsgroup,
			Subject:   dbArticle.Header.Subject,
			Author:    dbArticle.Header.Author,
			MsgID:     dbArticle.Header.MsgID,
			References:  []string{dbArticle.Header.ParentID},
		},
		Body: dbArticle.Body,
	}
	return article, nil
}
