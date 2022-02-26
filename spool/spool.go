package spool

import (
	"bytes"
	"fmt"
	"html"
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
	prefix      string
	concLimit   uint
}

type Credentials = reddit.Credentials

const nntpTimeFormat = "02 Jan 2006 15:04 -0700"

type Header struct {
	PostedAt   time.Time
	Newsgroup  string
	Subject    string
	Author     string
	MsgID      string
	References []string
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
	buf.WriteString(unQuoteHTMLString(h.Subject))
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
				buf.WriteRune(',')
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
	buf.Write(unQuoteHTML(a.Body))

	return buf
}

func unQuoteHTML(body []byte) []byte {
	bodyStr := strings.ReplaceAll(string(body), "&#x200B;", "")
	return []byte(html.UnescapeString(bodyStr))
}

func unQuoteHTMLString(payload string) string {
	bodyStr := strings.ReplaceAll(payload, "&#x200B;", "")
	return html.UnescapeString(bodyStr)
}

func New(fname string, concLimit uint, creds *reddit.Credentials) (*Spool, error) {
	db, err := store.Open(fname)
	if err != nil {
		return nil, fmt.Errorf("could not open DB: %w", err)
	}

	ua := reddit.WithUserAgent("server:reddit-nntp:0.0.1")
	var client *reddit.Client
	if creds == nil {
		client, err = reddit.NewReadonlyClient(ua)
		if err != nil {
			return nil, fmt.Errorf("could not open Reddit client: %w", err)
		}
	} else {
		client, err = reddit.NewClient(*creds, ua)
		if err != nil {
			return nil, fmt.Errorf("could not open Reddit client with creds: %w", err)
		}
	}

	now := time.Now()
	return &Spool{
		db:          db,
		client:      client,
		startDate:   &now,
		timeFetched: false,
		concLimit:   concLimit,
		prefix:      "",
	}, nil
}

func (s *Spool) Close() error {
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("error closing reddit spool: %w", err)
	}
	return nil
}

func (s *Spool) Init(startDate time.Time, prefix string) error {
	err := s.db.CreateNewSpool(startDate, prefix)
	if err != nil {
		return fmt.Errorf("Error initializing spool: %w", err)
	}
	return nil
}

func (s *Spool) Prefix() (string, error) {
	if s.prefix != "" {
		return s.prefix, nil
	}

	p, err := s.db.GetPrefix()
	if err != nil {
		return "", fmt.Errorf("error fetching prefix: %w", err)
	}
	s.prefix = p
	return s.prefix, nil
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

func (s *Spool) ArticleCount() (uint, error) {
	count, err := s.db.ArticleCount()
	if err != nil {
		return 0, fmt.Errorf("error fetching article count: %w", err)
	}
	return count, nil
}

func postToArticle(p *reddit.Post, prefix string) store.ArticleRecord {
	var body string
	if p.Body == "" {
		body = p.URL
	} else {
		body = p.Body
	}

	return store.ArticleRecord{
		PostedAt:  p.Created.Time,
		Newsgroup: prefix + "." + strings.ToLower(p.SubredditName),
		Subject:   p.Title,
		Author:    fmt.Sprintf("%s <%s@%s>", p.Author, p.Author, prefix),
		MsgID:     fmt.Sprintf("<%s.%s.%s.nntp>", p.FullID, p.SubredditID, prefix),
		ParentID:  "",
		Body:      body,
	}
}

func commentToArticle(c *reddit.Comment, title, prefix string) store.ArticleRecord {
	return store.ArticleRecord{
		PostedAt:  c.Created.Time,
		Newsgroup: prefix + "." + strings.ToLower(c.SubredditName),
		Subject:   "Re: " + title,
		Author:    fmt.Sprintf("%s <%s@%s>", c.Author, c.Author, prefix),
		MsgID:     fmt.Sprintf("<%s.%s.%s.nntp>", c.FullID, c.SubredditID, prefix),
		ParentID:  fmt.Sprintf("<%s.%s.%s.nntp>", c.ParentID, c.SubredditID, prefix),
		Body:      c.Body,
	}
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
		PostedAt:   postedAt,
		Newsgroup:  dbHeader.Newsgroup,
		Subject:    dbHeader.Subject,
		Author:     dbHeader.Author,
		MsgID:      dbHeader.MsgID,
		References: []string{dbHeader.ParentID},
	}
	return header, nil
}

func (s *Spool) GetHeaderByMsgID(group string, msgID string) (*Header, error) {
	dbHeader, err := s.db.GetHeaderByMsgID(msgID)
	if err != nil {
		return nil, fmt.Errorf("error fetching headers for msg ID %s: %w", msgID, err)
	}

	postedAt, err := store.FromDbTime(dbHeader.PostedAt)
	if err != nil {
		postedAt = time.UnixMilli(0)
	}
	header := &Header{
		PostedAt:   postedAt,
		Newsgroup:  dbHeader.Newsgroup,
		Subject:    dbHeader.Subject,
		Author:     dbHeader.Author,
		MsgID:      dbHeader.MsgID,
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
			PostedAt:   postedAt,
			Newsgroup:  dbArticle.Header.Newsgroup,
			Subject:    dbArticle.Header.Subject,
			Author:     dbArticle.Header.Author,
			MsgID:      dbArticle.Header.MsgID,
			References: []string{dbArticle.Header.ParentID},
		},
		Body: dbArticle.Body,
	}
	return article, nil
}

func (s *Spool) GetArticleByMsgID(group string, msgID string) (*Article, error) {
	dbArticle, err := s.db.GetArticleByMsgID(msgID)
	if err != nil {
		return nil, fmt.Errorf("error fetching headers for msg ID %s: %w", msgID, err)
	}

	postedAt, err := store.FromDbTime(dbArticle.Header.PostedAt)
	if err != nil {
		postedAt = time.UnixMilli(0)
	}
	article := &Article{
		Header: Header{
			PostedAt:   postedAt,
			Newsgroup:  dbArticle.Header.Newsgroup,
			Subject:    dbArticle.Header.Subject,
			Author:     dbArticle.Header.Author,
			MsgID:      dbArticle.Header.MsgID,
			References: []string{dbArticle.Header.ParentID},
		},
		Body: dbArticle.Body,
	}
	return article, nil
}

func (s *Spool) NewGroups(dt time.Time) ([]string, error) {
	var empty []string
	groups, err := s.db.FetchNewGroups(dt)
	if err != nil {
		return empty, fmt.Errorf("error getting new groups from spool: %w", err)
	}

	return groups, nil
}

func (s *Spool) AddGroupMetadata(name string, dateCreated time.Time, daysRetained uint) error {
	err := s.db.InsertGroupMetadata(&store.GroupMetadata{
		Name:         fmt.Sprintf("%s.%s", s.prefix, strings.ToLower(name)),
		DateCreated:  dateCreated,
		DaysRetained: daysRetained,
	})
	if err != nil {
		return fmt.Errorf("error adding group %s metadata: %w", name, err)
	}

	return nil
}

func (s *Spool) GetArticleNumsFromGroup(group string) ([]uint, error) {
	rowIDs, err := s.db.GetAllRowIDs(group)
	if err != nil {
		return nil, fmt.Errorf("error getting row IDs: %w", err)
	}

	if len(rowIDs) == 0 {
		return nil, fmt.Errorf("no headers found for group %s", group)
	}

	nums := make([]uint, len(rowIDs))
	for _, rid := range rowIDs {
		nums = append(nums, uint(rid))
	}

	return nums, nil
}
