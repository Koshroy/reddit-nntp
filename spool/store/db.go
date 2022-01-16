package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

type DB struct {
	db *sql.DB
}

type articleRecord struct {
	postedAt  time.Time
	newsgroup string
	subject   string
	author    string
	msgID     string
	body      string
}

func Open(dbPath string) (*DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("could not open sqlite db: %w", err)
	}

	return &DB{
		db: db,
	}, nil
}

func (db *DB) CreateNewSpool(startDate time.Time) error {
	sqlStmtConfig := `
        CREATE TABLE config(
               k TEXT NOT NULL,
               v TEXT NOT NULL
        );
        `
	_, err := db.db.Exec(sqlStmtConfig)
	if err != nil {
		return fmt.Errorf("error creating config table in spool: %w", err)
	}

	sqlStmtDt := `INSERT INTO config(k, v) VALUES(?, ?)`
	_, err = db.db.Exec(sqlStmtDt, "startdate", startDate.Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("error adding start date to config table in spool: %w", err)
	}

	sqlStmtSpool := `
        CREATE TABLE spool(
               posted_at INTEGER NOT NULL,
               subreddit TEXT NOT NULL,
               newsgroup TEXT NOT NULL,
               subject TEXT,
               author TEXT NOT NULL,
               message_id TEXT UNIQUE NOT NULL,
               body BLOB NOT NULL
        );
        `
	_, err = db.db.Exec(sqlStmtSpool)
	if err != nil {
		return fmt.Errorf("error creating spool table: %w", err)
	}

	return nil
}

func (db *DB) Close() error {
	err := db.Close()
	if err != nil {
		return fmt.Errorf("error closing database: %w", err)
	}
	return nil
}

func (db *DB) AddPostAndComments(pc *reddit.PostAndComments) error {
	a := postToArticle(pc.Post)
	err := db.insertArticleRecord(&a)
	if err != nil {
		return fmt.Errorf("error adding reddit post to spool: %w", err)
	}
	return nil
}

func postToArticle(p *reddit.Post) articleRecord {
	return articleRecord{
		postedAt:  p.Created.Time,
		newsgroup: "reddit." + strings.ToLower(p.SubredditName),
		subject:   p.Title,
		author:    p.Author + " <" + p.Author + "@reddit" + ">",
		msgID:     "<" + p.ID + ".reddit.nntp>",
		body:      p.Body,
	}
}

func (db *DB) insertArticleRecord(ar *articleRecord) error {
	insertStmt := `
        INSERT INTO spool(posted_at, newsgroup, subject, author, message_id, body)
        VALUES (?, ?, ?, ?, ?, ?)
        `

	_, err := db.db.Exec(
		insertStmt,
		ar.postedAt,
		ar.newsgroup,
		ar.subject,
		ar.author,
		ar.msgID,
		ar.body,
	)

	if err != nil {
		return fmt.Errorf("error inserting article into db: %w", err)
	}

	return nil
}

func (db *DB) GetStartDate() (*time.Time, error) {
	stmt, err := db.db.Prepare("SELECT v FROM config WHERE k = ?")
	if err != nil {
		return nil, fmt.Errorf("error preparing start date query: %w", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query("startdate")
	if err != nil {
		return nil, fmt.Errorf("error querying for start date: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var rawStartDate string
		err = rows.Scan(&rawStartDate)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal db row: %w", err)
		}

		t, err := time.Parse(time.RFC3339, rawStartDate)
		if err != nil {
			return nil, fmt.Errorf("could not parse date %s from db: %w", rawStartDate, err)
		}

		return &t, nil
	}

	return nil, errors.New("could not find start time in spool db")
}

func (db *DB) FetchNewsgroups() ([]string, error) {
	stmt, err := db.db.Prepare("SELECT DISTINCT newsgroup FROM spool")
	if err != nil {
		return nil, fmt.Errorf("error preparing newsgroup list query: %w", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		return nil, fmt.Errorf("error querying for start date: %w", err)
	}
	defer rows.Close()

	var groups []string
	for rows.Next() {
		var group string
		err = rows.Scan(&group)
		if err != nil {
			return groups, fmt.Errorf("could not unmarshal db row: %w", err)
		}
		groups = append(groups, group)
	}

	return groups, nil
}
