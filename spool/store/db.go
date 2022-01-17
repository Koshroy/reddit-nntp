package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	db *sql.DB
}

type ArticleRecord struct {
	PostedAt  time.Time
	Newsgroup string
	Subject   string
	Author    string
	MsgID     string
	Body      string
}

type Header struct {
	PostedAt  string
	Newsgroup string
	Subject   string
	Author    string
	MsgID     string
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

func (db *DB) InsertArticleRecord(ar *ArticleRecord) error {
	insertStmt := `
        INSERT INTO spool(posted_at, newsgroup, subject, author, message_id, body)
        VALUES (?, ?, ?, ?, ?, ?)
        `
	_, err := db.db.Exec(
		insertStmt,
		ar.PostedAt,
		ar.Newsgroup,
		ar.Subject,
		ar.Author,
		ar.MsgID,
		ar.Body,
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

func (db *DB) GroupArticleCount(group string) (int, error) {
	stmt, err := db.db.Prepare("SELECT COUNT(*) FROM spool WHERE newsgroup = ?")
	if err != nil {
		return 0, fmt.Errorf("error preparing article count query for group %s: %w", group, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(group)
	if err != nil {
		return 0, fmt.Errorf("error querying for article count for group %s: %w", group, err)
	}
	defer rows.Close()

	for rows.Next() {
		var count int
		err = rows.Scan(&count)
		if err != nil {
			return count, fmt.Errorf("could not unmarshal db row: %w", err)
		}
		return count, nil
	}

	return 0, nil
}

func (db *DB) GetHeaders(group string, count int) ([]Header, error) {
	headers := make([]Header, count)
	raw := `
        SELECT posted_at, subject, author, message_id
        FROM spool WHERE newsgroup = ?;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return headers, fmt.Errorf("error preparing header query for group %s: %w", group, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(group)
	if err != nil {
		return headers, fmt.Errorf("error querying for headers for group %s: %w", group, err)
	}
	defer rows.Close()

	for rows.Next() {
		var postedAt string
		var subject string
		var author string
		var messageID string

		err = rows.Scan(&postedAt, &subject, &author, &messageID)
		if err != nil {
			return headers, fmt.Errorf("could not unmarshal db row: %w", err)
		}

		header := Header{
			PostedAt:  postedAt,
			Newsgroup: group,
			Subject:   subject,
			Author:    author,
			MsgID:     messageID,
		}
		headers = append(headers, header)
	}

	return headers, nil
}
