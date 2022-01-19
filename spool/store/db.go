package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	db *sql.DB
}

type RowID uint

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

type Article struct {
	Header Header
	Body   []byte
}

const dbTimeFormat = "2006-01-02 15:04:05Z07:00"

func FromDbTime(s string) (time.Time, error) {
	return time.Parse(dbTimeFormat, strings.ReplaceAll(s, "+00:00", "Z"))
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

func (db *DB) GetRowIDs(group string, count uint) ([]RowID, error) {
	if count == 0 {
		return nil, errors.New("error: cannot request 0 rowids")
	}

	rowIDs := make([]RowID, count)
	raw := `
        SELECT rowid
        FROM spool WHERE newsgroup = ? ORDER BY posted_at LIMIT ?;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return rowIDs, fmt.Errorf("error preparing rowID query for group %s: %w", group, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(group, count)
	if err != nil {
		return rowIDs, fmt.Errorf("error querying for rowIDs for group %s: %w", group, err)
	}
	defer rows.Close()

	for rows.Next() {
		var rowID RowID
		err = rows.Scan(&rowID)
		if err != nil {
			return rowIDs, fmt.Errorf("could not unmarshal db row: %w", err)
		}

		rowIDs = append(rowIDs, rowID)
	}

	return rowIDs, nil
}

func (db *DB) GetHeaderByRowID(rowID RowID) (*Header, error) {
	raw := `
        SELECT posted_at, newsgroup, subject, author, message_id
        FROM spool WHERE rowid = ?;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return nil, fmt.Errorf("error preparing header rowid %d query: %w", rowID, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(rowID)
	if err != nil {
		return nil, fmt.Errorf("error querying for header by rowID %d: %w", rowID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var postedAt string
		var newsgroup string
		var subject string
		var author string
		var msgID string

		err = rows.Scan(&postedAt, &newsgroup, &subject, &author, &msgID)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal db row: %w", err)
		}

		return &Header{
			PostedAt:  postedAt,
			Newsgroup: newsgroup,
			Subject:   subject,
			Author:    author,
			MsgID:     msgID,
		}, nil
	}

	return nil, nil
}

func (db *DB) GetArticleByRowID(rowID RowID) (*Article, error) {
	raw := `
        SELECT posted_at, newsgroup, subject, author, message_id, body
        FROM spool WHERE rowid = ?;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return nil, fmt.Errorf("error preparing article rowid %d query: %w", rowID, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(rowID)
	if err != nil {
		return nil, fmt.Errorf("error querying for article by rowID %d: %w", rowID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var postedAt string
		var newsgroup string
		var subject string
		var author string
		var msgID string
		var body []byte

		err = rows.Scan(&postedAt, &newsgroup, &subject, &author, &msgID, &body)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal db row: %w", err)
		}

		return &Article{
			Header: Header{
				PostedAt:  postedAt,
				Newsgroup: newsgroup,
				Subject:   subject,
				Author:    author,
				MsgID:     msgID,
			},
			Body: body,
		}, nil
	}

	return nil, nil
}
