package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

type DB struct {
	db  *sql.DB
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
	_, err = db.db.Exec(sqlStmtDt, "startdate", startDate)
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

func (db *DB) AddPostAndComments(pc *reddit.PostAndComments) error {
	return nil
}
