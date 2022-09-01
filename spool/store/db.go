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
	ParentID  string
	Body      string
}

type Header struct {
	PostedAt  string
	Newsgroup string
	Subject   string
	Author    string
	MsgID     string
	ParentID  string
}

type Article struct {
	Header Header
	Body   []byte
}

type GroupMetadata struct {
	Name         string
	DateCreated  time.Time
	DaysRetained uint
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

func (db *DB) CreateNewSpool(startDate time.Time, prefix string) error {
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
	_, err = db.db.Exec(sqlStmtDt, "prefix", prefix)
	if err != nil {
		return fmt.Errorf("error adding prefix to config table in spool: %w", err)
	}

	sqlStmtSpool := `
        CREATE TABLE spool(
			   article_num INTEGER PRIMARY KEY AUTOINCREMENT,
               posted_at INTEGER NOT NULL,
               newsgroup TEXT NOT NULL,
               subject TEXT,
               author TEXT NOT NULL,
               message_id TEXT UNIQUE NOT NULL,
               parent_id TEXT,
               body BLOB NOT NULL
        );
        `
	_, err = db.db.Exec(sqlStmtSpool)
	if err != nil {
		return fmt.Errorf("error creating spool table: %w", err)
	}

	sqlStmtGroups := `
        CREATE TABLE groups(
               name TEXT UNIQUE NOT NULL,
               date_created TEXT NOT NULL,
               days_retained INTEGER NOT NULL
        );
        `
	_, err = db.db.Exec(sqlStmtGroups)
	if err != nil {
		return fmt.Errorf("error creating groups table: %w", err)
	}

	return nil
}

func (db *DB) Close() error {
	err := db.db.Close()
	if err != nil {
		return fmt.Errorf("error closing database: %w", err)
	}
	return nil
}

func (db *DB) InsertArticleRecord(ar *ArticleRecord) error {
	if ar == nil {
		return errors.New("cannot insert nil record into db")
	}

	exists, err := db.DoesMessageIDExist(ar.MsgID)
	if err == nil && exists {
		return nil
	}

	insertStmt := `
        INSERT INTO spool(posted_at, newsgroup, subject, author, message_id, parent_id, body)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        `
	_, err = db.db.Exec(
		insertStmt,
		ar.PostedAt,
		ar.Newsgroup,
		ar.Subject,
		ar.Author,
		ar.MsgID,
		ar.ParentID,
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

	if !rows.Next() {
		return nil, errors.New("could not find start time in spool db")
	}

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

func (db *DB) GetPrefix() (string, error) {
	stmt, err := db.db.Prepare("SELECT v FROM config WHERE k = ?")
	if err != nil {
		return "", fmt.Errorf("error preparing prefix query: %w", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query("prefix")
	if err != nil {
		return "", fmt.Errorf("error querying for prefix: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return "", errors.New("could not find start time in spool db")
	}
	var prefix string
	err = rows.Scan(&prefix)
	if err != nil {
		return "", fmt.Errorf("could not unmarshal db row: %w", err)
	}

	return prefix, nil
}

func (db *DB) FetchNewsgroups() ([]string, error) {
	stmt, err := db.db.Prepare("SELECT name FROM groups")
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

func (db *DB) ArticleCount() (uint, error) {
	stmt, err := db.db.Prepare("SELECT COUNT(*) FROM spool")
	if err != nil {
		return 0, fmt.Errorf("error preparing article count query: %w", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		return 0, fmt.Errorf("error querying for article count: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, nil
	}

	var count uint
	err = rows.Scan(&count)
	if err != nil {
		return count, fmt.Errorf("could not unmarshal db row: %w", err)
	}
	return count, nil

}

func (db *DB) DoesMessageIDExist(msgID string) (bool, error) {
	stmt, err := db.db.Prepare("SELECT COUNT(*) FROM spool WHERE message_id = ?")
	if err != nil {
		return false, fmt.Errorf("error preparing msg id existence query: %w", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(msgID)
	if err != nil {
		return false, fmt.Errorf("error querying for msg id existence: %w", err)
	}
	defer rows.Close()

	var count uint
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return false, fmt.Errorf("could not unmarshal db row: %w", err)
		}
	}

	return count > 0, nil
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

	if !rows.Next() {
		return 0, nil
	}

	var count int
	err = rows.Scan(&count)
	if err != nil {
		return count, fmt.Errorf("could not unmarshal db row: %w", err)
	}

	return count, nil
}

func (db *DB) GetRowIDs(group string) ([]RowID, error) {
	rowIDs := make([]RowID, 0)
	raw := `
        SELECT rowid
        FROM spool WHERE newsgroup = ? ORDER BY posted_at;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return rowIDs, fmt.Errorf("error preparing rowID query for group %s: %w", group, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(group)
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
        SELECT posted_at, newsgroup, subject, author, message_id, parent_id
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

	if !rows.Next() {
		return nil, nil
	}

	var postedAt string
	var newsgroup string
	var subject string
	var author string
	var msgID string
	var parentID string

	err = rows.Scan(&postedAt, &newsgroup, &subject, &author, &msgID, &parentID)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal db row: %w", err)
	}

	return &Header{
		PostedAt:  postedAt,
		Newsgroup: newsgroup,
		Subject:   subject,
		Author:    author,
		MsgID:     msgID,
		ParentID:  parentID,
	}, nil
}

func (db *DB) GetHeaderByMsgID(msgID string) (*Header, error) {
	raw := `
        SELECT posted_at, newsgroup, subject, author, message_id, parent_id
        FROM spool WHERE message_id = ?;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return nil, fmt.Errorf("error preparing header by msgID %s query: %w", msgID, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(msgID)
	if err != nil {
		return nil, fmt.Errorf("error querying for header by msgID %s: %w", msgID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var postedAt string
	var newsgroup string
	var subject string
	var author string
	var rowMsgID string
	var parentID string

	err = rows.Scan(&postedAt, &newsgroup, &subject, &author, &rowMsgID, &parentID)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal db row: %w", err)
	}

	return &Header{
		PostedAt:  postedAt,
		Newsgroup: newsgroup,
		Subject:   subject,
		Author:    author,
		MsgID:     rowMsgID,
		ParentID:  parentID,
	}, nil
}

func (db *DB) GetArticleByRowID(rowID RowID) (*Article, error) {
	raw := `
        SELECT posted_at, newsgroup, subject, author, message_id, parent_id, body
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

	if !rows.Next() {
		return nil, nil
	}

	var postedAt string
	var newsgroup string
	var subject string
	var author string
	var msgID string
	var parentID string
	var body []byte

	err = rows.Scan(&postedAt, &newsgroup, &subject, &author, &msgID, &parentID, &body)
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
			ParentID:  parentID,
		},
		Body: body,
	}, nil
}

func (db *DB) GetArticleByMsgID(msgID string) (*Article, error) {
	raw := `
        SELECT posted_at, newsgroup, subject, author, message_id, parent_id, body
        FROM spool WHERE message_id = ?;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return nil, fmt.Errorf("error preparing article by msgID %s query: %w", msgID, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(msgID)
	if err != nil {
		return nil, fmt.Errorf("error querying for article by msgID %s: %w", msgID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var postedAt string
	var newsgroup string
	var subject string
	var author string
	var rowMsgID string
	var parentID string
	var body []byte

	err = rows.Scan(&postedAt, &newsgroup, &subject, &author, &rowMsgID, &parentID, &body)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal db row: %w", err)
	}

	return &Article{
		Header: Header{
			PostedAt:  postedAt,
			Newsgroup: newsgroup,
			Subject:   subject,
			Author:    author,
			MsgID:     rowMsgID,
			ParentID:  parentID,
		},
		Body: body,
	}, nil
}

func (db *DB) FetchNewGroups(dt time.Time) ([]string, error) {
	stmt, err := db.db.Prepare("SELECT name FROM groups WHERE date_created > ?")
	if err != nil {
		return nil, fmt.Errorf("error preparing new groups query: %w", err)
	}
	defer stmt.Close()
	fmtTime := dt.Format(time.RFC3339)
	rows, err := stmt.Query(fmtTime)
	if err != nil {
		return nil, fmt.Errorf("error querying for new groups: %w", err)
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

func (db *DB) DoesGroupMetadataExist(gm *GroupMetadata) (bool, error) {
	stmt, err := db.db.Prepare("SELECT COUNT(*) FROM groups WHERE name = ?")
	if err != nil {
		return false, fmt.Errorf("error preparing group metadata existance query: %w", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(gm.Name)
	if err != nil {
		return false, fmt.Errorf("error querying for group metadata existence: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return false, nil
	}

	var count uint
	err = rows.Scan(&count)
	if err != nil {
		return false, fmt.Errorf("could not unmarshal db row: %w", err)
	}

	return count > 0, nil
}

func (db *DB) InsertGroupMetadata(gm *GroupMetadata) error {
	if gm == nil {
		return errors.New("cannot insert nil group metadata")
	}

	exists, err := db.DoesGroupMetadataExist(gm)
	if err == nil && exists {
		return nil
	}

	dateCreatedUTC := gm.DateCreated.In(time.UTC).Format(time.RFC3339)
	insertStmt := `
        INSERT INTO groups(name, date_created, days_retained)
        VALUES (?, ?, ?)
        `
	_, err = db.db.Exec(
		insertStmt,
		gm.Name,
		dateCreatedUTC,
		gm.DaysRetained,
	)

	if err != nil {
		return fmt.Errorf("error inserting article into db: %w", err)
	}

	return nil
}

func (db *DB) GetAllRowIDs(group string) ([]RowID, error) {
	rowIDs := make([]RowID, 0)
	raw := `
        SELECT rowid
        FROM spool WHERE newsgroup = ? ORDER BY posted_at;
        `
	stmt, err := db.db.Prepare(raw)
	if err != nil {
		return rowIDs, fmt.Errorf("error preparing rowID query for group %s: %w", group, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(group)
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
