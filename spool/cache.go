package spool

import (
	"errors"
	"fmt"
	"time"

	"github.com/Koshroy/reddit-nntp/spool/store"
)

const ROWID_TTL = time.Duration(5) * time.Second

type cacheEntry struct {
	rowIDs      []store.RowID
	lastFetched time.Time
}

func (s *Spool) GetRowIDsFromCache(group string) ([]store.RowID, error) {
	cacheHit := true
	v, ok := s.rowIDCache.Load(group)
	var entry cacheEntry
	if ok {
		entry, ok = v.(cacheEntry)
		cacheHit = ok && time.Since(entry.lastFetched) <= ROWID_TTL && entry.rowIDs != nil
	} else {
		cacheHit = false
	}

	if cacheHit {
		return entry.rowIDs, nil
	} else {
		fetchTime := time.Now()
		rowIDs, err := s.db.GetRowIDs(group)
		if err != nil {
			return rowIDs, err
		}

		newEntry := cacheEntry{
			rowIDs:      rowIDs,
			lastFetched: fetchTime,
		}

		v, ok := s.rowIDCache.Load(group)
		if !ok {
			s.rowIDCache.Store(group, newEntry)
			return rowIDs, nil
		}

		entry, ok = v.(cacheEntry)
		if !ok {
			s.rowIDCache.Store(group, newEntry)
			return rowIDs, nil
		}

		if fetchTime.After(entry.lastFetched) {
			// Possible race here
			s.rowIDCache.Store(group, newEntry)
		}

		return rowIDs, nil
	}
}

var ErrArticleNumNotFound = errors.New("article not found")

func (s *Spool) ArticleNumToRowIDCached(group string, articleNum uint) (store.RowID, error) {
	var zero store.RowID

	if articleNum < 1 {
		return zero, fmt.Errorf("cannot serve article #%d", articleNum)
	}

	allRowIDs, err := s.GetRowIDsFromCache(group)
	if err != nil {
		return zero, fmt.Errorf("error getting row IDs: %w", err)
	}

	if allRowIDs == nil || len(allRowIDs) == 0 {
		return zero, fmt.Errorf("no headers found for group %s", group)
	}

	if uint(len(allRowIDs)) < articleNum {
		return zero, ErrArticleNumNotFound
	}

	return allRowIDs[articleNum-1], nil
}
