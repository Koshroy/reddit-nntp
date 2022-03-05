package nntp

import (
	"fmt"
	"net/textproto"

	"github.com/Koshroy/reddit-nntp/spool"
)

func handleList(conn *textproto.Conn, spool *spool.Spool, args []string) error {
	const (
		ACTIVE_LIST = iota
		NEWSGROUP_LIST
		UNRECOGNIZED_LIST
	)
	mode := UNRECOGNIZED_LIST
	if len(args) == 0 {
		mode = ACTIVE_LIST
	} else {
		if args[0] == "ACTIVE" {
			mode = ACTIVE_LIST
		} else if args[0] == "NEWSGROUPS" {
			mode = NEWSGROUP_LIST
		}
	}

	if mode == UNRECOGNIZED_LIST {
		return conn.PrintfLine("503 This LIST argument is not supported")
	}

	groups, err := spool.Newsgroups()
	if err != nil {
		return conn.PrintfLine("403 error reading from spool")
	}

	if mode == ACTIVE_LIST {
		return handleListActive(conn, spool, groups)
	} else {
		return handleListNewsgroups(conn, spool, groups)
	}
}

func handleListActive(conn *textproto.Conn, spool *spool.Spool, groups []string) error {
	datum, err := getGroupData(spool, groups)
	if err != nil {
		return conn.PrintfLine("403 error reading from spool")
	}

	w := conn.DotWriter()
	_, err = w.Write([]byte("215 list of newsgroups follows\n"))
	if err != nil {
		return fmt.Errorf("error returning active list status line: %w", err)
	}

	for _, data := range datum {
		_, err = w.Write([]byte(data.String(false)))
		if err != nil {
			return fmt.Errorf("error writing group response line to socket: %w", err)
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			return fmt.Errorf("error writing group response line to socket: %w", err)
		}
	}

	return w.Close()
}

func handleListNewsgroups(conn *textproto.Conn, spool *spool.Spool, groups []string) error {
	w := conn.DotWriter()
	_, err := w.Write([]byte("215 information follows\n"))
	if err != nil {
		return fmt.Errorf("error returning newsgroup list status line: %w", err)
	}

	for _, group := range groups {
		_, err = w.Write([]byte(group + "\n"))
		if err != nil {
			return fmt.Errorf("error writing list newsgroup response line to socket: %w", err)
		}
	}

	return w.Close()
}
