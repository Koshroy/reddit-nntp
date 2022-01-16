package nntp

import (
	"fmt"
	"io"
	"log"
	"net/textproto"
	"strings"

	"github.com/Koshroy/reddit-nntp/spool"
)

type Server struct {
	conn  *textproto.Conn
	spool *spool.Spool
}

type nntpCmd struct {
	cmd  string
	args []string
}

type groupStatus uint
type groupData struct {
	name   string
	high   int
	low    int
	status groupStatus
}

const (
	POSTING_PERMITTED = iota
	POSTING_NONPERMITTED
	POSTING_MODERATED
)

func (g groupData) String(groupMode bool) string {
	status := "n"
	switch g.status {
	case POSTING_PERMITTED:
		status = "y"
	case POSTING_NONPERMITTED:
		status = "n"
	case POSTING_MODERATED:
		status = "m"
	}

	if groupMode {
		return fmt.Sprintf("%s %d %d", g.name, g.low, g.high)
	}
	return fmt.Sprintf("%s %d %d %s", g.name, g.high, g.low, status)
}

func NewServer(conn *textproto.Conn, spool *spool.Spool) Server {
	return Server{
		conn:  conn,
		spool: spool,
	}
}

func (s Server) Close() {
	log.Println("Closing connection")
	err := s.conn.Close()
	if err != nil {
		log.Println("error closing connection: %v\n", err)
	}
}

func (s Server) Process() {
	defer s.Close()

	err := s.conn.PrintfLine("201 Hello!")
	if err != nil {
		log.Printf("error writing to connection: %v\n", err)
		return
	}

	requests := make(chan string)
	defer func() {
		close(requests)
	}()

	go s.processLoop(requests)
	for {
		line, err := s.conn.ReadLine()
		if err != nil {
			if err != io.EOF {
				log.Printf("error reading line from connection: %v\n", err)
			}
			return
		}

		requests <- line
	}
}

func (s Server) processLoop(requests <-chan string) {
	for {
		line, ok := <-requests
		if !ok {
			return
		}

		log.Println("Received line:", line)
		cmd, err := parseLine(line)
		if err != nil {
			log.Printf("error parsing line from client: %v\n", err)
			return
		}

		switch cmd.cmd {
		case "CAPABILITIES":
			if err := printCapabilities(s.conn); err != nil {
				log.Printf("error sending capabilities to client: %v\n", err)
				return
			}
		case "QUIT":
			if err := printQuit(s.conn); err != nil {
				log.Printf("error sending quit to client: %v\n", err)
			}
			return
		case "LIST":
			if err := printList(s.conn, s.spool, cmd.args); err != nil {
				log.Printf("error sending list to client: %v\n", err)
			}
		case "GROUP":
			if err := printGroup(s.conn, s.spool, cmd.args); err != nil {
				log.Printf("error sending group to client: %v\n", err)
			}
		default:
			log.Printf("Unknown command found: %s\n", cmd.cmd)
			if err := printUnknown(s.conn); err != nil {
				log.Printf("error printing unknown command: %v\n", err)
				return
			}
		}

	}
}

func parseLine(line string) (*nntpCmd, error) {
	splits := strings.SplitN(strings.ToUpper(line), " ", 2)
	if len(splits) == 0 {
		return nil, fmt.Errorf("unable to split line received on connection")
	}

	return &nntpCmd{
		cmd:  splits[0],
		args: splits[1:],
	}, nil
}

func printCapabilities(conn *textproto.Conn) error {
	if err := conn.PrintfLine("101 Capability list:"); err != nil {
		return fmt.Errorf("could not print line: %w", err)
	}
	if err := conn.PrintfLine("READER"); err != nil {
		return fmt.Errorf("could not print line: %w", err)
	}

	return conn.PrintfLine("VERSION")
}

func printQuit(conn *textproto.Conn) error {
	return conn.PrintfLine("205 Connection closing")
}

func printUnknown(conn *textproto.Conn) error {
	return conn.PrintfLine("500 Unknown command")
}

func getGroupData(spool *spool.Spool, groups []string) ([]groupData, error) {
	var datum []groupData
	for _, group := range groups {
		count, err := spool.GroupArticleCount(group)
		if err != nil {
			return nil, err
		}

		var grpData groupData
		if count == 0 {
			grpData = groupData{
				name:   group,
				high:   1,
				low:    0,
				status: POSTING_NONPERMITTED,
			}
		} else {
			grpData = groupData{
				name:   group,
				high:   count,
				low:    1,
				status: POSTING_NONPERMITTED,
			}
		}
		datum = append(datum, grpData)
	}

	return datum, nil
}

func printList(conn *textproto.Conn, spool *spool.Spool, args []string) error {
	active := false
	if len(args) == 0 {
		active = true
	} else {
		if args[0] == "ACTIVE" {
			active = true
		}
	}

	// TODO: We need to handle LIST properly
	if !active {
		return printUnknown(conn)
	}

	groups, err := spool.Newsgroups()
	datum, err := getGroupData(spool, groups)
	if err != nil {
		err2 := conn.PrintfLine("403 error reading from spool")
		if err2 != nil {
			log.Println("could not write error response to connection:", err2)
		}
		return err
	}

	err = conn.PrintfLine("215 list of newsgroups follows")
	if err != nil {
		return fmt.Errorf("error returning active list status line: %w", err)
	}

	for _, data := range datum {
		err := conn.PrintfLine("%s\n.", data.String(false))
		if err != nil {
			return fmt.Errorf("error writing group response line to socket: %w", err)
		}
	}

	return nil
}

func printGroup(conn *textproto.Conn, spool *spool.Spool, args []string) error {
	groups, err := spool.Newsgroups()
	if err != nil {
		err2 := conn.PrintfLine("403 error reading from spool")
		if err2 != nil {
			log.Println("could not write error response to connection:", err2)
		}
		return err
	}

	datum, err := getGroupData(spool, groups)
	if err != nil {
		err2 := conn.PrintfLine("403 error reading from spool")
		if err2 != nil {
			log.Println("could not write error response to connection:", err2)
		}
		return err
	}

	for _, data := range datum {
		err := conn.PrintfLine("%s\n.", data.String(true))
		if err != nil {
			return fmt.Errorf("error writing group response line to socket: %w", err)
		}
	}

	return nil
}
