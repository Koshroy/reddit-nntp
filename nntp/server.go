package nntp

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/textproto"
	"strconv"
	"strings"
	"sync"

	"github.com/Koshroy/reddit-nntp/spool"
)

const (
	GROUP_KEY = iota
)

type Server struct {
	conn   *textproto.Conn
	spool  *spool.Spool
	locals *sync.Map
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

const POST_LINE = "201 Posting prohibited"

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
		est := g.high - g.low
		return fmt.Sprintf("%d %d %d %s", est, g.low, g.high, g.name)
	}
	return fmt.Sprintf("%s %d %d %s", g.name, g.high, g.low, status)
}

func NewServer(conn *textproto.Conn, spool *spool.Spool) Server {
	var locals sync.Map

	return Server{
		conn:   conn,
		spool:  spool,
		locals: &locals,
	}
}

func (s Server) Close() {
	log.Println("Closing connection")
	err := s.conn.Close()
	if err != nil {
		log.Println("error closing connection: %v\n", err)
	}
}

func curGroup(locals *sync.Map) string {
	v, ok := locals.Load(GROUP_KEY)
	if !ok {
		return ""
	}
	grp, ok := v.(string)
	if !ok {
		return ""
	}
	return grp
}

func setCurGroup(locals *sync.Map, group string) {
	locals.Store(GROUP_KEY, group)
}

func (s Server) Process(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer s.Close()
	defer cancel()

	if ctx.Err() != nil {
		return
	}

	err := s.conn.PrintfLine(POST_LINE)
	if err != nil {
		log.Printf("error writing to connection: %v\n", err)
		return
	}

	requests := make(chan string)
	defer func() {
		close(requests)
	}()

	lineChan := make(chan string)
	doneReader := make(chan struct{})
	doneProcess := make(chan struct{})
	go readerLoop(ctx, s.conn, lineChan, doneReader)
	go processLoop(ctx, s.conn, s.spool, s.locals, requests, doneProcess)
	for {
		select {
		case line := <-lineChan:
			requests <- line
		case <-ctx.Done():
			return
		case <-doneReader:
			return
		case <-doneProcess:
			return
		}
	}
}

func readerLoop(ctx context.Context, conn *textproto.Conn, lineChan chan<- string, done chan<- struct{}) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		close(done)
	}()

	for {
		line, err := conn.ReadLine()
		if err != nil {
			if err != io.EOF && ctx.Err() != nil {
				log.Printf("error reading line from connection: %v\n", err)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		lineChan <- line
	}
}

func processLoop(ctx context.Context, conn *textproto.Conn, spool *spool.Spool, locals *sync.Map, requests <-chan string, done chan<- struct{}) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		close(done)
	}()

	for {
		select {
		case line := <-requests:
			if len(line) == 0 {
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
				if err := printCapabilities(conn); err != nil {
					log.Printf("error sending capabilities to client: %v\n", err)
					return
				}
			case "QUIT":
				if err := printQuit(conn); err != nil && ctx.Err() == nil {
					log.Printf("error sending quit to client: %v\n", err)
				}
				return
			case "LIST":
				if err := printList(conn, spool, cmd.args); err != nil {
					log.Printf("error sending list to client: %v\n", err)
				}
			case "GROUP":
				if len(cmd.args) < 1 {
					err := conn.PrintfLine("500 No group name provided")
					if err != nil {
						log.Printf("error sending group to client: %v\n", err)
					}
					continue
				}

				group := cmd.args[0]
				setCurGroup(locals, group)
				if err := printGroup(conn, spool, group); err != nil {
					log.Printf("error sending group to client: %v\n", err)
				}
			case "HEAD":
				group := curGroup(locals)
				if len(group) == 0 {
					err := conn.PrintfLine("500 No active group set. Server error.")
					log.Println("No active group found for HEAD command")
					if err != nil {
						log.Println("error sending HEAD to client:", err)
					}
				}
				if err := printHead(conn, spool, group, cmd.args); err != nil {
					log.Printf("error sending group to client: %v\n", err)
				}
			case "ARTICLE":
				group := curGroup(locals)
				if len(group) == 0 {
					err := conn.PrintfLine("500 No active group set. Server error.")
					log.Println("No active group found for ARTICLE command")
					if err != nil {
						log.Println("error sending HEAD to client:", err)
					}
				}
				if err := printArticle(conn, spool, group, cmd.args); err != nil {
					log.Printf("error sending group to client: %v\n", err)
				}
			case "MODE":
				if err := printMode(conn, cmd.args); err != nil {
					log.Printf("error sending group to client: %v\n", err)
				}
			default:
				log.Printf("Unknown command found: %s\n", cmd.cmd)
				if err := printUnknown(conn); err != nil {
					log.Printf("error printing unknown command: %v\n", err)
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func parseLine(line string) (*nntpCmd, error) {
	splits := strings.SplitN(line, " ", 2)
	if len(splits) == 0 {
		return nil, fmt.Errorf("unable to split line received on connection")
	}

	return &nntpCmd{
		cmd:  strings.ToUpper(splits[0]),
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

func printMode(conn *textproto.Conn, args []string) error {
	if len(args) < 1 {
		// TODO: should we do this here?
		return printUnknown(conn)
	}
	if args[0] == "READER" {
		return conn.PrintfLine(POST_LINE)
	} else {
		return conn.PrintfLine("500 Only READER is supported")
	}

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
		return conn.PrintfLine("403 error reading from spool")
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

func printGroup(conn *textproto.Conn, spool *spool.Spool, group string) error {
	count, err := spool.GroupArticleCount(group)
	if err != nil {
		log.Println("error getting group", group, "article count:", err)
		return conn.PrintfLine("403 error reading from spool")
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

	return conn.PrintfLine("211 %s", grpData.String(true))
}

func printHead(conn *textproto.Conn, spool *spool.Spool, group string, args []string) error {
	if len(args) < 1 {
		// TODO: no arg is unsupported
		return conn.PrintfLine("500 current article mode unsupported")
	}

	arg := args[0]
	if len(arg) == 0 {
		log.Println("error: received empty argument that should have been parsed out")
		return conn.PrintfLine("500 could not parse line properly")
	}

	if arg[0] == '<' && arg[len(arg)-1] == '>' {
		// Message-ID mode
		return conn.PrintfLine("500 message-id mode unsupported")
	} else {
		articleNum, err := strconv.Atoi(arg)
		if err != nil {
			return conn.PrintfLine("500 could not parse argument properly")
		}

		header, err := spool.GetHeaderByNGNum(group, uint(articleNum))
		if err != nil || header == nil {
			return conn.PrintfLine("423 No article with that number")
		}

		w := conn.DotWriter()
		buf := header.Bytes()
		_, err = w.Write([]byte(fmt.Sprintf("221 %d %s\n", articleNum, header.MsgID)))
		if err != nil {
			return fmt.Errorf("error writing header response header: %w", err)
		}
		_, err = buf.WriteTo(w)
		if err != nil {
			return fmt.Errorf("error writing header response: %w", err)
		}

		return w.Close()
	}

	return nil
}

func printArticle(conn *textproto.Conn, spool *spool.Spool, group string, args []string) error {
	if len(args) < 1 {
		// TODO: no arg is unsupported
		return conn.PrintfLine("500 current article mode unsupported")
	}

	arg := args[0]
	if len(arg) == 0 {
		log.Println("error: received empty argument that should have been parsed out")
		return conn.PrintfLine("500 could not parse line properly")
	}

	if arg[0] == '<' && arg[len(arg)-1] == '>' {
		// Message-ID mode
		return conn.PrintfLine("500 message-id mode unsupported")
	} else {
		articleNum, err := strconv.Atoi(arg)
		if err != nil {
			return conn.PrintfLine("500 could not parse argument properly")
		}

		article, err := spool.GetArticleByNGNum(group, uint(articleNum))
		if err != nil || article == nil {
			return conn.PrintfLine("423 No article with that number")
		}

		w := conn.DotWriter()
		buf := article.Bytes()
		_, err = w.Write([]byte(fmt.Sprintf("220 %d %s\n", articleNum, article.Header.MsgID)))
		if err != nil {
			return fmt.Errorf("error writing header response header: %w", err)
		}
		_, err = buf.WriteTo(w)
		if err != nil {
			return fmt.Errorf("error writing article response: %w", err)
		}

		return w.Close()
	}

	return nil
}
