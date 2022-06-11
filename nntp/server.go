package nntp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Koshroy/reddit-nntp/data"
	"github.com/Koshroy/reddit-nntp/spool"
)

const (
	GROUP_KEY = iota
	ARTICLE_KEY
)

const CMD_WORD_LIMIT = 2048

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

type rangeClass uint

const (
	CLOSED_RANGE = iota
	HALF_OPEN_RANGE
	SINGLETON_RANGE
)

type articleRange struct {
	low   uint
	high  uint
	class rangeClass
	valid bool
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
		log.Println("error closing connection:", err)
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

func curArticleNum(locals *sync.Map) uint {
	v, ok := locals.Load(ARTICLE_KEY)
	if !ok {
		return 0
	}
	aNum, ok := v.(uint)
	if !ok {
		return 0
	}
	return aNum
}

func setCurArticleNum(locals *sync.Map, aNum uint) {
	locals.Store(ARTICLE_KEY, aNum)
}

func isMessageID(s string) bool {
	if len(s) == 0 {
		return false
	}

	if s[0] == '<' && s[len(s)-1] == '>' {
		return true
	}

	return false
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
			ctxErr := ctx.Err()
			if err != io.EOF && ctxErr != nil && ctxErr != context.Canceled {
				log.Printf("error reading line from connection: %v\n", err)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if line == "" {
			continue
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

			// log.Println("Received line:", line)
			cmd, err := parseLine(line)
			if err != nil {
				log.Printf("error parsing line from client: %v\n", err)
				return
			}

			switch cmd.cmd {
			case "CAPABILITIES":
				if err := printCapabilities(conn); err != nil {
					log.Printf("error sending capabilities to client: %v\n", err)
				}
			case "QUIT":
				if err := printQuit(conn); err != nil && ctx.Err() == nil {
					log.Printf("error sending quit to client: %v\n", err)
				}
				return
			case "LIST":
				if err := handleList(conn, spool, cmd.args); err != nil {
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
				newsgroups, err := spool.Newsgroups()
				if err != nil {
					err = conn.PrintfLine("500 Server error: could not fetch groups")
					if err != nil {
						log.Printf("error sending group to client: %v\n", err)
					}
					continue
				}

				found := false
				for _, ng := range newsgroups {
					if group == ng {
						found = true
					}
				}
				if !found {
					err = conn.PrintfLine("411 No such newsgroup")
					if err != nil {
						log.Printf("error sending group to client: %v\n", err)
					}
					continue
				}

				if err = handleGroup(conn, spool, group, locals); err != nil {
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
					continue
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
					continue
				}
				if err := printArticle(conn, spool, group, cmd.args); err != nil {
					log.Printf("error sending group to client: %v\n", err)
				}
			case "MODE":
				if err := printMode(conn, cmd.args); err != nil {
					log.Printf("error sending group to client: %v\n", err)
				}
			case "NEWGROUPS":
				if len(cmd.args) < 2 {
					err := conn.PrintfLine("403 not enough arguments provided to NEWGROUPS")
					if err != nil {
						log.Printf("error sending error response to client: %v\n", err)
					}
					continue
				}

				err := handleNewGroups(conn, spool, cmd.args[0], cmd.args[1])
				if err != nil {
					log.Println("error sending error response to client:", err)
				}
			case "LISTGROUP":
				var group string
				var explicitGroup bool
				if len(cmd.args) < 1 {
					group = curGroup(locals)
				} else {
					group = cmd.args[0]
					explicitGroup = true
				}

				if group == "" {
					err := conn.PrintfLine("412 No Newsgroup Selected")
					if err != nil {
						log.Println("error sending LISTGROUP error to client:", err)
					}
					continue
				}

				var aRange articleRange
				var err error
				if len(cmd.args) >= 2 {
					aRange, err = parseArticleRange(cmd.args[1])
					if err != nil {
						err := conn.PrintfLine("403 could not parse article range: %v", err)
						if err != nil {
							log.Println("error sending error response to client:", err)
						}
						continue
					}
				}

				firstANum, err := handleListGroup(conn, spool, group, aRange)
				if err != nil {
					log.Println("error sending LISTGROUP response:", err)
				} else {
					// set the current group to this group if a group was specified and articles are returned
					if explicitGroup {
						setCurGroup(locals, group)
						setCurArticleNum(locals, firstANum)
					}
				}
			case "STAT":
				group := curGroup(locals)
				aNum := curArticleNum(locals)
				if err := handleStat(conn, spool, group, aNum, cmd.args); err != nil {
					log.Println("error sending group to client:", err)
				}
			default:
				log.Printf("Unknown command found: %s\n", cmd.cmd)
				if err := printUnknown(conn); err != nil {
					log.Printf("error printing unknown command: %v\n", err)
					continue
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func parseLine(line string) (*nntpCmd, error) {
	splits := strings.SplitN(line, " ", CMD_WORD_LIMIT)
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

func handleGroup(conn *textproto.Conn, spool *spool.Spool, group string, locals *sync.Map) error {
	count, err := spool.GroupArticleCount(group)
	if err != nil {
		log.Println("error getting group", group, "article count:", err)
		return conn.PrintfLine("403 error reading from spool")
	}
	var grpData groupData
	var articlesFound bool
	if count == 0 {
		grpData = groupData{
			name:   group,
			high:   1,
			low:    0,
			status: POSTING_NONPERMITTED,
		}
	} else {
		articlesFound = true
		grpData = groupData{
			name:   group,
			high:   count,
			low:    1,
			status: POSTING_NONPERMITTED,
		}
	}

	setCurGroup(locals, group)
	if articlesFound {
		setCurArticleNum(locals, 1)
	}

	return conn.PrintfLine("211 %s", grpData.String(true))
}

func printHead(conn *textproto.Conn, sp *spool.Spool, group string, args []string) error {
	if len(args) < 1 {
		// TODO: no arg is unsupported
		return conn.PrintfLine("500 current article mode unsupported")
	}

	arg := args[0]
	if len(arg) == 0 {
		log.Println("error: received empty argument that should have been parsed out")
		return conn.PrintfLine("500 could not parse line properly")
	}

	var header *data.Header
	var err error
	var articleNum int
	if isMessageID(arg) {
		// Message-ID mode
		header, err = sp.GetHeaderByMsgID(arg)
		articleNum = 0
	} else {
		articleNum, err = strconv.Atoi(arg)
		if err != nil {
			return conn.PrintfLine("500 could not parse argument properly")
		}

		header, err = sp.GetHeaderByNGNum(group, uint(articleNum))
	}

	if err != nil || header == nil {
		return conn.PrintfLine("423 No article with that number")
	}

	w := conn.DotWriter()
	buf := header.Bytes()
	_, err = w.Write([]byte(fmt.Sprintf("221 %d %s\n", articleNum, header.MsgID)))
	if err != nil {
		w.Close()
		return fmt.Errorf("error writing header response header: %w", err)
	}
	_, err = buf.WriteTo(w)
	if err != nil {
		w.Close()
		return fmt.Errorf("error writing header response: %w", err)
	}

	return w.Close()
}

func printArticle(conn *textproto.Conn, sp *spool.Spool, group string, args []string) error {
	if len(args) < 1 {
		// TODO: no arg is unsupported
		return conn.PrintfLine("500 current article mode unsupported")
	}

	arg := args[0]
	if len(arg) == 0 {
		log.Println("error: received empty argument that should have been parsed out")
		return conn.PrintfLine("500 could not parse line properly")
	}

	var article *data.Article
	var err error
	var articleNum int
	if isMessageID(arg) {
		// Message-ID mode
		article, err = sp.GetArticleByMsgID(group, arg)
		articleNum = 0
	} else {
		articleNum, err = strconv.Atoi(arg)
		if err != nil {
			return conn.PrintfLine("500 could not parse argument properly")
		}

		article, err = sp.GetArticleByNGNum(group, uint(articleNum))
	}

	if err != nil || article == nil {
		return conn.PrintfLine("423 No article with that number")
	}

	w := conn.DotWriter()
	buf := article.Bytes()
	_, err = w.Write([]byte(fmt.Sprintf("220 %d %s\n", articleNum, article.Header.MsgID)))
	if err != nil {
		w.Close()
		return fmt.Errorf("error writing article response header: %w", err)
	}
	_, err = buf.WriteTo(w)
	if err != nil {
		w.Close()
		return fmt.Errorf("error writing article response: %w", err)
	}

	return w.Close()
}

func handleNewGroups(conn *textproto.Conn, sp *spool.Spool, rawDate, rawTime string) error {
	dateTime := rawDate + rawTime

	var groupTime time.Time
	var err error

	if len(rawDate) == 8 {
		groupTime, err = time.Parse("20060102150405", dateTime)
		if err != nil {
			return conn.PrintfLine("403 error parsing date format")
		}
	} else if len(rawDate) == 6 {
		groupTime, err = time.Parse("060102150405", dateTime)
		if err != nil {
			return conn.PrintfLine("403 error parsing date format")
		}
	} else {
		return conn.PrintfLine("403 error parsing date format")
	}

	groups, err := sp.NewGroups(groupTime)
	if err != nil {
		return conn.PrintfLine("500 query to spool failed")
	}

	datum, err := getGroupData(sp, groups)
	if err != nil {
		return conn.PrintfLine("500 error reading from spool")
	}

	w := conn.DotWriter()
	_, err = w.Write([]byte("231 list of newsgroups follows\n"))
	if err != nil {
		w.Close()
		return fmt.Errorf("error returning newgroups status line: %w", err)
	}

	for _, data := range datum {
		_, err = w.Write([]byte(data.String(false)))
		if err != nil {
			w.Close()
			return fmt.Errorf("error writing group response line to socket: %w", err)
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			w.Close()
			return fmt.Errorf("error writing group response line to socket: %w", err)
		}
	}

	return w.Close()
}

func parseArticleRange(rawRange string) (articleRange, error) {
	var aRange articleRange

	if strings.ContainsRune(rawRange, '-') {
		splits := strings.SplitN(rawRange, "-", 2)
		if len(splits) == 0 {
			return aRange, errors.New("could not parse article range: invalid split")
		}

		low, err := strconv.Atoi(splits[0])
		if err != nil {
			return aRange, fmt.Errorf("could not parse lower bound of article range: %w", err)
		}

		var high int
		var closedRange bool
		if len(splits) == 2 && len(splits[1]) > 0 {
			closedRange = true
			high, err = strconv.Atoi(splits[1])
			if err != nil {
				return aRange, fmt.Errorf("could not parse upper bound of closed article range: %w", err)
			}
		}

		aRange.low = uint(low)
		if closedRange {
			aRange.high = uint(high)
			aRange.class = CLOSED_RANGE
		} else {
			aRange.class = HALF_OPEN_RANGE
		}
		aRange.valid = true

		return aRange, nil
	} else {
		num, err := strconv.Atoi(rawRange)
		if err != nil {
			return aRange, fmt.Errorf("could not parse singleton article range: %w", err)
		}

		aRange.low = uint(num)
		aRange.class = SINGLETON_RANGE
		aRange.valid = true

		return aRange, nil
	}
}

func handleListGroup(conn *textproto.Conn, sp *spool.Spool, group string, rng articleRange) (uint, error) {
	if len(group) < 1 {
		return 0, conn.PrintfLine("412 No newsgroup selected")
	}

	aNums, err := sp.GetArticleNumsFromGroup(group)
	if err != nil {
		return 0, conn.PrintfLine("500 query to spool failed")
	}

	if len(aNums) == 0 {
		return 0, conn.PrintfLine("411 group not found")
	}

	var newNums []uint
	if rng.valid {
		newNums = make([]uint, 0)
		for _, aNum := range aNums {
			if rng.class == CLOSED_RANGE {
				if aNum >= rng.low && aNum <= rng.high {
					newNums = append(newNums, aNum)
				}
			} else if rng.class == HALF_OPEN_RANGE {
				if aNum >= rng.low {
					newNums = append(newNums, aNum)
				}
			} else {
				if aNum == rng.low {
					newNums = append(newNums, aNum)
				}
			}
		}
	} else {
		newNums = make([]uint, len(aNums))
		copy(newNums, aNums)
	}

	var min, max, span uint
	if len(newNums) == 0 {
		min = 1
		max = 0
		span = 1
	} else {
		min = newNums[0]
		max = newNums[len(newNums)-1]
		span = max - min + 1
	}

	w := conn.DotWriter()
	_, err = w.Write([]byte(fmt.Sprintf("211 %d %d %d list follows\n", span, min, max)))
	if err != nil {
		w.Close()
		return 0, fmt.Errorf("error returning listgroups status line: %w", err)
	}

	for _, num := range newNums {
		_, err = w.Write([]byte(fmt.Sprintf("%d", num)))
		if err != nil {
			w.Close()
			return 0, fmt.Errorf("error writing article number line to socket: %w", err)
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			w.Close()
			return 0, fmt.Errorf("error writing article number line to socket: %w", err)
		}
	}

	return 1, w.Close()
}

func handleStat(conn *textproto.Conn, sp *spool.Spool, group string, aNum uint, args []string) error {
	const (
		IMPLICIT_STAT = iota
		EXPLICIT_ANUM
		EXPLICIT_MSGID
	)

	queryType := IMPLICIT_STAT
	if len(args) == 0 {
		queryType = IMPLICIT_STAT
	} else if isMessageID(args[0]) {
		queryType = EXPLICIT_MSGID
	} else {
		queryType = EXPLICIT_ANUM
	}

	if (queryType == IMPLICIT_STAT || queryType == EXPLICIT_ANUM) && group == "" {
		err := conn.PrintfLine("412 No Newsgroup Selected")
		if err != nil {
			return fmt.Errorf("error returning stat response: %w", err)
		}
	}

	if queryType == EXPLICIT_MSGID {
		header, err := sp.GetHeaderByMsgID(args[0])
		if err != nil {
			err := conn.PrintfLine("423 No article with that number")
			if err != nil {
				return fmt.Errorf("error returning stat response: %w", err)
			}

			return nil
		}
		// TODO: return the correct article number here
		err = conn.PrintfLine(fmt.Sprintf("223 0 %s", header.MsgID))
		if err != nil {
			return fmt.Errorf("error returning stat response: %w", err)
		}
		return nil
	} else {
		if queryType == EXPLICIT_ANUM {
			num, err := strconv.Atoi(args[0])
			if err != nil {
				err = conn.PrintfLine("423 No article with that number")
				if err != nil {
					return fmt.Errorf("error returning stat response: %w", err)
				}
				return nil
			}

			aNum = uint(num)
		}

		header, err := sp.GetHeaderByNGNum(group, aNum)
		if err != nil {
			err := conn.PrintfLine("423 No article with that number")
			if err != nil {
				return fmt.Errorf("error returning stat response: %w", err)
			}

			return nil
		}

		err = conn.PrintfLine(fmt.Sprintf("223 %d %s", aNum, header.MsgID))
		if err != nil {
			return fmt.Errorf("error returning stat response: %w", err)
		}
		return nil

	}
}
