package data

import (
	"bytes"
	"html"
	"strings"
	"time"
)

const nntpTimeFormat = "02 Jan 2006 15:04 -0700"

type Header struct {
	PostedAt   time.Time
	Newsgroup  string
	Subject    string
	Author     string
	MsgID      string
	References []string
}

func (h Header) Bytes() bytes.Buffer {
	var buf bytes.Buffer

	buf.WriteString("Path: reddit!not-for-mail\n")
	buf.WriteString("From: ")
	buf.WriteString(h.Author)
	buf.WriteRune('\n')
	buf.WriteString("Newsgroups: ")
	buf.WriteString(h.Newsgroup)
	buf.WriteRune('\n')
	buf.WriteString("Subject: ")
	buf.WriteString(unQuoteHTMLString(h.Subject))
	buf.WriteRune('\n')
	buf.WriteString("Date: ")
	buf.WriteString(h.PostedAt.Format(nntpTimeFormat))
	buf.WriteRune('\n')
	buf.WriteString("Message-ID: ")
	buf.WriteString(h.MsgID)
	buf.WriteRune('\n')
	if len(h.References) > 0 {
		buf.WriteString("References: ")
		for i, ref := range h.References {
			if i > 0 {
				buf.WriteRune(',')
			}
			buf.WriteString(ref)
		}
	}
	buf.WriteRune('\n')

	return buf
}

type Article struct {
	Header Header
	Body   []byte
}

func (a Article) Bytes() bytes.Buffer {
	var buf bytes.Buffer

	hdrBytes := a.Header.Bytes()
	buf.ReadFrom(&hdrBytes)
	buf.WriteRune('\n')
	buf.Write(unQuoteHTML(a.Body))

	return buf
}

func unQuoteHTML(body []byte) []byte {
	bodyStr := strings.ReplaceAll(string(body), "&#x200B;", "")
	return []byte(html.UnescapeString(bodyStr))
}

func unQuoteHTMLString(payload string) string {
	bodyStr := strings.ReplaceAll(payload, "&#x200B;", "")
	return html.UnescapeString(bodyStr)
}
