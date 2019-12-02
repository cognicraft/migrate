package migrate

import (
	"bufio"
	"bytes"
	"log"
	"regexp"
	"strings"
)

func Statements(script string) []string {
	ss := []string{}
	builder := NewStatementBuilder()
	scanner := bufio.NewScanner(strings.NewReader(script))
	for scanner.Scan() {
		builder.Append(scanner.Text())
		if builder.IsTerminated() {
			ss = append(ss, builder.Statement())
			builder = NewStatementBuilder()
		}
	}
	return ss
}

func NewStatementBuilder() *StatementBuilder {
	return &StatementBuilder{
		buffer: &bytes.Buffer{},
	}
}

type StatementBuilder struct {
	createTrigger bool
	terminated    bool
	buffer        *bytes.Buffer
}

func (b *StatementBuilder) Append(line string) {
	line = strings.TrimSpace(line)
	var err error
	if b.buffer.Len() == 0 {
		b.createTrigger, err = regexp.MatchString("CREATE( TEMP| TEMPORARY)? TRIGGER.*", line)
		if err != nil {
			log.Printf("match: %+v", err)
		}
	} else {
		b.buffer.WriteString("\n")
	}
	b.buffer.WriteString(line)
	if b.createTrigger {
		b.terminated = strings.HasSuffix(line, "END;")
	} else {
		b.terminated = strings.HasSuffix(line, ";")
	}
}

func (b *StatementBuilder) IsTerminated() bool {
	return b.terminated
}

func (b *StatementBuilder) Statement() string {
	return b.buffer.String()
}
