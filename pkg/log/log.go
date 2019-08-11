package log

import (
	"fmt"
	"log"
)

type Logger interface {
	Printf(format string, v ...interface{})
	Fatalf(format string, v ...interface{})
	Panicf(format string, v ...interface{})
}

func New(prefix string) Logger {
	return &loggerImpl{prefix}
}

type loggerImpl struct {
	prefix string
}

func (i *loggerImpl) Printf(format string, v ...interface{}) {
	log.Println(i.formatf(format, v...))
}

func (i *loggerImpl) Fatalf(format string, v ...interface{}) {
	log.Fatalln(i.formatf(format, v...))
}

func (i *loggerImpl) Panicf(format string, v ...interface{}) {
	log.Panicln(i.formatf(format, v...))
}

func (i *loggerImpl) formatf(format string, v ...interface{}) string {
	msg := fmt.Sprintf(format, v...)
	if i.prefix != "" {
		msg = fmt.Sprintf("%s: %s", i.prefix, msg)
	}
	return msg
}
