package log

import (
	"fmt"
	"log"
)

type Logger interface {
	Errorf(format string, v ...interface{})
	Printf(format string, v ...interface{})
	Verbosef(format string, v ...interface{})
	IsEnabled(level Level) bool
}

type Level int

const (
	None Level = iota
	Verbose
	Info
	Error
)

var (
	minLogLevel Level = Verbose
)

func New(prefix string) Logger {
	return &loggerImpl{prefix}
}

func SetMinLevel(level Level) {
	minLogLevel = level
}

func IsEnabled(level Level) bool {
	return level >= minLogLevel
}

type loggerImpl struct {
	prefix string
}

func (i *loggerImpl) IsEnabled(level Level) bool {
	return IsEnabled(level)
}

func (i *loggerImpl) Errorf(format string, v ...interface{}) {
	i.printf(Error, format, v...)
}

func (i *loggerImpl) Printf(format string, v ...interface{}) {
	i.printf(Info, format, v...)
}

func (i *loggerImpl) Verbosef(format string, v ...interface{}) {
	i.printf(Verbose, format, v...)
}

func (i *loggerImpl) printf(level Level, format string, v ...interface{}) {
	if !IsEnabled(level) {
		return
	}

	msg := fmt.Sprintf(format, v...)
	if i.prefix != "" {
		msg = fmt.Sprintf("[%s] %s", i.prefix, msg)
	}

	log.Println(msg)
}
