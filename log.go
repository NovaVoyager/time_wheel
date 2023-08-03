package time_wheel

import (
	"github.com/fatih/color"
)

type LogLevel int

type printFunc func(format string, a ...interface{})

const (
	// Debug debug log level
	Debug LogLevel = iota + 1
	// Info info log level
	Info
	// Warn warn log level
	Warn
	// Error error log level
	Error
)

var (
	warnf  = color.New(color.FgYellow).PrintfFunc()
	infof  = color.New(color.FgBlue).PrintfFunc()
	errorf = color.New(color.FgRed).PrintfFunc()
	debugf = color.New(color.FgWhite).PrintfFunc()
)

type Log interface {
	Debug(string, ...interface{})
	Info(string, ...interface{})
	Warn(string, ...interface{})
	Error(string, ...interface{})
}

type logger struct {
	logLevel LogLevel
}

func (l *logger) Info(str string, args ...interface{}) {
	if l.logLevel <= Info {
		printLog(infof, str, args...)
	}
}

func (l *logger) Warn(str string, args ...interface{}) {
	if l.logLevel <= Warn {
		printLog(warnf, str, args...)
	}
}

func (l *logger) Error(str string, args ...interface{}) {
	if l.logLevel <= Error {
		printLog(errorf, str, args...)
	}
}

func (l *logger) Debug(str string, args ...interface{}) {
	if l.logLevel <= Debug {
		printLog(debugf, str, args...)
	}
}

func printLog(f printFunc, str string, args ...interface{}) {
	if len(args) > 0 {
		f(str, args)
	} else {
		f(str)
	}
}
