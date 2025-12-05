package log

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Level represents the logging level
type Level int

const (
	TRACE Level = iota
	DEBUG
	INFO
	ERROR
)

// String returns the string representation of the level
func (l Level) String() string {
	switch l {
	case TRACE:
		return "TRACE"
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel parses a string into a Level
func ParseLevel(s string) Level {
	switch strings.ToLower(s) {
	case "trace":
		return TRACE
	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "error":
		return ERROR
	default:
		return INFO
	}
}

// Logger provides structured logging
type Logger struct {
	level Level
}

// global logger instance
var defaultLogger = &Logger{level: INFO}

// SetLevel sets the global log level
func SetLevel(level Level) {
	defaultLogger.level = level
}

// GetLevel returns the current log level
func GetLevel() Level {
	return defaultLogger.level
}

// log outputs a log message if the level is enabled
func (l *Logger) log(level Level, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] [%s] %s\n", timestamp, level.String(), message)
}

// Trace logs a trace message
func Trace(format string, args ...interface{}) {
	defaultLogger.log(TRACE, format, args...)
}

// Debug logs a debug message
func Debug(format string, args ...interface{}) {
	defaultLogger.log(DEBUG, format, args...)
}

// Info logs an info message
func Info(format string, args ...interface{}) {
	defaultLogger.log(INFO, format, args...)
}

// Error logs an error message
func Error(format string, args ...interface{}) {
	defaultLogger.log(ERROR, format, args...)
}

// Fatal logs an error message and exits
func Fatal(format string, args ...interface{}) {
	defaultLogger.log(ERROR, format, args...)
	os.Exit(1)
}
