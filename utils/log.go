package utils

import (
	"log"
	"os"
)

type MapLevelPrefix map[LogLevel]string

type LogLevel int

const (
	LogVerbose LogLevel = iota
	LogInfo
	LogWarning
	LogError
)

type AppLogger struct {
	Levels MapLevelPrefix
	*log.Logger
}

func (A *AppLogger) LeveledFunc(level LogLevel, f func(...any), args ...any) {
	if prefix, ok := A.Levels[level]; ok {
		A.SetPrefix(prefix)
		f(args...)
	}
}

func (A *AppLogger) LeveledFuncF(level LogLevel, f func(string, ...any), format string, args ...any) {
	if prefix, ok := A.Levels[level]; ok {
		A.SetPrefix(prefix)
		f(format, args...)
	}
}

func (A *AppLogger) PrintError(i ...interface{}) {
	A.LeveledFunc(LogError, A.Print, i...)
}

func (A *AppLogger) PrintErrorf(format string, i ...interface{}) {
	A.LeveledFuncF(LogError, A.Printf, format, i...)
}

func (A *AppLogger) PrintErrorln(i ...interface{}) {
	A.LeveledFunc(LogError, A.Println, i...)
}

func NewAppLogger(levels map[LogLevel]string) AppLogger {
	return AppLogger{
		Levels: levels,
		Logger: log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmsgprefix),
	}
}
