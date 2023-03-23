package logging

import log "github.com/sirupsen/logrus"

type Logger interface {
	LogError(args ...any)
	LogErrorWithFields(fields log.Fields, args ...any)
	LogWarn(args ...any)
	LogWarnWithFields(fields log.Fields, args ...any)
	LogInfo(args ...any)
	LogInfoWithFields(fields log.Fields, args ...any)
	LogDebug(args ...any)
	LogDebugWithFields(fields log.Fields, args ...any)
}

type Log struct {
	*log.Entry
}

func NewLogWithFields(fields log.Fields) *Log {
	logger := log.WithFields(fields)
	return &Log{logger}
}

func (l *Log) LogError(args ...any) {
	l.Error(args...)
}

func (l *Log) LogWarn(args ...any) {
	l.Warn(args...)
}

func (l *Log) LogWarnWithFields(fields log.Fields, args ...any) {
	l.WithFields(fields).Warn(args...)
}

func (l *Log) LogErrorWithFields(fields log.Fields, args ...any) {
	l.WithFields(fields).Error(args...)
}

func (l *Log) LogInfo(args ...any) {
	l.Info(args...)
}

func (l *Log) LogInfoWithFields(fields log.Fields, args ...any) {
	l.WithFields(fields).Info(args...)
}

func (l *Log) LogDebug(args ...any) {
	l.Debug(args...)
}

func (l *Log) LogDebugWithFields(fields log.Fields, args ...any) {
	l.WithFields(fields).Debug(args...)
}
