// Package logging provides the request-scoped logger that is dependency-injected
// through the handler -> service -> store layers.
//
// The DI shape here is deliberate and predates this file's slog conversion: a
// Logger interface is constructed once per invocation with the identifiers for
// that invocation already attached, then threaded down through
// service.NewPackagesService -> store.WithLogging -> store.NewQueries, so every
// log line emitted anywhere in the call tree carries the same correlation
// fields without each layer having to re-thread them by hand.
//
// What changed is only the backing store: log/slog instead of sirupsen/logrus.
// Fields is a plain map[string]any (it used to be logrus.Fields), so the
// ...WithFields call sites are unchanged apart from the type name.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
)

// Fields is a set of structured log attributes. Prefer the key constants in
// keys.go over free-text keys so a DataDog/CloudWatch query can filter on them.
type Fields map[string]any

// Logger is the injected logging surface. Kept method-for-method compatible
// with the pre-slog interface so the existing dependency-injection chain and
// its call sites did not have to be rearchitected.
type Logger interface {
	LogError(args ...any)
	LogErrorWithFields(fields Fields, args ...any)
	LogWarn(args ...any)
	LogWarnWithFields(fields Fields, args ...any)
	LogInfo(args ...any)
	LogInfoWithFields(fields Fields, args ...any)
	LogDebug(args ...any)
	LogDebugWithFields(fields Fields, args ...any)
}

// Log is the slog-backed implementation of Logger. The embedded *slog.Logger is
// exported so callers that want to emit directly in slog style
// (logger.Info("msg", slog.String(...))) can, while the LogXxx methods keep the
// older call sites working.
type Log struct {
	*slog.Logger
}

// NewLog wraps an existing *slog.Logger.
func NewLog(logger *slog.Logger) *Log {
	return &Log{Logger: logger}
}

// NewLogWithFields returns a Log derived from slog.Default() with the given
// fields permanently attached — the per-invocation constructor used by the
// Lambda entrypoints.
func NewLogWithFields(fields Fields) *Log {
	return &Log{Logger: slog.Default().With(attrs(fields)...)}
}

// WithFields returns a child Log carrying additional permanent fields, for
// enriching a request-scoped logger once more context has been resolved.
func (l *Log) WithFields(fields Fields) *Log {
	return &Log{Logger: l.Logger.With(attrs(fields)...)}
}

// attrs converts a Fields map to slog attributes, sorted by key so that log
// output is deterministic (map iteration order otherwise varies per call,
// which makes log diffing and tests unnecessarily painful).
func attrs(fields Fields) []any {
	if len(fields) == 0 {
		return nil
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, slog.Any(k, fields[k]))
	}
	return out
}

// message renders the variadic args of the LogXxx methods into a single slog
// message, matching how logrus's Info/Error/... joined their operands. Call
// sites that pass an error as the sole arg still read correctly; new code
// should prefer a static message plus a structured "error" field.
func message(args []any) string {
	switch len(args) {
	case 0:
		return ""
	case 1:
		if s, ok := args[0].(string); ok {
			return s
		}
		return fmt.Sprint(args[0])
	default:
		return fmt.Sprint(args...)
	}
}

func (l *Log) LogError(args ...any) {
	l.Logger.Error(message(args))
}

func (l *Log) LogErrorWithFields(fields Fields, args ...any) {
	l.Logger.Error(message(args), attrs(fields)...)
}

func (l *Log) LogWarn(args ...any) {
	l.Logger.Warn(message(args))
}

func (l *Log) LogWarnWithFields(fields Fields, args ...any) {
	l.Logger.Warn(message(args), attrs(fields)...)
}

func (l *Log) LogInfo(args ...any) {
	l.Logger.Info(message(args))
}

func (l *Log) LogInfoWithFields(fields Fields, args ...any) {
	l.Logger.Info(message(args), attrs(fields)...)
}

func (l *Log) LogDebug(args ...any) {
	l.Logger.Debug(message(args))
}

func (l *Log) LogDebugWithFields(fields Fields, args ...any) {
	l.Logger.Debug(message(args), attrs(fields)...)
}

// SetDefaultLogger builds a JSON logger at the given level and installs it as
// slog.Default. Returns the logger and its LevelVar (so the level can be
// changed at runtime). An unparseable level falls back to INFO.
func SetDefaultLogger(level string) (*slog.Logger, *slog.LevelVar) {
	logger, levelVar := NewJSONLogger(level)
	slog.SetDefault(logger)
	slog.Debug("log level set", slog.String("level", levelVar.String()))
	return logger, levelVar
}

// SetDefaultFromEnv installs the default logger using LOG_LEVEL from the
// environment (INFO when unset/invalid). Convenience wrapper for the Lambda
// entrypoints, whose deployments already set LOG_LEVEL.
func SetDefaultFromEnv() (*slog.Logger, *slog.LevelVar) {
	return SetDefaultLogger(os.Getenv("LOG_LEVEL"))
}

// NewJSONLogger returns a JSON-handler slog.Logger at the given level (INFO if
// the string is empty or unparseable) plus its LevelVar. Pure constructor: no
// global side effects, so tests can build isolated loggers.
func NewJSONLogger(level string) (*slog.Logger, *slog.LevelVar) {
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(level)); err != nil {
		if level != "" {
			slog.Error("error unmarshalling log level value",
				slog.String("logLevel", level),
				slog.Any("error", err))
		}
		logLevel = slog.LevelInfo
	}

	levelVar := new(slog.LevelVar)
	levelVar.Set(logLevel)

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: levelVar})
	return slog.New(handler), levelVar
}
