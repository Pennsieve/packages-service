package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/google/uuid"
)

// Structured log attribute keys.
//
// This lambda is its own Go module (it does not depend on
// packages-service/api), so it carries a small local copy of the key constants
// rather than taking on that module just for logging. Keep the string values
// identical to api/logging/keys.go so that one DataDog query spans both.
const (
	keyTraceID       = "traceId"
	keyAWSRequestID  = "awsRequestId"
	keyError         = "error"
	keySecretName    = "secretName"
	keySecretVersion = "secretVersionId"
	keyRotationStep  = "rotationStep"
	keyPublicKeyID   = "cloudFrontPublicKeyId"
	keyKeyGroupID    = "cloudFrontKeyGroupId"
	keyKeyID         = "cloudFrontKeyId"
	keyPublicKeyName = "cloudFrontPublicKeyName"
	keyGraceHours    = "gracePeriodHoursRemaining"
	keyTokenLength   = "clientRequestTokenLength"
)

// setDefaultLoggerFromEnv installs a JSON slog logger at LOG_LEVEL (INFO when
// unset or unparseable) as slog.Default.
func setDefaultLoggerFromEnv() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
}

type loggerCtxKey struct{}

// withLogger stores the per-invocation logger on the context.
//
// This lambda's rotation steps are a deep call tree of package-level functions
// that already thread ctx everywhere, so carrying the request-scoped logger on
// the context gets the trace id onto every log line without changing eleven
// function signatures.
func withLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerCtxKey{}, logger)
}

// logger retrieves the per-invocation logger, falling back to the default one
// if the context does not carry it.
func logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerCtxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// newInvocationLogger builds the per-invocation logger.
//
// Secrets Manager invokes this lambda directly on a rotation schedule, so there
// is no caller-supplied correlation id to adopt: it mints its own traceId per
// invocation. The Lambda request id is logged separately under its own key —
// the two are different kinds of id (one per hop, one per logical operation)
// and are deliberately not conflated.
func newInvocationLogger(ctx context.Context, event RotationEvent) *slog.Logger {
	l := slog.Default().With(
		slog.String(keyTraceID, uuid.NewString()),
		slog.String(keyRotationStep, event.Step),
		slog.String(keySecretName, event.SecretId),
	)
	if lc, ok := lambdacontext.FromContext(ctx); ok && lc != nil {
		l = l.With(slog.String(keyAWSRequestID, lc.AwsRequestID))
	}
	return l
}
