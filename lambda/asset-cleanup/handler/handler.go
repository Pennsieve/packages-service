package handler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Structured log attribute keys. This lambda is its own Go module and does not
// depend on packages-service/api, so it carries a small local copy of the key
// constants it needs rather than pulling in that module just for logging. Keep
// the string values identical to api/logging/keys.go so a single DataDog query
// spans both.
const (
	keyTraceID      = "traceId"
	keyAWSRequestID = "awsRequestId"
	keyError        = "error"
	keyEntryID      = "entryId"
	keyS3Bucket     = "s3Bucket"
	keyS3Prefix     = "s3Prefix"
	keyCount        = "count"
	keyDeletedCount = "deletedCount"
)

// SetDefaultLoggerFromEnv installs a JSON slog logger at LOG_LEVEL (INFO when
// unset or unparseable) as slog.Default.
func SetDefaultLoggerFromEnv() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
}

// newInvocationLogger returns the per-invocation logger.
//
// This lambda is triggered on a schedule rather than by a caller, so there is no
// inbound correlation id to adopt: it mints its own traceId per invocation, and
// logs the Lambda request id separately under its own key (the two are
// different kinds of id and are deliberately not conflated).
func newInvocationLogger(ctx context.Context) *slog.Logger {
	logger := slog.Default().With(slog.String(keyTraceID, uuid.NewString()))
	if lc, ok := lambdacontext.FromContext(ctx); ok && lc != nil {
		logger = logger.With(slog.String(keyAWSRequestID, lc.AwsRequestID))
	}
	return logger
}

var (
	PennsieveDB *sql.DB
	S3Client    *s3.Client
)

type cleanupEntry struct {
	ID       int64
	S3Bucket string
	S3Prefix string
}

func HandleCleanup(ctx context.Context) error {
	logger := newInvocationLogger(ctx)
	for {
		entries, err := fetchCleanupEntries(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch cleanup entries: %w", err)
		}

		if len(entries) == 0 {
			logger.Info("no more viewer asset cleanup entries to process")
			return nil
		}

		logger.Info("processing viewer asset cleanup entries", slog.Int(keyCount, len(entries)))

		for _, entry := range entries {
			entryLogger := logger.With(
				slog.Int64(keyEntryID, entry.ID),
				slog.String(keyS3Bucket, entry.S3Bucket),
				slog.String(keyS3Prefix, entry.S3Prefix))
			entryLogger.Info("cleaning up S3 objects for deleted viewer asset")

			if err := deleteS3Prefix(ctx, entryLogger, entry.S3Bucket, entry.S3Prefix); err != nil {
				entryLogger.Error("failed to delete S3 objects, will retry next run", slog.Any(keyError, err))
				continue
			}

			if err := removeCleanupEntry(ctx, entry.ID); err != nil {
				entryLogger.Error("failed to remove cleanup entry", slog.Any(keyError, err))
				continue
			}

			entryLogger.Info("cleanup complete")
		}
	}
}

func fetchCleanupEntries(ctx context.Context) ([]cleanupEntry, error) {
	rows, err := PennsieveDB.QueryContext(ctx,
		`SELECT id, s3_bucket, s3_prefix
		 FROM pennsieve.viewer_asset_cleanup_queue
		 ORDER BY created_at ASC
		 LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []cleanupEntry
	for rows.Next() {
		var e cleanupEntry
		if err := rows.Scan(&e.ID, &e.S3Bucket, &e.S3Prefix); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func removeCleanupEntry(ctx context.Context, id int64) error {
	_, err := PennsieveDB.ExecContext(ctx,
		`DELETE FROM pennsieve.viewer_asset_cleanup_queue WHERE id = $1`, id)
	return err
}

func deleteS3Prefix(ctx context.Context, logger *slog.Logger, bucket, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(S3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	totalDeleted := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		if len(page.Contents) == 0 {
			continue
		}

		objects := make([]types.ObjectIdentifier, len(page.Contents))
		for i, obj := range page.Contents {
			objects[i] = types.ObjectIdentifier{Key: obj.Key}
		}

		_, err = S3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: objects},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		totalDeleted += len(objects)
	}

	if totalDeleted > 0 {
		logger.Info("deleted S3 objects",
			slog.Int(keyDeletedCount, totalDeleted),
			slog.String(keyS3Bucket, bucket),
			slog.String(keyS3Prefix, prefix))
	}

	return nil
}
