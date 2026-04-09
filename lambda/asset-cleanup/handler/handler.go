package handler

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
)

var (
	PennsieveDB *sql.DB
	S3Client    *s3.Client
)

type cleanupEntry struct {
	ID        int64
	OrgID     int64
	DatasetID int64
	AssetID   string
	S3Bucket  string
}

func HandleCleanup(ctx context.Context) error {
	entries, err := fetchCleanupEntries(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch cleanup entries: %w", err)
	}

	if len(entries) == 0 {
		log.Info("no viewer asset cleanup entries to process")
		return nil
	}

	log.Infof("processing %d viewer asset cleanup entries", len(entries))

	for _, entry := range entries {
		prefix := fmt.Sprintf("viewer-assets/O%d/D%d/%s/", entry.OrgID, entry.DatasetID, entry.AssetID)

		log.WithFields(log.Fields{
			"entryID":  entry.ID,
			"orgID":    entry.OrgID,
			"assetID":  entry.AssetID,
			"s3Bucket": entry.S3Bucket,
			"prefix":   prefix,
		}).Info("cleaning up S3 objects for deleted viewer asset")

		if err := deleteS3Prefix(ctx, entry.S3Bucket, prefix); err != nil {
			log.WithError(err).WithField("entryID", entry.ID).Error("failed to delete S3 objects, will retry next run")
			continue
		}

		if err := removeCleanupEntry(ctx, entry.ID); err != nil {
			log.WithError(err).WithField("entryID", entry.ID).Error("failed to remove cleanup entry")
			continue
		}

		log.WithField("entryID", entry.ID).Info("cleanup complete")
	}

	return nil
}

func fetchCleanupEntries(ctx context.Context) ([]cleanupEntry, error) {
	rows, err := PennsieveDB.QueryContext(ctx,
		`SELECT id, org_id, dataset_id, asset_id, s3_bucket
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
		if err := rows.Scan(&e.ID, &e.OrgID, &e.DatasetID, &e.AssetID, &e.S3Bucket); err != nil {
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

func deleteS3Prefix(ctx context.Context, bucket, prefix string) error {
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
		log.Infof("deleted %d objects from s3://%s/%s", totalDeleted, bucket, prefix)
	}

	return nil
}