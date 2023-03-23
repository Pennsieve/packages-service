package store

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/packages-service/api/logging"
)

type S3Store struct {
	Client *s3.Client
}

func NewS3Store(s3Client *s3.Client) *S3Store {
	return &S3Store{Client: s3Client}
}

func (s *S3Store) WithLogging(log *logging.Log) ObjectStore {
	return &s3Store{
		S3Store: s,
		Log:     log,
	}
}

type s3Store struct {
	*S3Store
	*logging.Log
}

type ObjectStore interface {
	logging.Logger
}

func (s *s3Store) DeleteObjectVersion(ctx context.Context, bucket, key, versionId string) error {
	input := s3.DeleteObjectInput{
		Bucket:    &bucket,
		Key:       &key,
		VersionId: &versionId,
	}
	if _, err := s.Client.DeleteObject(ctx, &input); err != nil {
		return fmt.Errorf("api/store/s3: error deleting %s:%s, version %s: %w", bucket, key, versionId, err)
	}
	return nil
}
