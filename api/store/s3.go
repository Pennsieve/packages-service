package store

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Store struct {
	Client *s3.Client
}

type ObjectStore interface {
}

func NewObjectStore(s3Client *s3.Client) ObjectStore {
	return &s3Store{Client: s3Client}
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
