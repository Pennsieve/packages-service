package store

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pennsieve/packages-service/api/logging"
)

const maxDeleteObjects = 1000

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
	DeleteObjectsVersion(ctx context.Context, objInfos ...S3ObjectInfo) ([]types.Error, error)
	logging.Logger
}

func (s *s3Store) DeleteObjectsVersion(ctx context.Context, objInfos ...S3ObjectInfo) ([]types.Error, error) {
	if len(objInfos) == 0 {
		return nil, nil
	}
	var awsErrors []types.Error
	byBucket := map[string][][]types.ObjectIdentifier{}
	for _, objInfo := range objInfos {
		bucket := objInfo.Bucket
		objectId := types.ObjectIdentifier{
			Key:       aws.String(objInfo.Key),
			VersionId: aws.String(objInfo.VersionId),
		}
		batches := byBucket[bucket]
		nBatches := len(batches)
		if nBatches == 0 {
			byBucket[bucket] = append(batches, []types.ObjectIdentifier{})
		}
		lastBatchIdx := len(byBucket[bucket]) - 1
		batch := byBucket[bucket][lastBatchIdx]
		if len(batch) < maxDeleteObjects {
			byBucket[bucket][lastBatchIdx] = append(batch, objectId)
		} else {
			byBucket[bucket] = append(byBucket[bucket], []types.ObjectIdentifier{objectId})
		}
	}
	for bucket, batches := range byBucket {
		for i, batch := range batches {
			input := s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &types.Delete{
					Objects: batch,
				},
			}
			if output, err := s.Client.DeleteObjects(ctx, &input); err != nil {
				return nil, fmt.Errorf("api/store/s3: error deleting batch %d of %d for bucket %s: %w", i, len(batches), bucket, err)
			} else {
				awsErrors = append(awsErrors, output.Errors...)
			}
		}
	}
	return awsErrors, nil
}
