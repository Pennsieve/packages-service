package store

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	log "github.com/sirupsen/logrus"
	"os"
)

type sqsStore struct {
	Client            *sqs.Client
	RestorePackageURL *string
}

type QueueStore interface {
}

func NewQueueStore(config aws.Config) (QueueStore, error) {
	client := sqs.NewFromConfig(config)
	restorePackageQueue := os.Getenv("RESTORE_PACKAGE_QUEUE")
	restorePackageUrlRequest := sqs.GetQueueUrlInput{QueueName: &restorePackageQueue}
	restorePackageResp, err := client.GetQueueUrl(context.Background(), &restorePackageUrlRequest)
	if err != nil {
		return nil, fmt.Errorf("unable to get restore package queue URL from name %q: %w", restorePackageQueue, err)
	}
	log.Info("restore package queue URL:", restorePackageResp.QueueUrl)
	return &sqsStore{Client: client, RestorePackageURL: restorePackageResp.QueueUrl}, nil
}
