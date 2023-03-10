package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/packages-service/api/models"
	"os"
)

const m = "api/store/sqs"

type sqsStore struct {
	Client            *sqs.Client
	RestorePackageURL *string
}

type QueueStore interface {
	SendRestorePackage(ctx context.Context, restoreMessage models.RestorePackageMessage) error
}

func NewQueueStore(config aws.Config) (QueueStore, error) {
	client := sqs.NewFromConfig(config)
	restorePackageQueue := os.Getenv("RESTORE_PACKAGE_QUEUE")
	restorePackageUrlRequest := sqs.GetQueueUrlInput{QueueName: &restorePackageQueue}
	restorePackageResp, err := client.GetQueueUrl(context.Background(), &restorePackageUrlRequest)
	if err != nil {
		return nil, fmt.Errorf("%s: unable to get restore package queue URL from name %q: %w", m, restorePackageQueue, err)
	}
	return &sqsStore{Client: client, RestorePackageURL: restorePackageResp.QueueUrl}, nil
}

func (s *sqsStore) SendRestorePackage(ctx context.Context, restoreMessage models.RestorePackageMessage) error {
	body, err := json.Marshal(restoreMessage)
	if err != nil {
		return fmt.Errorf("%s: unable to marshal %s: %w", m, restoreMessage, err)
	}
	bodyStr := string(body)
	request := sqs.SendMessageInput{QueueUrl: s.RestorePackageURL, MessageBody: &bodyStr}
	_, err = s.Client.SendMessage(ctx, &request)
	if err != nil {
		return fmt.Errorf("%s: unable to add %s to the restore package queue: %w", m, bodyStr, err)
	}
	return nil
}
