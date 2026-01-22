package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/packages-service/api/models"
)

const m = "api/store/sqs"

const RestorePackageQueueURLEnvKey = "RESTORE_PACKAGE_QUEUE_URL"

type sqsStore struct {
	Client            *sqs.Client
	RestorePackageURL string
}

type QueueStore interface {
	SendRestorePackage(ctx context.Context, restoreMessage models.RestorePackageMessage) error
}

func NewQueueStore(sqsClient *sqs.Client, restorePackageQueueURL string) QueueStore {
	restorePackageQueue := restorePackageQueueURL
	return &sqsStore{Client: sqsClient, RestorePackageURL: restorePackageQueue}
}

func (s *sqsStore) SendRestorePackage(ctx context.Context, restoreMessage models.RestorePackageMessage) error {
	body, err := json.Marshal(restoreMessage)
	if err != nil {
		return fmt.Errorf("%s: unable to marshal %v: %w", m, restoreMessage, err)
	}
	bodyStr := string(body)
	request := sqs.SendMessageInput{QueueUrl: &s.RestorePackageURL, MessageBody: &bodyStr}
	_, err = s.Client.SendMessage(ctx, &request)
	if err != nil {
		return fmt.Errorf("%s: unable to add %s to the restore package queue: %w", m, bodyStr, err)
	}
	return nil
}
