package restore

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"time"
)

const JobsQueueIDEnvKey = "JOBS_QUEUE_ID"

type SQSChangelogStore struct {
	Client *changelog.Client
	Queue  string
}

func NewSQSChangelogStore(sqsClient *sqs.Client, jobsQueueID string) *SQSChangelogStore {
	return &SQSChangelogStore{Client: changelog.NewClient(*sqsClient, jobsQueueID), Queue: jobsQueueID}
}

func (s *SQSChangelogStore) WithLogging(log *logging.Log) ChangelogStore {
	return &sqsChangelogStore{
		SQSChangelogStore: s,
		Log:               log,
	}
}

type sqsChangelogStore struct {
	*SQSChangelogStore
	*logging.Log
}

type ChangelogStore interface {
	LogRestores(ctx context.Context, orgId, datasetId int64, userId string, changelogEvents []changelog.PackageRestoreEvent) error
	logging.Logger
}

func (s *sqsChangelogStore) LogRestores(ctx context.Context, orgId, datasetId int64, userId string, changelogEvents []changelog.PackageRestoreEvent) error {
	events := make([]changelog.Event, len(changelogEvents))
	now := time.Now()
	for i, e := range changelogEvents {
		events[i] = changelog.Event{
			EventType:   changelog.RestorePackage,
			EventDetail: e,
			Timestamp:   now,
		}
	}
	params := changelog.MessageParams{
		OrganizationId: orgId,
		DatasetId:      datasetId,
		UserId:         userId,
		Events:         events,
		TraceId:        uuid.NewString(),
		Id:             uuid.NewString(),
	}

	message := changelog.Message{
		DatasetChangelogEventJob: params,
	}
	if err := s.Client.EmitEvents(ctx, message); err != nil {
		return fmt.Errorf("api/store/restore error sending restore changelog events to queue %s: %w", s.Queue, err)
	}
	return nil
}
