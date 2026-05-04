package restore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/logging"
	log "github.com/sirupsen/logrus"
)

const FileFinalizedTopicEnvKey = "FILE_FINALIZED_TOPIC_ARN"

// RescanFile is the minimal projection needed to publish a scan-trigger
// event. Sourced from a per-org files row for a package being restored
// from trash.
type RescanFile struct {
	FileID   int64
	FileUUID string
	S3Bucket string
	S3Key    string
	Size     int64
}

// fileFinalizedEvent mirrors the field set scan-service actually reads
// (scan-service/cmd/scanner/main.go references FileID, OrganizationID,
// FileUUID, S3Bucket, S3Key, Size — nothing else). manifestId / fileType /
// extension / checksum / complianceTier / datasetId / timestamp / traceId
// are intentionally omitted: scan-service ignores them, and a restore-
// triggered event has no upload-session manifest to honestly cite.
type fileFinalizedEvent struct {
	EventType      string `json:"eventType"`
	EventVersion   int    `json:"eventVersion"`
	FileID         int64  `json:"fileId"`
	FileUUID       string `json:"fileUUID"`
	S3Bucket       string `json:"s3Bucket"`
	S3Key          string `json:"s3Key"`
	Size           int64  `json:"size"`
	OrganizationID int    `json:"organizationId"`
}

type ScanTriggerStore interface {
	PublishRescanRequests(ctx context.Context, orgId int, files []RescanFile) error
	logging.Logger
}

// SNSPublisher is the subset of the SNS client we use; lets tests inject
// a fake without standing up a real SNS endpoint.
type SNSPublisher interface {
	PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

type SNSScanTriggerStore struct {
	Client   SNSPublisher
	TopicArn string
}

func NewSNSScanTriggerStore(snsClient SNSPublisher, topicArn string) *SNSScanTriggerStore {
	return &SNSScanTriggerStore{Client: snsClient, TopicArn: topicArn}
}

func (s *SNSScanTriggerStore) WithLogging(log *logging.Log) ScanTriggerStore {
	return &snsScanTriggerStore{
		SNSScanTriggerStore: s,
		Log:                 log,
	}
}

type snsScanTriggerStore struct {
	*SNSScanTriggerStore
	*logging.Log
}

const scanTriggerBatchSize = 10

// PublishRescanRequests emits one FileFinalized event per file to the
// FileFinalized SNS topic, in PublishBatch chunks. A missing topic ARN
// (local/test env) is a no-op. Caller treats errors as best-effort —
// a publish failure must not roll back a successful restore.
func (s *snsScanTriggerStore) PublishRescanRequests(ctx context.Context, orgId int, files []RescanFile) error {
	if s.TopicArn == "" || len(files) == 0 {
		return nil
	}

	entries := make([]snstypes.PublishBatchRequestEntry, 0, scanTriggerBatchSize)
	for _, f := range files {
		evt := fileFinalizedEvent{
			EventType:      "FileFinalized",
			EventVersion:   1,
			FileID:         f.FileID,
			FileUUID:       f.FileUUID,
			S3Bucket:       f.S3Bucket,
			S3Key:          f.S3Key,
			Size:           f.Size,
			OrganizationID: orgId,
		}
		body, err := json.Marshal(evt)
		if err != nil {
			s.LogWarnWithFields(log.Fields{"fileId": f.FileID, "error": err}, "scan trigger marshal failed; skipping file")
			continue
		}
		entries = append(entries, snstypes.PublishBatchRequestEntry{
			Id:      aws.String(uuid.NewString()),
			Message: aws.String(string(body)),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"eventType": {DataType: aws.String("String"), StringValue: aws.String("FileFinalized")},
			},
		})
		if len(entries) == scanTriggerBatchSize {
			if err := s.publishBatch(ctx, entries); err != nil {
				return err
			}
			entries = entries[:0]
		}
	}
	if len(entries) > 0 {
		if err := s.publishBatch(ctx, entries); err != nil {
			return err
		}
	}
	return nil
}

func (s *snsScanTriggerStore) publishBatch(ctx context.Context, entries []snstypes.PublishBatchRequestEntry) error {
	out, err := s.Client.PublishBatch(ctx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(s.TopicArn),
		PublishBatchRequestEntries: entries,
	})
	if err != nil {
		return fmt.Errorf("scan trigger PublishBatch: %w", err)
	}
	for _, f := range out.Failed {
		s.LogWarnWithFields(log.Fields{
			"id":   aws.ToString(f.Id),
			"code": aws.ToString(f.Code),
			"msg":  aws.ToString(f.Message),
		}, "scan trigger PublishBatch entry failed")
	}
	return nil
}