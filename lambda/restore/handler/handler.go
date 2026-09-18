package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	pennsievelog "github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/packages-service/api/store/restore"
	changelog2 "github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"os"
)

const m = "restore/handler"

var PennsieveDB *sql.DB
var S3Client *s3.Client
var DyDBClient *dynamodb.Client
var SQSClient *sqs.Client

type BaseStore interface {
	NewStore(log *pennsievelog.Log) *Store
}

type baseStore struct {
	sqlFactory *store.PostgresStoreFactory
	dyDB       *store.DynamoDBStore
	s3         *store.S3Store
	changelog  *restore.SQSChangelogStore
}

func NewBaseStore(sqlFactory *store.PostgresStoreFactory, dyDB *store.DynamoDBStore, s3 *store.S3Store, changelog *restore.SQSChangelogStore) BaseStore {
	return &baseStore{sqlFactory: sqlFactory, dyDB: dyDB, s3: s3, changelog: changelog}
}

func (b *baseStore) NewStore(log *pennsievelog.Log) *Store {
	noSQLStore := b.dyDB.WithLogging(log)
	objectStore := b.s3.WithLogging(log)
	sqlFactory := b.sqlFactory.WithLogging(log)
	changelog := b.changelog.WithLogging(log)
	return &Store{NoSQL: noSQLStore, Object: objectStore, SQLFactory: sqlFactory, Changelog: changelog}
}

type Store struct {
	SQLFactory store.SQLStoreFactory
	Object     store.ObjectStore
	NoSQL      store.NoSQLStore
	Changelog  restore.ChangelogStore
}

func RestorePackagesHandler(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	sqlFactory := store.NewPostgresStoreFactory(PennsieveDB)
	objectStore := store.NewS3Store(S3Client)
	nosqlStore := store.NewDynamoDBStore(DyDBClient, os.Getenv(store.DeleteRecordTableNameEnvKey))
	changelogStore := restore.NewSQSChangelogStore(SQSClient, os.Getenv(restore.JobsQueueIDEnvKey))
	base := NewBaseStore(sqlFactory, nosqlStore, objectStore, changelogStore)
	return handleBatches(ctx, event, base)
}

func handleBatches(ctx context.Context, event events.SQSEvent, base BaseStore) (events.SQSEventResponse, error) {
	response := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}
	for _, r := range event.Records {
		handler := NewMessageHandlerWithContext(ctx, r, base)
		if err := handler.handleBatch(ctx); err != nil {
			handler.LogError(err)
			response.BatchItemFailures = append(response.BatchItemFailures, handler.newBatchItemFailure())
		}
	}
	return response, nil
}

type MessageHandler struct {
	Message events.SQSMessage
	Store   *Store
	*pennsievelog.Log
}

func NewMessageHandler(message events.SQSMessage, base BaseStore) *MessageHandler {
	return NewMessageHandlerWithContext(context.Background(), message, base)
}

// NewMessageHandlerWithContext builds the per-message, request-scoped logger.
//
// Both the per-hop AWS id (the SQS message id) and this service's own trace id
// are attached, under distinct keys: the SQS message id identifies this queue
// hop only, whereas traceId is meant to follow the logical restore operation.
// The Lambda invocation id is added too when the context carries one — note
// that one Lambda invocation covers a whole batch, so it is deliberately not
// used as the per-message correlation id.
func NewMessageHandlerWithContext(ctx context.Context, message events.SQSMessage, base BaseStore) *MessageHandler {
	fields := pennsievelog.Fields{
		pennsievelog.KeySQSMessageID: message.MessageId,
		pennsievelog.KeyTraceID:      traceIDForMessage(message),
	}
	if lc, ok := lambdacontext.FromContext(ctx); ok && lc != nil {
		fields[pennsievelog.KeyAWSRequestID] = lc.AwsRequestID
	}
	plog := pennsievelog.NewLogWithFields(fields)
	storeWithLogger := base.NewStore(plog)
	handler := MessageHandler{
		Message: message,
		Store:   storeWithLogger,
		Log:     plog,
	}
	handler.LogInfoWithFields(pennsievelog.Fields{pennsievelog.KeyBody: message.Body}, "received message")
	return &handler
}

// traceIDForMessage adopts a correlation id supplied by the producer via an SQS
// message attribute, if there is one, so that a restore initiated by the
// service lambda keeps a single id across the queue hop. Otherwise a new one is
// minted here.
func traceIDForMessage(message events.SQSMessage) string {
	for _, name := range []string{"TraceId", "traceId", "X-Request-Id"} {
		if attr, ok := message.MessageAttributes[name]; ok && attr.StringValue != nil && *attr.StringValue != "" {
			return *attr.StringValue
		}
	}
	return uuid.NewString()
}

func (h *MessageHandler) handleBatch(ctx context.Context) error {
	restoreMessage := models.RestorePackageMessage{}
	if err := json.Unmarshal([]byte(h.Message.Body), &restoreMessage); err != nil {
		return h.errorf("could not unmarshal message [%s]: %w", h.Message.Body, err)
	}
	if err := h.handleMessage(ctx, restoreMessage); err != nil {
		return h.errorf("error handling message [%v]: %w", restoreMessage, err)
	}
	return nil
}

func (h *MessageHandler) handleMessage(ctx context.Context, message models.RestorePackageMessage) error {
	var changelog []changelog2.PackageRestoreEvent
	var err error
	p := message.Package
	if p.Type == packageType.Collection {
		changelog, err = h.handleFolderPackage(ctx, message.OrgId, message.DatasetId, p)
	} else {
		changelog, err = h.handleFilePackage(ctx, message.OrgId, message.DatasetId, p)
	}
	if err != nil {
		return h.errorf("could not restore folder %s in org %d: %w", p.NodeId, message.OrgId, err)
	}
	if err := h.Store.Changelog.LogRestores(ctx, int64(message.OrgId), message.DatasetId, message.UserId, changelog); err != nil {
		h.LogWarnWithFields(pennsievelog.Fields{pennsievelog.KeyError: err}, "unable to send changelog events")
	}

	return nil
}

func (h *MessageHandler) newBatchItemFailure() events.SQSBatchItemFailure {
	return events.SQSBatchItemFailure{ItemIdentifier: h.Message.MessageId}
}

func (h *MessageHandler) errorf(format string, args ...any) error {
	expanded := make([]any, len(args)+1)
	expanded[0] = m
	copy(expanded[1:], args)
	return fmt.Errorf("%s: "+format, expanded...)
}
