package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	pennsievelog "github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/packages-service/api/store/restore"
	changelog2 "github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	log "github.com/sirupsen/logrus"
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
	nosqlStore := store.NewDynamoDBStore(DyDBClient)
	changelogStore := restore.NewSQSChangelogStore(SQSClient)
	base := NewBaseStore(sqlFactory, nosqlStore, objectStore, changelogStore)
	return handleBatches(ctx, event, base)
}

func handleBatches(ctx context.Context, event events.SQSEvent, base BaseStore) (events.SQSEventResponse, error) {
	response := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}
	for _, r := range event.Records {
		handler := NewMessageHandler(r, base)
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
	plog := pennsievelog.NewLogWithFields(log.Fields{
		"messageId": message.MessageId,
	})
	storeWithLogger := base.NewStore(plog)
	handler := MessageHandler{
		Message: message,
		Store:   storeWithLogger,
		Log:     plog,
	}
	handler.LogInfoWithFields(log.Fields{"body": message.Body}, "received message")
	return &handler
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
		h.LogWarnWithFields(log.Fields{"error": err}, "unable to send changelog events")
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
