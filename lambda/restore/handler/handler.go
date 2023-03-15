package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	log "github.com/sirupsen/logrus"
)

const m = "restore/handler"

var PennsieveDB *sql.DB
var S3Client *s3.Client
var DyDBClient *dynamodb.Client

func RestorePackagesHandler(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	sqlFactory := store.NewSQLStoreFactory(PennsieveDB)
	objectStore := store.NewObjectStore(S3Client)
	nosqlStore := store.NewNoSQLStore(DyDBClient)
	str := Store{SQLFactory: sqlFactory, Object: objectStore, NoSQL: nosqlStore}
	return handleBatch(ctx, event, str)
}

func handleBatch(ctx context.Context, event events.SQSEvent, store Store) (events.SQSEventResponse, error) {
	response := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}
	for _, r := range event.Records {
		handler := NewMessageHandler(r, store)
		if err := handler.handle(ctx); err != nil {
			handler.logError(err)
			response.BatchItemFailures = append(response.BatchItemFailures, handler.newBatchItemFailure())
		}
	}
	return response, nil
}

type Store struct {
	SQLFactory store.SQLStoreFactory
	Object     store.ObjectStore
	NoSQL      store.NoSQLStore
}

type MessageHandler struct {
	Message events.SQSMessage
	Logger  *log.Entry
	Store   Store
}

func NewMessageHandler(message events.SQSMessage, store Store) *MessageHandler {
	handler := MessageHandler{Message: message, Store: store}
	logger := log.WithFields(log.Fields{
		"messageId": handler.Message.MessageId,
	})
	logger.WithFields(log.Fields{"body": handler.Message.Body}).Info("received message")
	handler.Logger = logger
	return &handler
}

func (h *MessageHandler) handle(ctx context.Context) error {
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
	for _, p := range message.Packages {
		if p.IsCollection {
			if err := h.handleFolder(ctx, message.OrgId, message.DatasetId, p.NodeId); err != nil {
				return h.errorf("could not restore folder %s in org %d: %w", p.NodeId, message.OrgId, err)
			}
		} else {
			if err := h.handlePackage(ctx, message.OrgId, message.DatasetId, p.NodeId); err != nil {
				return h.errorf("could not restore package %s in org %d: %w", p.NodeId, message.OrgId, err)
			}
		}
	}
	return nil
}

func (h *MessageHandler) handlePackage(ctx context.Context, orgId int, datasetId int64, nodeId string) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		return nil
	})
	return err
}

func (h *MessageHandler) handleFolder(ctx context.Context, orgId int, datasetId int64, nodeId string) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		return nil
	})
	return err
}

func (h *MessageHandler) logError(args ...any) {
	h.Logger.Error(args...)
}

func (h *MessageHandler) logErrorWithFields(fields log.Fields, args ...any) {
	h.Logger.WithFields(fields).Error(args...)
}

func (h *MessageHandler) logInfo(args ...any) {
	h.Logger.Info(args...)
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
