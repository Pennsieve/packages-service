package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	log "github.com/sirupsen/logrus"
)

const m = "restore/handler"

var PennsieveDB *sql.DB

func RestorePackagesHandler(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	sqlFactory := store.NewSQLStoreFactory(PennsieveDB)
	return handleBatch(ctx, event, sqlFactory)
}

func handleBatch(ctx context.Context, event events.SQSEvent, sqlFactory store.SQLStoreFactory) (events.SQSEventResponse, error) {
	response := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}
	for _, r := range event.Records {
		handler := NewMessageHandler(r, sqlFactory)
		if err := handler.handle(ctx); err != nil {
			handler.logError(err)
			response.BatchItemFailures = append(response.BatchItemFailures, handler.newBatchItemFailure())
		}
	}
	return response, nil
}

type MessageHandler struct {
	Message    events.SQSMessage
	Logger     *log.Entry
	SQLFactory store.SQLStoreFactory
}

func NewMessageHandler(message events.SQSMessage, sqlFactory store.SQLStoreFactory) *MessageHandler {
	handler := MessageHandler{Message: message, SQLFactory: sqlFactory}
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
		return h.errorf("could not unmarshal message: %w", err)
	}
	if err := h.handleMessage(ctx, restoreMessage); err != nil {
		return h.errorf("error handling message: %w", err)
	}
	return nil
}

func (h *MessageHandler) handleMessage(ctx context.Context, message models.RestorePackageMessage) error {
	for _, p := range message.NodeIds {
		if err := h.handlePackage(ctx, message.OrgId, p); err != nil {
			return h.errorf("could not restore package %s in org %d: %w", p, message.OrgId, err)
		}
	}
	return nil
}

func (h *MessageHandler) handlePackage(ctx context.Context, orgId int, nodeId string) error {
	err := h.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
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
