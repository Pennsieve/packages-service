package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/models"
	log "github.com/sirupsen/logrus"
)

var PennsieveDB *sql.DB

func RestorePackagesHandler(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	response := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}
	for _, r := range event.Records {
		logger := log.WithFields(log.Fields{
			"messageId": r.MessageId,
		})
		logger.WithFields(log.Fields{"body": r.Body}).Info("received message")
		restoreMessage := models.RestorePackageMessage{}
		err := json.Unmarshal([]byte(r.Body), &restoreMessage)
		if err != nil {
			response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
			logger.Errorf("could not unmarshal message: %v", err)
		}
	}
	return response, nil
}
