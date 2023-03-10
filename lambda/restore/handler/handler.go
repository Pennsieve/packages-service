package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
)

var PennsieveDB *sql.DB

func RestorePackagesHandler(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	for _, r := range event.Records {
		fmt.Println(r.Body)
	}
	return events.SQSEventResponse{}, nil
}
