package main

import (
	"context"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/restore/handler"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"log/slog"
	"os"
)

func init() {
	uuid.EnableRandPool()
	logging.SetDefaultFromEnv()

	// Open DB connection pool here so that it can be reused if lambda handles more than one request
	db, err := pgdb.ConnectRDS()
	if err != nil {
		fatal("unable to open connection pool to RDS database", err)
	}
	if err := db.Ping(); err != nil {
		fatal("unable to connect to RDS database", err)
	}
	slog.Info("connected to RDS database")
	handler.PennsieveDB = db

	// Create AWS config
	region := os.Getenv("REGION")
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		fatal("AWS configuration error", err)
	}

	handler.S3Client = s3.NewFromConfig(cfg)
	handler.DyDBClient = dynamodb.NewFromConfig(cfg)
	handler.SQSClient = sqs.NewFromConfig(cfg)
}

// fatal reports an unrecoverable cold-start failure and exits. See the same
// helper in the service and asset-cleanup lambdas: one consistent mechanism
// (structured slog.Error + os.Exit(1)) rather than a mix of panic and
// log.Fatalf.
func fatal(msg string, err error) {
	slog.Error(msg, slog.Any(logging.KeyError, err))
	os.Exit(1)
}

func main() {
	lambda.Start(handler.RestorePackagesHandler)
}
