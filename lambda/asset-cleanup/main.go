package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/packages-service/asset-cleanup/handler"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
)

func init() {
	handler.SetDefaultLoggerFromEnv()

	db, err := pgdb.ConnectRDS()
	if err != nil {
		fatal("unable to open connection pool to RDS database", err)
	}
	if err := db.Ping(); err != nil {
		fatal("unable to connect to RDS database", err)
	}
	slog.Info("connected to RDS database")
	handler.PennsieveDB = db

	region := os.Getenv("REGION")
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		fatal("AWS configuration error", err)
	}

	handler.S3Client = s3.NewFromConfig(cfg)
}

// fatal reports an unrecoverable cold-start failure and exits. See the same
// helper in the service and restore lambdas: one consistent mechanism
// (structured slog.Error + os.Exit(1)) rather than a mix of panic and
// log.Fatalf.
func fatal(msg string, err error) {
	slog.Error(msg, slog.String("error", err.Error()))
	os.Exit(1)
}

func main() {
	lambda.Start(handler.HandleCleanup)
}
