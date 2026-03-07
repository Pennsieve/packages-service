package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/packages-service/service/handler"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	// Create connection pool to Postgres DB
	db, err := pgdb.ConnectRDS()
	if err != nil {
		panic(fmt.Sprintf("unable to open connection pool to RDS database: %s", err))
	}
	if err := db.Ping(); err != nil {
		panic(fmt.Sprintf("unable to connect to RDS database: %s", err))
	}
	log.Info("connected to RDS database")
	handler.PennsieveDB = db

	// Create AWS config
	region := os.Getenv("REGION")
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("AWS configuration error: %v\n", err)
	}

	handler.SQSClient = sqs.NewFromConfig(cfg)
	handler.S3Client = s3.NewFromConfig(cfg)
}

func main() {
	lambda.Start(handler.PackagesServiceHandler)
}
