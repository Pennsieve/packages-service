package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/restore/handler"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	uuid.EnableRandPool()
	log.SetFormatter(&log.JSONFormatter{})
	if level, ok := os.LookupEnv("LOG_LEVEL"); !ok {
		log.SetLevel(log.InfoLevel)
	} else {
		if ll, err := log.ParseLevel(level); err == nil {
			log.SetLevel(ll)
		} else {
			log.SetLevel(log.InfoLevel)
			log.Warnf("could not set log level to %q: %v", level, err)
		}
	}

	// Open DB connection pool here so that it can be reused if lambda handles more than one request
	db, err := pgdb.ConnectRDS()
	if err != nil {
		panic(fmt.Sprintf("unable open connection pool to RDS database: %s", err))
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

	handler.S3Client = s3.NewFromConfig(cfg)
	handler.DyDBClient = dynamodb.NewFromConfig(cfg)
	handler.SQSClient = sqs.NewFromConfig(cfg)
}

func main() {
	lambda.Start(handler.RestorePackagesHandler)
}
