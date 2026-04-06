package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	_ "github.com/lib/pq"
	"github.com/pennsieve/packages-service/service/handler"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
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
	handler.AssumeRoleClient = sts.NewFromConfig(cfg)

	// Connect to discover_postgres database
	discoverDB, err := connectRDSDiscover()
	if err != nil {
		log.Warnf("unable to connect to discover database: %s (discover endpoints will be unavailable)", err)
	} else {
		handler.DiscoverDB = discoverDB
		log.Info("connected to discover database")
	}
}

// connectRDSDiscover connects to the discover_postgres database using IAM auth.
func connectRDSDiscover() (*sql.DB, error) {
	env := os.Getenv("ENV")
	if env == "DOCKER" {
		// For local testing, use environment variables
		host := os.Getenv("POSTGRES_HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv("POSTGRES_PORT")
		if port == "" {
			port = "5432"
		}
		user := os.Getenv("POSTGRES_USER")
		if user == "" {
			user = "postgres"
		}
		password := os.Getenv("POSTGRES_PASSWORD")
		if password == "" {
			password = "password"
		}
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=discover_postgres sslmode=disable",
			host, port, user, password)
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, fmt.Errorf("error opening discover DB connection: %w", err)
		}
		return db, db.Ping()
	}

	dbHost := os.Getenv("RDS_PROXY_ENDPOINT")
	dbPort := 5432
	dbUser := fmt.Sprintf("%s_rds_proxy_user", env)
	dbEndpoint := fmt.Sprintf("%s:%d", dbHost, dbPort)
	region := os.Getenv("REGION")

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error loading AWS config: %w", err)
	}

	authToken, err := auth.BuildAuthToken(
		context.Background(), dbEndpoint, region, dbUser, cfg.Credentials)
	if err != nil {
		return nil, fmt.Errorf("error building RDS auth token for discover: %w", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=discover_postgres",
		dbHost, strconv.Itoa(dbPort), dbUser, authToken)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening discover DB connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging discover DB: %w", err)
	}

	return db, nil
}

func main() {
	lambda.Start(handler.PackagesServiceHandler)
}
