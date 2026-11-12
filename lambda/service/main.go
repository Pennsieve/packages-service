package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	_ "github.com/lib/pq"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/service/handler"
	"github.com/pennsieve/packages-service/service/internal/rdsiam"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
)

const dockerEnv = "DOCKER"

func init() {
	logging.SetDefaultFromEnv()

	// Create AWS config
	region := os.Getenv("REGION")
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		fatal("AWS configuration error", err)
	}

	// Create connection pool to Postgres DB
	db, err := connectRDSPennsieve(cfg)
	if err != nil {
		fatal("unable to connect to RDS database", err)
	}
	slog.Info("connected to RDS database")
	handler.PennsieveDB = db

	handler.SQSClient = sqs.NewFromConfig(cfg)
	handler.S3Client = s3.NewFromConfig(cfg)
	handler.AssumeRoleClient = sts.NewFromConfig(cfg)

	// Connect to discover_postgres database
	discoverDB, err := connectRDSDiscover(cfg)
	if err != nil {
		slog.Warn("unable to connect to discover database; discover endpoints will be unavailable",
			slog.Any(logging.KeyError, err))
	} else {
		handler.DiscoverDB = discoverDB
		slog.Info("connected to discover database")
	}
}

// fatal reports an unrecoverable cold-start failure and exits.
//
// One consistent mechanism across all of this service's lambdas: a structured
// slog.Error (so the failure is queryable like every other log line) followed by
// os.Exit(1). Previously these sites were a mix of bare panic(fmt.Sprintf(...))
// — which emits an unstructured stack trace — and stdlib log.Fatalf, which wrote
// to a logger that was never JSON-configured.
func fatal(msg string, err error) {
	slog.Error(msg, slog.Any(logging.KeyError, err))
	os.Exit(1)
}

// rdsProxyConnector returns a connector for dbName through the RDS Proxy that
// builds a fresh IAM auth token for every new connection. A token built once at
// cold start would expire after 15 minutes, and every connection the pool opens
// after that (e.g. once the proxy closes an idle one) would fail.
func rdsProxyConnector(cfg aws.Config, dbName string) *rdsiam.Connector {
	return rdsiam.NewConnector(
		os.Getenv("RDS_PROXY_ENDPOINT"),
		5432,
		fmt.Sprintf("%s_rds_proxy_user", os.Getenv("ENV")),
		dbName,
		os.Getenv("REGION"),
		cfg.Credentials,
	)
}

// connectRDSPennsieve connects to the pennsieve_postgres database using IAM auth.
func connectRDSPennsieve(cfg aws.Config) (*sql.DB, error) {
	if env := os.Getenv("ENV"); env == "" || env == dockerEnv {
		// pgdb uses the POSTGRES_* environment variables in Docker (and when ENV is unset)
		db, err := pgdb.ConnectRDS()
		if err != nil {
			return nil, err
		}
		return db, db.Ping()
	}
	return rdsiam.Open(context.Background(), rdsProxyConnector(cfg, "pennsieve_postgres"))
}

// connectRDSDiscover connects to the discover_postgres database using IAM auth.
func connectRDSDiscover(cfg aws.Config) (*sql.DB, error) {
	env := os.Getenv("ENV")
	if env == dockerEnv {
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
	return rdsiam.Open(context.Background(), rdsProxyConnector(cfg, "discover_postgres"))
}

func main() {
	lambda.Start(handler.PackagesServiceHandler)
}
