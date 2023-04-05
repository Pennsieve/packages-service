package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type TestDB struct {
	*sql.DB
	t *testing.T
}

// PingUntilReady pings the db up to 10 times, stopping when
// a ping is successful. Used because there have been problems on Jenkins with
// the test DB not being fully started and ready to make connections.
// But there must be a better way.
func (tdb *TestDB) PingUntilReady() error {
	var err error
	wait := 100 * time.Millisecond
	for i := 0; i < 10; i++ {
		if err = tdb.Ping(); err == nil {
			return nil
		}
		time.Sleep(wait)
		wait = 2 * wait

	}
	return err
}

func OpenDB(t *testing.T, additionalOptions ...PostgresOption) TestDB {
	pgConfig := PostgresConfigFromEnv()
	db, err := pgConfig.Open(additionalOptions...)
	if err != nil {
		assert.FailNowf(t, "cannot open database", "config: %s, err: %v", pgConfig, err)
	}
	testDB := TestDB{
		DB: db,
		t:  t,
	}
	if err = testDB.PingUntilReady(); err != nil {
		assert.FailNow(testDB.t, "cannot ping database", err)
	}
	return testDB
}

func (tdb *TestDB) ExecSQLFile(sqlFile string) {
	path := filepath.Join("testdata", sqlFile)
	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		assert.FailNowf(tdb.t, "error reading SQL file", "%s: %v", path, err)
	}
	sqlStr := string(sqlBytes)
	_, err = tdb.Exec(sqlStr)
	if err != nil {
		assert.FailNowf(tdb.t, "error executing SQL file", "%s: %v", path, err)
	}
}

func (tdb *TestDB) Truncate(orgID int, table string) {
	query := fmt.Sprintf(`TRUNCATE TABLE "%d".%s CASCADE`, orgID, table)
	_, err := tdb.Exec(query)
	if err != nil {
		assert.FailNowf(tdb.t, "error truncating table", "orgID: %d, table: %s, error: %v", orgID, table, err)
	}
}

func (tdb *TestDB) TruncatePennsieve(table string) {
	query := fmt.Sprintf(`TRUNCATE TABLE pennsieve.%s CASCADE`, table)
	_, err := tdb.Exec(query)
	if err != nil {
		assert.FailNowf(tdb.t, "error truncating table in pennsieve schema", "table: %s, error: %v", table, err)
	}
}

func (tdb *TestDB) Close() {
	if err := tdb.DB.Close(); err != nil {
		assert.FailNowf(tdb.t, "error closing database", "error: %v", err)
	}
}

func (tdb *TestDB) CloseRows(rows *sql.Rows) {
	if err := rows.Close(); err != nil {
		assert.FailNowf(tdb.t, "error cloting rows", "error: %v", err)
	}
}

type NoLogger struct{}

func (n NoLogger) LogWarn(_ ...any) {}

func (n NoLogger) LogWarnWithFields(_ log.Fields, _ ...any) {}

func (n NoLogger) LogDebug(_ ...any) {}

func (n NoLogger) LogDebugWithFields(_ log.Fields, _ ...any) {}

func (n NoLogger) LogError(_ ...any) {}

func (n NoLogger) LogErrorWithFields(_ log.Fields, _ ...any) {}

func (n NoLogger) LogInfo(_ ...any) {}

func (n NoLogger) LogInfoWithFields(_ log.Fields, _ ...any) {}

func GetTestAWSConfig(t *testing.T) aws.Config {
	awsKey := "awstestkey"
	awsSecret := "awstestsecret"
	minioURL := os.Getenv("MINIO_URL")
	dynamodbURL := os.Getenv("DYNAMODB_URL")
	awsConfig, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsKey, awsSecret, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{URL: minioURL, HostnameImmutable: true}, nil
			} else if service == dynamodb.ServiceID {
				return aws.Endpoint{URL: dynamodbURL}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown test endpoint requested for service: %s", service)
		})))
	if err != nil {
		assert.FailNow(t, "error creating AWS config", err)
	}
	return awsConfig
}
