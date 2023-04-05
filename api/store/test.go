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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

type Fixture struct {
	T *testing.T
}

type S3Fixture struct {
	Fixture
	Client *s3.Client
	// Buckets is a set of bucket names
	Buckets map[string]bool
}

func NewS3Fixture(t *testing.T, client *s3.Client, inputs ...*s3.CreateBucketInput) *S3Fixture {
	f := S3Fixture{
		Fixture: Fixture{T: t},
		Client:  client,
		Buckets: map[string]bool{},
	}
	ctx := context.Background()
	for _, input := range inputs {
		bucketName := aws.ToString(input.Bucket)
		if _, err := f.Client.CreateBucket(ctx, input); err != nil {
			assert.FailNow(f.T, "error creating test bucket", "bucket: %s, error: %v", bucketName, err)
		}
		f.Buckets[bucketName] = true
	}
	return &f
}

func (f *S3Fixture) WithObjects(objectInputs ...*s3.PutObjectInput) *S3Fixture {
	ctx := context.Background()
	for _, input := range objectInputs {
		if _, err := f.Client.PutObject(ctx, input); err != nil {
			assert.FailNow(f.T, "error putting test object", "bucket: %s, key: %s, error: %v", aws.ToString(input.Bucket), aws.ToString(input.Key), err)
		}
	}
	return f
}

func (f *S3Fixture) Teardown() {
	ctx := context.Background()
	for name := range f.Buckets {
		listInput := s3.ListObjectVersionsInput{Bucket: aws.String(name)}
		listOutput, err := f.Client.ListObjectVersions(ctx, &listInput)
		if err != nil {
			assert.FailNow(f.T, "error listing test objects", "bucket: %s, error: %v", name, err)
		}
		if listOutput.IsTruncated {
			assert.FailNow(f.T, "test object list is truncated; handling truncated object list is not yet implemented", "bucket: %s, error: %v", name, err)
		}
		if len(listOutput.DeleteMarkers)+len(listOutput.Versions) > 0 {
			objectIds := make([]types.ObjectIdentifier, len(listOutput.DeleteMarkers)+len(listOutput.Versions))
			i := 0
			for _, dm := range listOutput.DeleteMarkers {
				objectIds[i] = types.ObjectIdentifier{Key: dm.Key, VersionId: dm.VersionId}
				i++
			}
			for _, obj := range listOutput.Versions {
				objectIds[i] = types.ObjectIdentifier{Key: obj.Key, VersionId: obj.VersionId}
				i++
			}
			deleteObjectsInput := s3.DeleteObjectsInput{Bucket: aws.String(name), Delete: &types.Delete{Objects: objectIds}}
			if deleteObjectsOutput, err := f.Client.DeleteObjects(ctx, &deleteObjectsInput); err != nil {
				assert.FailNow(f.T, "error deleting test objects", "bucket: %s, error: %v", name, err)
			} else if len(deleteObjectsOutput.Errors) > 0 {
				// Convert to AWSErrors so that all the pointers AWS uses become de-referenced and readable in the output
				errs := make([]AWSError, len(deleteObjectsOutput.Errors))
				for i, err := range deleteObjectsOutput.Errors {
					errs[i] = NewAWSError(name, err)
				}
				assert.FailNow(f.T, "errors deleting test objects", "bucket: %s, errors: %v", name, errs)
			}
		}
		deleteBucketInput := s3.DeleteBucketInput{Bucket: aws.String(name)}
		if _, err := f.Client.DeleteBucket(ctx, &deleteBucketInput); err != nil {
			assert.FailNow(f.T, "error deleting test bucket", "bucket: %s, error: %v", name, err)
		}
	}
}

type DynamoDBFixture struct {
	Fixture
	Client *dynamodb.Client
	// Tables is a set of table names
	Tables map[string]bool
}

func NewDynamoDBFixture(t *testing.T, client *dynamodb.Client, inputs ...*dynamodb.CreateTableInput) *DynamoDBFixture {
	f := DynamoDBFixture{
		Fixture: Fixture{T: t},
		Client:  client,
		Tables:  map[string]bool{},
	}
	ctx := context.Background()
	for _, input := range inputs {
		tableName := aws.ToString(input.TableName)
		if _, err := f.Client.CreateTable(ctx, input); err != nil {
			assert.FailNow(f.T, "error creating test table", "table: %s, error: %v", tableName, err)
		}
		f.Tables[tableName] = true
	}
	return &f
}

func (f *DynamoDBFixture) WithItems(inputs ...*dynamodb.PutItemInput) *DynamoDBFixture {
	ctx := context.Background()
	for _, input := range inputs {
		if _, err := f.Client.PutItem(ctx, input); err != nil {
			assert.FailNow(f.T, "error adding item test table", "table: %s, item: %v, error: %v", aws.ToString(input.TableName), input.Item, err)
		}
	}
	return f
}

func (f *DynamoDBFixture) Teardown() {
	ctx := context.Background()
	for name := range f.Tables {
		input := dynamodb.DeleteTableInput{TableName: aws.String(name)}
		if _, err := f.Client.DeleteTable(ctx, &input); err != nil {
			assert.FailNow(f.T, "error deleting test table", "table: %s, error: %v", name, err)

		}
	}
}
