package handler

import (
	"context"
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"os"
	"strings"
	"testing"
)

func TestNewMessageHandler(t *testing.T) {
	a := assert.New(t)
	expectedMessageId := "new-message-handler-message-id"
	message := events.SQSMessage{
		MessageId: expectedMessageId,
	}
	sqlFactory := new(MockSQLFactory)
	handler := NewMessageHandler(message, StubBaseStore{SQLStoreFactory: sqlFactory})

	a.Equal(message, handler.Message)
	a.Equal(sqlFactory, handler.Store.SQLFactory)
	a.NotNil(handler.Logger)

	a.Equal(expectedMessageId, handler.newBatchItemFailure().ItemIdentifier)

	wrappedError := errors.New("inner error")
	err := handler.errorf("had a problem with %d: %w", 4, wrappedError)
	a.True(errors.Is(err, wrappedError))
	a.Equal(wrappedError, errors.Unwrap(err))
	a.True(strings.HasPrefix(err.Error(), m))
	a.Contains(err.Error(), "had a problem with 4")
	a.Contains(err.Error(), wrappedError.Error())

}

type MockSQLFactory struct {
	mock.Mock
}

func (m *MockSQLFactory) NewSimpleStore(orgId int) store.SQLStore {
	args := m.Called(orgId)
	return args.Get(0).(store.SQLStore)
}

func (m *MockSQLFactory) ExecStoreTx(ctx context.Context, orgId int, fn func(store store.SQLStore) error) error {
	args := m.Called(ctx, orgId, fn)
	return args.Error(0)
}

type StubBaseStore struct {
	SQLStoreFactory store.SQLStoreFactory
}

func (s StubBaseStore) NewStore(_ *logging.Log) *Store {
	return &Store{SQLFactory: s.SQLStoreFactory}
}

func TestHandleMessage(t *testing.T) {

	awsConfig := store.GetTestAWSConfig(t)

	s3Client := s3.NewFromConfig(awsConfig)
	dyClient := dynamodb.NewFromConfig(awsConfig)

	bucketName := "test-bucket"
	key := "test-folder/test-object"
	s3Fixture := store.NewS3Fixture(
		t,
		s3Client,
		&s3.CreateBucketInput{Bucket: aws.String(bucketName), ObjectLockEnabledForBucket: true}).WithObjects(
		&s3.PutObjectInput{Bucket: aws.String(bucketName), Key: aws.String(key), Body: strings.NewReader("object content")})
	defer s3Fixture.Teardown()

	ctx := context.Background()
	if _, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(key)}); err != nil {
		assert.FailNow(t, "error setting up deleted object", err)
	}

	tableName := os.Getenv(store.DeleteRecordTableNameEnvKey)
	createTableInput := dynamodb.CreateTableInput{TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("NodeId"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("NodeId"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest}
	if _, err := dyClient.CreateTable(ctx, &createTableInput); err != nil {
		assert.FailNow(t, "error creating test table", err)
	}
}
