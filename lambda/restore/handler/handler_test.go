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
	assert := assert.New(t)
	expectedMessageId := "new-message-handler-message-id"
	message := events.SQSMessage{
		MessageId: expectedMessageId,
	}
	sqlFactory := new(MockSQLFactory)
	handler := NewMessageHandler(message, StubBaseStore{SQLStoreFactory: sqlFactory})

	assert.Equal(message, handler.Message)
	assert.Equal(sqlFactory, handler.Store.SQLFactory)
	assert.NotNil(handler.Logger)

	assert.Equal(expectedMessageId, handler.newBatchItemFailure().ItemIdentifier)

	wrappedError := errors.New("inner error")
	err := handler.errorf("had a problem with %d: %w", 4, wrappedError)
	assert.True(errors.Is(err, wrappedError))
	assert.Equal(wrappedError, errors.Unwrap(err))
	assert.True(strings.HasPrefix(err.Error(), m))
	assert.Contains(err.Error(), "had a problem with 4")
	assert.Contains(err.Error(), wrappedError.Error())

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
	createBucketInput := s3.CreateBucketInput{Bucket: aws.String(bucketName), ObjectLockEnabledForBucket: true}
	if _, err := s3Client.CreateBucket(context.Background(), &createBucketInput); err != nil {
		assert.FailNow(t, "error creating test bucket", err)
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
	if _, err := dyClient.CreateTable(context.Background(), &createTableInput); err != nil {
		assert.FailNow(t, "error creating test table", err)
	}
}
