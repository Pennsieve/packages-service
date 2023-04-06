package handler

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"os"
	"strconv"
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
	ctx := context.Background()

	orgId := 2
	awsConfig := store.GetTestAWSConfig(t)

	s3Client := s3.NewFromConfig(awsConfig)
	dyClient := dynamodb.NewFromConfig(awsConfig)

	db := store.OpenDB(t)
	defer db.Close()

	defer db.Truncate(orgId, "packages")
	defer db.Truncate(orgId, "package_storage")
	defer db.Truncate(orgId, "dataset_storage")
	defer db.TruncatePennsieve("organization_storage")

	rootCollectionPkg := store.NewTestPackage(1, 1, 1).
		WithType(packageType.Collection).
		WithState(packageState.Ready).
		Insert(ctx, db, orgId)
	restoringCollection := store.NewTestPackage(2, 1, 1).
		WithParentId(rootCollectionPkg.Id).
		WithType(packageType.Collection).
		Restoring().
		Insert(ctx, db, orgId)

	// Insert the packages inside restoringCollection and prepare the S3 put requests.
	bucketName := "test-bucket"
	putObjectInputByNodeId := map[string]*s3.PutObjectInput{}
	for i := int64(3); i < 53; i++ {
		pkg := store.NewTestPackage(i, 1, 1).
			WithParentId(restoringCollection.Id).
			Deleted().
			WithType(packageType.CSV).
			Insert(ctx, db, orgId)
		if pkg.PackageType != packageType.Collection {
			s3Key := fmt.Sprintf("%s/%s", uuid.NewString(), uuid.NewString())
			putObjectInputByNodeId[pkg.NodeId] = &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(s3Key),
				Body:   strings.NewReader(fmt.Sprintf("object %d content", i))}
		}
	}

	// Create the S3 fixture with the bucket and put requests
	putObjectInputs := make([]*s3.PutObjectInput, 0, len(putObjectInputByNodeId))
	for _, v := range putObjectInputByNodeId {
		putObjectInputs = append(putObjectInputs, v)
	}
	s3Fixture := store.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{Bucket: aws.String(bucketName), ObjectLockEnabledForBucket: true}).WithObjects(putObjectInputs...)
	defer s3Fixture.Teardown()

	// Delete the S3 objects and prepare put requests for the delete-records in Dynamo
	deleteRecordTableName := os.Getenv(store.DeleteRecordTableNameEnvKey)
	var deleteRecordInputs []*dynamodb.PutItemInput
	for nodeId, putObject := range putObjectInputByNodeId {
		// Delete the object from S3
		deleteObjectOutput, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: putObject.Key})
		if err != nil {
			assert.FailNow(t, "error setting up deleted object", err)
		}

		// Put a delete-record in DynamoDB for object
		deleteRecord := store.S3ObjectInfo{Bucket: bucketName, Key: aws.ToString(putObject.Key), NodeId: nodeId, Size: strconv.Itoa(rand.Intn(1000000)), VersionId: aws.ToString(deleteObjectOutput.VersionId)}
		deleteRecordItem, err := attributevalue.MarshalMap(deleteRecord)
		if err != nil {
			assert.FailNow(t, "error setting up item in delete record table", err)
		}
		deleteRecordInputs = append(deleteRecordInputs, &dynamodb.PutItemInput{TableName: aws.String(deleteRecordTableName), Item: deleteRecordItem})
	}

	createTableInput := dynamodb.CreateTableInput{TableName: aws.String(deleteRecordTableName),
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

	dyFixture := store.NewDynamoDBFixture(t, dyClient, &createTableInput).WithItems(deleteRecordInputs...)
	defer dyFixture.Teardown()

	sqlFactory := store.NewPostgresStoreFactory(db.DB)
	objectStore := store.NewS3Store(s3Client)
	nosqlStore := store.NewDynamoDBStore(dyClient)
	base := NewBaseStore(sqlFactory, nosqlStore, objectStore)
	expectedMessageId := "handle-message-message-id"
	message := events.SQSMessage{
		MessageId: expectedMessageId,
	}
	handler := NewMessageHandler(message, base)
	err := handler.handleMessage(ctx, models.RestorePackageMessage{
		OrgId:     orgId,
		DatasetId: 1,
		Package: models.RestorePackageInfo{
			Id:       restoringCollection.Id,
			NodeId:   restoringCollection.NodeId,
			Name:     restoringCollection.Name,
			ParentId: &restoringCollection.ParentId.Int64,
			Type:     restoringCollection.PackageType},
	})
	if assert.NoError(t, err) {
		verifyQuery := fmt.Sprintf(`SELECT id, name, node_id, state from "%d".packages`, orgId)
		rows, err := db.QueryContext(ctx, verifyQuery)
		if assert.NoError(t, err) {
			defer db.CloseRows(rows)
			for rows.Next() {
				var id int64
				var name, nodeId string
				var state packageState.State
				err = rows.Scan(&id, &name, &nodeId, &state)
				if assert.NoError(t, err) {
					if id == rootCollectionPkg.Id {
						assert.Equal(t, rootCollectionPkg.Name, name)
						assert.Equal(t, rootCollectionPkg.PackageState, state)
					} else {
						assert.NotEqual(t, packageState.Restoring, state)
						assert.NotEqual(t, packageState.Deleted, state)
						assert.NotContains(t, name, fmt.Sprintf("__%s__%s", packageState.Deleted, nodeId))
					}
				}
			}
			assert.NoError(t, rows.Err())
		}

	}

}
