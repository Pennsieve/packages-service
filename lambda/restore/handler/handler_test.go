package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/packages-service/api/store/restore"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net/http"
	"net/http/httptest"
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
	datasetId := 1

	db := store.OpenDB(t)
	defer db.Close()

	defer db.Truncate(orgId, "packages")
	defer db.Truncate(orgId, "package_storage")
	defer db.Truncate(orgId, "dataset_storage")
	defer db.TruncatePennsieve("organization_storage")

	rootCollectionPkg := store.NewTestPackage(1, datasetId, 1).
		WithType(packageType.Collection).
		WithState(packageState.Ready).
		Insert(ctx, db, orgId)
	restoringCollection := store.NewTestPackage(2, datasetId, 1).
		WithParentId(rootCollectionPkg.Id).
		WithType(packageType.Collection).
		Restoring().
		Insert(ctx, db, orgId)
	untouchedPackages := []*pgdb.Package{rootCollectionPkg}
	var restoringFilePackages []*pgdb.Package
	restoringCollectionPackages := []*pgdb.Package{restoringCollection}
	// Insert the packages inside restoringCollection and prepare the S3 put requests.
	bucketName := "test-bucket"
	putObjectInputByNodeId := map[string]*s3.PutObjectInput{}
	for i := int64(3); i < 53; i++ {
		pkg := store.NewTestPackage(i, 1, 1).
			WithParentId(restoringCollection.Id).
			Deleted().
			Insert(ctx, db, orgId)
		if pkg.PackageType != packageType.Collection {
			s3Key := fmt.Sprintf("%s/%s", uuid.NewString(), uuid.NewString())
			putObjectInputByNodeId[pkg.NodeId] = &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(s3Key),
				Body:   strings.NewReader(fmt.Sprintf("object %d content", i))}
			restoringFilePackages = append(restoringFilePackages, pkg)

		} else {
			restoringCollectionPackages = append(restoringCollectionPackages, pkg)
		}
	}

	jobsQueueID := "TestJobsQueueUrl"

	mockSQSServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		var sqsMessage struct {
			MessageBody string
			QueueUrl    string
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sqsMessage))
		assert.Equal(t, jobsQueueID, sqsMessage.QueueUrl)

		// cannot unmarshall message as changelog.Message because we didn't define Unmarshall for changelog.Type
		assert.Contains(t, sqsMessage.MessageBody, fmt.Sprintf(`"organizationId":%d`, orgId))
		assert.Contains(t, sqsMessage.MessageBody, fmt.Sprintf(`"datasetId":%d`, datasetId))
		for _, p := range untouchedPackages {
			assert.NotContains(t, sqsMessage.MessageBody, fmt.Sprintf(`"nodeId":%q`, p.NodeId))
		}
		for _, p := range restoringFilePackages {
			assert.Contains(t, sqsMessage.MessageBody, fmt.Sprintf(`"nodeId":%q`, p.NodeId))
		}
		for _, p := range restoringCollectionPackages {
			assert.Contains(t, sqsMessage.MessageBody, fmt.Sprintf(`"nodeId":%q`, p.NodeId))
		}
	}))
	defer mockSQSServer.Close()

	awsConfig := store.GetTestAWSConfig(t)

	s3Client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(store.GetTestMinioURL())
		// by default minio expects path style
		options.UsePathStyle = true
	})
	dyClient := dynamodb.NewFromConfig(awsConfig, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String(store.GetTestDynamoDBURL())
	})
	sqsClient := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(mockSQSServer.URL)
	})

	// Create the S3 fixture with the bucket and put requests
	putObjectInputs := make([]*s3.PutObjectInput, 0, len(putObjectInputByNodeId))
	for _, v := range putObjectInputByNodeId {
		putObjectInputs = append(putObjectInputs, v)
	}
	s3Fixture := store.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{Bucket: aws.String(bucketName), ObjectLockEnabledForBucket: aws.Bool(true)}).WithObjects(putObjectInputs...)
	defer s3Fixture.Teardown()

	// Delete the S3 objects and prepare put requests for the delete-records in Dynamo
	deleteRecordTableName := "TestDeleteRecords"
	putItemInputByNodeId := map[string]*dynamodb.PutItemInput{}
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
		putItemInputByNodeId[nodeId] = &dynamodb.PutItemInput{TableName: aws.String(deleteRecordTableName), Item: deleteRecordItem}
	}

	putItemInputs := make([]*dynamodb.PutItemInput, 0, len(putItemInputByNodeId))
	for _, input := range putItemInputByNodeId {
		putItemInputs = append(putItemInputs, input)
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

	dyFixture := store.NewDynamoDBFixture(t, dyClient, &createTableInput).WithItems(putItemInputs...)
	defer dyFixture.Teardown()

	sqlFactory := store.NewPostgresStoreFactory(db.DB)
	objectStore := store.NewS3Store(s3Client)
	nosqlStore := store.NewDynamoDBStore(dyClient, deleteRecordTableName)
	sqsChangelogStore := restore.NewSQSChangelogStore(sqsClient, jobsQueueID)
	base := NewBaseStore(sqlFactory, nosqlStore, objectStore, sqsChangelogStore)
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
		v := db.Queries(orgId)
		for _, untouched := range untouchedPackages {
			actual, err := v.GetPackageByNodeId(ctx, untouched.NodeId)
			if assert.NoError(t, err) {
				assertUntouchedPackage(t, untouched, actual)
			}
		}
		for _, restoring := range restoringCollectionPackages {
			actual, err := v.GetPackageByNodeId(ctx, restoring.NodeId)
			if assert.NoError(t, err) {
				assertRestoredPackage(t, CollectionRestoredState, restoring, actual)
			}
		}
		for _, restoring := range restoringFilePackages {
			actual, err := v.GetPackageByNodeId(ctx, restoring.NodeId)
			if assert.NoError(t, err) {
				assertRestoredPackage(t, FileRestoredState, restoring, actual)
			}
		}

		listOut, err := s3Fixture.Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucketName)})
		if assert.NoError(t, err) {
			// No more delete markers and the number of versions is the same.
			assert.Empty(t, listOut.DeleteMarkers)
			assert.Len(t, listOut.Versions, len(putObjectInputs))
		}
		scanOut, err := dyFixture.Client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(deleteRecordTableName)})
		if assert.NoError(t, err) {
			// All delete records have been removed
			assert.Zero(t, scanOut.ScannedCount)
			assert.Empty(t, scanOut.Items)
		}
	}

}

func assertUntouchedPackage(t *testing.T, initial, current *pgdb.Package) {
	assert.Equal(t, initial, current)
}

func assertRestoredPackage(t *testing.T, expectedState packageState.State, initial, current *pgdb.Package) {
	assert.Equal(t, expectedState, current.PackageState)
	deletedNamePrefix := DeletedNamePrefix(initial.NodeId)
	assert.False(t, strings.HasPrefix(current.Name, deletedNamePrefix))
	assert.Equal(t, initial.Name[len(deletedNamePrefix):], current.Name)
}
