package handler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/packages-service/api/store/restore"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net/http"
	"net/http/httptest"
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
	var restoringFilePackages []*TestSourcePackage
	restoringCollectionPackages := []*pgdb.Package{restoringCollection}
	// Insert the packages inside restoringCollection and prepare the S3 put requests.
	bucketName := "test-bucket"
	for i := 3; i < 53; i++ {
		if i%2 == 1 {
			pkg := store.NewTestPackage(i, 1, 1).
				WithParentId(restoringCollection.Id).
				WithType(packageType.Collection).
				Deleted().
				Insert(ctx, db, orgId)
			restoringCollectionPackages = append(restoringCollectionPackages, pkg)
		} else {
			pkg := NewTestSourcePackage(i, 1, 1, func(testPackage *store.TestPackage) {
				testPackage.WithType(packageType.CSV)
				testPackage.WithParentId(restoringCollection.Id)
				testPackage.Deleted()
			}).WithSources(rand.Intn(2)+1, bucketName, func(testFile *store.TestFile) {
				testFile.WithPublished(i%4 == 0)
			}).Insert(ctx, db, orgId)
			restoringFilePackages = append(restoringFilePackages, pkg)
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

		var logMsg changelog.Message
		require.NoError(t, json.Unmarshal([]byte(sqsMessage.MessageBody), &logMsg))
		assert.Equal(t, int64(orgId), logMsg.DatasetChangelogEventJob.OrganizationId)
		assert.Equal(t, int64(datasetId), logMsg.DatasetChangelogEventJob.DatasetId)

		assert.Len(t, logMsg.DatasetChangelogEventJob.Events, len(restoringCollectionPackages)+len(restoringFilePackages))
		var changelogNodeIds []string
		for _, e := range logMsg.DatasetChangelogEventJob.Events {
			assert.Equal(t, changelog.RestorePackage, e.EventType)
			actualDetail := requireAsType[map[string]any](t, e.EventDetail)
			require.Contains(t, actualDetail, "nodeId")
			actualNodeId := requireAsType[string](t, actualDetail["nodeId"])
			changelogNodeIds = append(changelogNodeIds, actualNodeId)
		}
		for _, p := range untouchedPackages {
			assert.NotContains(t, changelogNodeIds, p.NodeId)
		}
		for _, p := range restoringFilePackages {
			assert.Contains(t, changelogNodeIds, p.Package.NodeId)
		}
		for _, p := range restoringCollectionPackages {
			assert.Contains(t, changelogNodeIds, p.NodeId)
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
	var putObjectInputs []*s3.PutObjectInput
	for _, v := range restoringFilePackages {
		putObjectInputs = append(putObjectInputs, v.PutObjectInputs()...)
	}
	s3Fixture := store.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}).
		WithBucketVersioning(bucketName).
		WithObjects(putObjectInputs...)
	defer s3Fixture.Teardown()

	// Delete the S3 objects and prepare put requests for the delete-records in Dynamo
	deleteRecordTableName := "TestDeleteRecords"
	var putItemInputs []*dynamodb.PutItemInput
	for _, fp := range restoringFilePackages {
		keyToDeleteVersionId := fp.DeleteFiles(ctx, t, s3Client)
		putItemInputs = append(putItemInputs, fp.PutItemInputs(t, deleteRecordTableName, keyToDeleteVersionId)...)
	}
	createTableInput := store.TestCreateDeleteRecordTableInput(deleteRecordTableName)
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
				assertRestoredPackage(t, CollectionRestoredState, *restoring, actual)
			}
		}
		for _, restoring := range restoringFilePackages {
			actual, err := v.GetPackageByNodeId(ctx, restoring.Package.NodeId)
			if assert.NoError(t, err) {
				assertRestoredPackage(t, FileRestoredState, restoring.Package.AsPackage(), actual)
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

func assertRestoredName(t *testing.T, nodeId string, deletedName string, currentName string) {
	deletedNamePrefix := DeletedNamePrefix(nodeId)
	assert.True(t, strings.HasPrefix(deletedName, deletedNamePrefix))
	assert.False(t, strings.HasPrefix(currentName, deletedNamePrefix))
	assert.Equal(t, deletedName[len(deletedNamePrefix):], currentName)
}

func assertRestoredPackage(t *testing.T, expectedState packageState.State, initial pgdb.Package, current *pgdb.Package) {
	assert.Equal(t, expectedState, current.PackageState, "expected state: %s, actual state: %s", expectedState.String(), current.PackageState.String())
	assertRestoredName(t, initial.NodeId, initial.Name, current.Name)
}

// requireAsType tests that the type of actual is ExpectedType and if so, returns actual converted to that type.
func requireAsType[ExpectedType any](t *testing.T, actual any) ExpectedType {
	var expected ExpectedType
	require.IsType(t, expected, actual)
	return actual.(ExpectedType)
}
