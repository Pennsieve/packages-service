package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMessageHandler_handleFolderPackage(t *testing.T) {
	ctx := context.Background()

	db := store.OpenDB(t)
	orgId := 2

	awsConfig := store.GetTestAWSConfig(t)

	s3Client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(store.GetTestMinioURL())
		// by default minio expects path style
		options.UsePathStyle = true
	})

	deleteRecordTableName := "TestDeleteRecordHandleFolder"
	dyClient := dynamodb.NewFromConfig(awsConfig, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String(store.GetTestDynamoDBURL())
	})

	t.Cleanup(func() {
		db.Truncate(orgId, "packages")
		db.Truncate(orgId, "package_storage")
		db.Truncate(orgId, "dataset_storage")
		db.TruncatePennsieve("organization_storage")
		db.Close()
	})

	datasetId := 1
	bucketName := "test-bucket-handle-folder"

	testFolderPackage := store.NewTestPackage(1, datasetId, 1).WithType(packageType.Collection).Restoring().Insert(ctx, db, orgId)
	testSourcePackage := NewTestSourcePackage(2, datasetId, 1, func(testPackage *store.TestPackage) {
		testPackage.Deleted()
		testPackage.WithParentId(testFolderPackage.Id)
	}).WithSources(1, bucketName, func(testFile *store.TestFile) {
		testFile.WithPublished(false)
	}).Insert(ctx, db, orgId)
	putObjectInputs := testSourcePackage.PutObjectInputs()

	s3Fixture := store.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}).
		WithBucketVersioning(bucketName).
		WithObjects(putObjectInputs...)
	defer s3Fixture.Teardown()

	keyToDeleteVersionId := testSourcePackage.DeleteFiles(ctx, t, s3Client)

	putItemInputs := testSourcePackage.PutItemInputs(t, deleteRecordTableName, keyToDeleteVersionId)

	createTableInput := store.TestCreateDeleteRecordTableInput(deleteRecordTableName)
	dyFixture := store.NewDynamoDBFixture(t, dyClient, &createTableInput).WithItems(putItemInputs...)
	defer dyFixture.Teardown()

	sqlFactory := store.NewPostgresStoreFactory(db.DB)
	dyStore := store.NewDynamoDBStore(dyClient, deleteRecordTableName)
	objectStore := store.NewS3Store(s3Client)
	handler := NewMessageHandler(events.SQSMessage{MessageId: uuid.NewString(), Body: "{}"}, NewBaseStore(sqlFactory, dyStore, objectStore, nil))
	restoreInfo := models.RestorePackageInfo{
		Id:     testFolderPackage.Id,
		NodeId: testFolderPackage.NodeId,
		Name:   testFolderPackage.Name,
		Type:   testFolderPackage.PackageType,
	}
	changelogEvents, err := handler.handleFolderPackage(ctx, orgId, int64(datasetId), restoreInfo)
	require.NoError(t, err)
	assert.Len(t, changelogEvents, 2)
	for _, changelogEvent := range changelogEvents {
		var expectedPackage pgdb.Package
		if nodeId := changelogEvent.NodeId; nodeId == testFolderPackage.NodeId {
			expectedPackage = *testFolderPackage
		} else if nodeId == testSourcePackage.Package.NodeId {
			expectedPackage = testSourcePackage.Package.AsPackage()
		} else {
			require.FailNow(t, "unexpected node id in changelog event", nodeId)
		}
		assert.Equal(t, expectedPackage.Id, changelogEvent.Id)
		assertRestoredName(t, expectedPackage.NodeId, expectedPackage.Name, changelogEvent.Name)
		assert.Empty(t, changelogEvent.OriginalName)
		assert.Nil(t, changelogEvent.Parent)
	}

	v := db.Queries(orgId)
	actualFolderPackage, err := v.GetPackageByNodeId(ctx, testFolderPackage.NodeId)
	require.NoError(t, err)
	assertRestoredPackage(t, CollectionRestoredState, *testFolderPackage, actualFolderPackage)

	actualSourcePackage, err := v.GetPackageByNodeId(ctx, testSourcePackage.Package.NodeId)
	require.NoError(t, err)
	assertRestoredPackage(t, FileRestoredState, testSourcePackage.Package.AsPackage(), actualSourcePackage)

	listOut, err := s3Fixture.Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)
	// No more delete markers and the number of versions is the same.
	assert.Empty(t, listOut.DeleteMarkers)
	assert.Len(t, listOut.Versions, len(putObjectInputs))

	scanOut, err := dyFixture.Client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(deleteRecordTableName)})
	require.NoError(t, err)
	// All delete records have been removed
	assert.Zero(t, scanOut.ScannedCount)
	assert.Empty(t, scanOut.Items)

}
