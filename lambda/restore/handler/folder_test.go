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
	"slices"
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
	storageBucketName := "test-storage-bucket-handle-folder"
	publishBucketName := "test-publish-bucket-handle-folder"

	folderPackage := store.NewTestPackage(1, datasetId, 1).WithType(packageType.Collection).Restoring().Insert(ctx, db, orgId)
	unpublishedSourcePackage := NewTestSourcePackage(2, datasetId, 1, func(testPackage *store.TestPackage) {
		testPackage.Deleted()
		testPackage.WithParentId(folderPackage.Id)
	}).WithSources(1, storageBucketName, func(testFile *store.TestFile) {
		testFile.WithPublished(false)
	}).Insert(ctx, db, orgId)

	publishedSourcePackage := NewTestSourcePackage(3, datasetId, 1, func(testPackage *store.TestPackage) {
		testPackage.Deleted()
		testPackage.WithParentId(folderPackage.Id)
	}).WithSources(1, publishBucketName, func(testFile *store.TestFile) {
		testFile.WithPublished(true)
	}).Insert(ctx, db, orgId)

	s3Fixture := store.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{Bucket: aws.String(storageBucketName)}).
		WithBucketVersioning(storageBucketName)
	t.Cleanup(func() { s3Fixture.Teardown() })

	putObjectResults := s3Fixture.PutObjects(unpublishedSourcePackage.PutObjectInputs()...)

	keyToDeleteVersionId := unpublishedSourcePackage.DeleteFiles(ctx, t, s3Client)

	putItemInputs := unpublishedSourcePackage.PutItemInputs(t, deleteRecordTableName, keyToDeleteVersionId)
	putItemInputs = append(putItemInputs, publishedSourcePackage.PutItemInputs(t, deleteRecordTableName, nil)...)

	createTableInput := store.TestCreateDeleteRecordTableInput(deleteRecordTableName)
	dyFixture := store.NewDynamoDBFixture(t, dyClient, &createTableInput).WithItems(putItemInputs...)
	t.Cleanup(func() { dyFixture.Teardown() })

	sqlFactory := store.NewPostgresStoreFactory(db.DB)
	dyStore := store.NewDynamoDBStore(dyClient, deleteRecordTableName)
	objectStore := store.NewS3Store(s3Client)
	handler := NewMessageHandler(events.SQSMessage{MessageId: uuid.NewString(), Body: "{}"}, NewBaseStore(sqlFactory, dyStore, objectStore, nil))
	restoreInfo := models.RestorePackageInfo{
		Id:     folderPackage.Id,
		NodeId: folderPackage.NodeId,
		Name:   folderPackage.Name,
		Type:   folderPackage.PackageType,
	}
	changelogEvents, err := handler.handleFolderPackage(ctx, orgId, int64(datasetId), restoreInfo)
	require.NoError(t, err)
	assert.Len(t, changelogEvents, 3)
	for _, changelogEvent := range changelogEvents {
		var expectedPackage pgdb.Package
		if nodeId := changelogEvent.NodeId; nodeId == folderPackage.NodeId {
			expectedPackage = *folderPackage
		} else if nodeId == unpublishedSourcePackage.Package.NodeId {
			expectedPackage = unpublishedSourcePackage.Package.AsPackage()
		} else if nodeId == publishedSourcePackage.Package.NodeId {
			expectedPackage = publishedSourcePackage.Package.AsPackage()
		} else {
			require.FailNow(t, "unexpected node id in changelog event", nodeId)
		}
		assert.Equal(t, expectedPackage.Id, changelogEvent.Id)
		assertRestoredName(t, expectedPackage.NodeId, expectedPackage.Name, changelogEvent.Name)
		assert.Empty(t, changelogEvent.OriginalName)
		assert.Nil(t, changelogEvent.Parent)
	}

	// no storage tables values have been set up, so the storage now should just be the size of the source package
	unpublishedPackageSize := unpublishedSourcePackage.Size()
	actualUnpublishedPackageStorage := db.GetPackageStorage(orgId, int(unpublishedSourcePackage.Package.Id))
	publishedPackageSize := publishedSourcePackage.Size()
	actualPublishedPackageStorage := db.GetPackageStorage(orgId, int(publishedSourcePackage.Package.Id))
	assert.Equal(t, actualUnpublishedPackageStorage, unpublishedPackageSize)
	assert.Equal(t, actualPublishedPackageStorage, publishedPackageSize)
	assert.Equal(t, db.GetDatasetStorage(orgId, datasetId), unpublishedPackageSize+publishedPackageSize)
	assert.Equal(t, db.GetOrganizationStorage(orgId), unpublishedPackageSize+publishedPackageSize)

	v := db.Queries(orgId)
	actualFolderPackage, err := v.GetPackageByNodeId(ctx, folderPackage.NodeId)
	require.NoError(t, err)
	assertRestoredPackage(t, CollectionRestoredState, *folderPackage, actualFolderPackage)

	actualUnpublishedSourcePackage, err := v.GetPackageByNodeId(ctx, unpublishedSourcePackage.Package.NodeId)
	require.NoError(t, err)
	assertRestoredPackage(t, FileRestoredState, unpublishedSourcePackage.Package.AsPackage(), actualUnpublishedSourcePackage)

	actualPublishedSourcePackage, err := v.GetPackageByNodeId(ctx, publishedSourcePackage.Package.NodeId)
	require.NoError(t, err)
	assertRestoredPackage(t, FileRestoredState, publishedSourcePackage.Package.AsPackage(), actualPublishedSourcePackage)

	listOut, err := s3Fixture.Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(storageBucketName)})
	require.NoError(t, err)
	// No more delete markers and the number of versions is the same.
	assert.Empty(t, listOut.DeleteMarkers)
	if assert.Len(t, listOut.Versions, len(putObjectResults)) {
		for _, actualVersion := range listOut.Versions {
			expectedIdx := slices.IndexFunc(putObjectResults, func(s store.PutObjectResponse) bool {
				return aws.ToString(s.Input.Bucket) == storageBucketName && aws.ToString(s.Input.Key) == aws.ToString(actualVersion.Key)
			})
			require.True(t, expectedIdx != -1, "expected object with key %s not found in bucket %s", aws.ToString(actualVersion.Key), storageBucketName)
			expectedVersion := aws.ToString(putObjectResults[expectedIdx].Output.VersionId)
			assert.Equal(t, expectedVersion, aws.ToString(actualVersion.VersionId))
		}
	}

	scanOut, err := dyFixture.Client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(deleteRecordTableName)})
	require.NoError(t, err)
	// All delete records have been removed
	assert.Zero(t, scanOut.ScannedCount)
	assert.Empty(t, scanOut.Items)

}
