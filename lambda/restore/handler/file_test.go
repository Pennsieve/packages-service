package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"testing"
)

func TestGetOriginalName(t *testing.T) {
	expected := "file.txt"
	nodeId := "N:package:12345"
	for name, testData := range map[string]struct {
		prefix      string
		expectError bool
	}{
		"no prefix":   {prefix: "", expectError: true},
		"bad prefix":  {prefix: "NotWhatIsExpected_", expectError: true},
		"good prefix": {prefix: fmt.Sprintf("__%s__%s_", packageState.Deleted, nodeId), expectError: false},
	} {
		deletedName := fmt.Sprintf("%s%s", testData.prefix, expected)
		t.Run(name, func(t *testing.T) {
			actual, err := GetOriginalName(deletedName, nodeId)
			if testData.expectError {
				assert.Error(t, err)
			} else {
				if assert.NoError(t, err) {
					assert.Equal(t, expected, actual)
				}
			}
		})
	}
}

func TestNewNameParts(t *testing.T) {
	for name, testData := range map[string]struct {
		input        string
		expectedBase string
		expectedExt  string
	}{
		"no extension":      {"test", "test", ""},
		"extension":         {"test.txt", "test", ".txt"},
		"more than one dot": {"test.main.txt", "test.main", ".txt"},
		"final dot":         {"test.", "test", "."},
	} {
		t.Run(name, func(t *testing.T) {
			actual := NewNameParts(testData.input)
			assert.Equal(t, testData.expectedBase, actual.Base)
			assert.Equal(t, testData.expectedExt, actual.Ext)
		})
	}
}

func TestNameParts_Next(t *testing.T) {
	parts := NewNameParts("file.txt")

	first := parts.Next()
	assert.Equal(t, "file-restored_1.txt", first)
	assert.True(t, parts.More())

	second := parts.Next()
	assert.Equal(t, "file-restored_2.txt", second)
	assert.True(t, parts.More())
}

func TestNameParts_Limit(t *testing.T) {
	parts := NameParts{
		Base:  "file",
		Ext:   ".txt",
		i:     0,
		limit: 2,
		more:  true,
	}

	first := parts.Next()
	assert.Equal(t, "file-restored_1.txt", first)
	assert.True(t, parts.More())

	afterLimit := parts.Next()
	assert.Regexp(t, "file-restored_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.txt", afterLimit)
	assert.False(t, parts.More())
}

func TestRestoreName(t *testing.T) {
	db := store.OpenDB(t)
	defer db.Close()
	orgId := 2
	for name, d := range map[string]struct {
		id             int64
		nodeId         string
		name           string
		expectedResult string
	}{"simple rename": {
		int64(1),
		"N:package:ae253796-256a-4b9e-ba80-1c4c5a2afe6b",
		"__DELETED__N:package:ae253796-256a-4b9e-ba80-1c4c5a2afe6b_file.txt",
		"file.txt",
	}, "conflict with non-deleted file": {
		int64(2),
		"N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8",
		"__DELETED__N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8_another-file.txt",
		"another-file-restored_1.txt",
	}} {
		db.ExecSQLFile("restore-package-name-test.sql")
		sqlFactory := store.NewPostgresStoreFactory(db.DB)
		ctx := context.Background()
		messageHandler := NewMessageHandler(events.SQSMessage{}, NewBaseStore(sqlFactory, nil, nil, nil))
		restoreInfo := models.RestorePackageInfo{
			Id:     d.id,
			NodeId: d.nodeId,
			Name:   d.name,
		}
		orginalName, err := GetOriginalName(d.name, d.nodeId)
		if err != nil {
			assert.FailNow(t, "test case does not use correct deleted file name format", err)
		}
		t.Run(name, func(t *testing.T) {
			var restoredName *RestoredName
			err := messageHandler.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) (restoreNameError error) {
				restoredName, restoreNameError = messageHandler.restoreName(ctx, restoreInfo, store)
				return
			})
			if assert.NoError(t, err) {
				query := fmt.Sprintf(`SELECT name from "%d".packages where id = $1`, orgId)
				var actualName string
				err = db.QueryRow(query, restoreInfo.Id).Scan(&actualName)
				if assert.NoError(t, err) {
					assert.Equal(t, d.expectedResult, actualName)
					assert.Equal(t, d.expectedResult, restoredName.Value)
					if actualName == orginalName {
						assert.Empty(t, restoredName.OriginalName)
					} else {
						assert.Equal(t, orginalName, restoredName.OriginalName)
					}
				}
			}
		})
		db.Truncate(orgId, "packages")

	}
}

func TestRestoreName_ConflictWithDeletedFile(t *testing.T) {
	db := store.OpenDB(t)
	defer db.Close()
	orgId := 2
	db.ExecSQLFile("restore-package-name-test.sql")
	defer db.Truncate(orgId, "packages")

	sqlFactory := store.NewPostgresStoreFactory(db.DB)
	ctx := context.Background()
	handler := NewMessageHandler(events.SQSMessage{}, NewBaseStore(sqlFactory, nil, nil, nil))
	originalName := "root-dir"
	restoreInfo1 := models.RestorePackageInfo{
		Id:     5,
		NodeId: "N:collection:180d4f48-ea2b-435c-ac69-780eeaf89745",
		Name:   fmt.Sprintf("__DELETED__N:collection:180d4f48-ea2b-435c-ac69-780eeaf89745_%s", originalName),
	}
	expectedName2 := "root-dir-restored_1"
	restoreInfo2 := models.RestorePackageInfo{
		Id:     6,
		NodeId: "N:collection:0f197fab-cb7b-4414-8f7c-27d7aafe7c53",
		Name:   fmt.Sprintf("__DELETED__N:collection:0f197fab-cb7b-4414-8f7c-27d7aafe7c53_%s", originalName),
	}

	err := handler.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		restoredName1, err := handler.restoreName(ctx, restoreInfo1, store)
		if assert.NoError(t, err) {
			assert.Equal(t, originalName, restoredName1.Value)
			assert.Empty(t, restoredName1.OriginalName)
			restoredName2, err := handler.restoreName(ctx, restoreInfo2, store)
			assert.NoError(t, err)
			assert.Equal(t, expectedName2, restoredName2.Value)
			assert.Equal(t, originalName, restoredName2.OriginalName)
		}
		return nil
	})
	if assert.NoError(t, err) {
		query := fmt.Sprintf(`SELECT name from "%d".packages where id = $1`, orgId)

		var actualName1 string
		err = db.QueryRow(query, restoreInfo1.Id).Scan(&actualName1)
		if assert.NoError(t, err) {
			assert.Equal(t, originalName, actualName1)
		}

		var actualName2 string
		err = db.QueryRow(query, restoreInfo2.Id).Scan(&actualName2)
		if assert.NoError(t, err) {
			assert.Equal(t, expectedName2, actualName2)
		}
	}

}

func TestMessageHandler_handleFilePackage(t *testing.T) {
	ctx := context.Background()

	db := store.OpenDB(t)
	orgId := 2

	awsConfig := store.GetTestAWSConfig(t)

	s3Client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(store.GetTestMinioURL())
		// by default minio expects path style
		options.UsePathStyle = true
	})

	deleteRecordTableName := "TestDeleteRecordHandleFile"
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
	bucketName := "test-bucket-handle-file"

	testPackage := NewTestSourcePackage(1, datasetId, 1, func(testPackage *store.TestPackage) {
		testPackage.Restoring()
	}).WithSources(1, bucketName, func(testFile *store.TestFile) {
		testFile.WithPublished(false)
	}).Insert(ctx, db, orgId)
	putObjectInputs := testPackage.PutObjectInputs()

	s3Fixture := store.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}).
		WithBucketVersioning(bucketName).
		WithObjects(putObjectInputs...)
	defer s3Fixture.Teardown()

	keyToDeleteVersionId := testPackage.DeleteFiles(ctx, t, s3Client)

	putItemInputs := testPackage.PutItemInputs(t, deleteRecordTableName, keyToDeleteVersionId)

	createTableInput := store.TestCreateDeleteRecordTableInput(deleteRecordTableName)
	dyFixture := store.NewDynamoDBFixture(t, dyClient, &createTableInput).WithItems(putItemInputs...)
	defer dyFixture.Teardown()

	sqlFactory := store.NewPostgresStoreFactory(db.DB)
	dyStore := store.NewDynamoDBStore(dyClient, deleteRecordTableName)
	objectStore := store.NewS3Store(s3Client)
	handler := NewMessageHandler(events.SQSMessage{MessageId: uuid.NewString(), Body: "{}"}, NewBaseStore(sqlFactory, dyStore, objectStore, nil))
	restoreInfo := models.RestorePackageInfo{
		Id:     testPackage.Package.Id,
		NodeId: testPackage.Package.NodeId,
		Name:   testPackage.Package.Name,
		Type:   testPackage.Package.PackageType,
	}
	changelogEvents, err := handler.handleFilePackage(ctx, orgId, int64(datasetId), restoreInfo)
	require.NoError(t, err)
	assert.Len(t, changelogEvents, 1)
	changelogEvent := changelogEvents[0]
	assert.Equal(t, testPackage.Package.Id, changelogEvent.Id)
	assert.Equal(t, testPackage.Package.NodeId, changelogEvent.NodeId)
	assertRestoredName(t, testPackage.Package.NodeId, testPackage.Package.Name, changelogEvent.Name)
	assert.Empty(t, changelogEvent.OriginalName)
	assert.Nil(t, changelogEvent.Parent)

	v := db.Queries(orgId)
	actual, err := v.GetPackageByNodeId(ctx, testPackage.Package.NodeId)
	require.NoError(t, err)
	assertRestoredPackage(t, FileRestoredState, testPackage.Package.AsPackage(), actual)

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

type TestSourcePackage struct {
	Package *store.TestPackage
	Files   []*store.TestFile
}

type PackageConfigFunc func(testPackage *store.TestPackage)

type FileConfigFunc func(testFile *store.TestFile)

func NewTestSourcePackage(packageId, datasetId, ownerId int, packageConfig ...PackageConfigFunc) *TestSourcePackage {
	sourcePackage := TestSourcePackage{}
	sourcePackage.Package = store.NewTestPackage(packageId, datasetId, ownerId)
	for _, configFunc := range packageConfig {
		configFunc(sourcePackage.Package)
	}
	return &sourcePackage
}

// WithSources adds count store.TestFile to this TestSourcePackage. All files will have object type "source" and have the
// given bucketName as their S3Bucket. The given FileConfigFunc will be applied to all files.
func (s *TestSourcePackage) WithSources(count int, bucketName string, fileConfig ...FileConfigFunc) *TestSourcePackage {
	for range count {
		testFile := store.NewTestFile(int(s.Package.Id)).
			WithObjectType(objectType.Source).
			WithBucket(bucketName)
		for _, configFunc := range fileConfig {
			configFunc(testFile)
		}
		s.Files = append(s.Files, testFile)
	}
	return s
}

func (s *TestSourcePackage) Insert(ctx context.Context, db store.TestDB, orgId int) *TestSourcePackage {
	s.Package.Insert(ctx, db, orgId)
	for _, f := range s.Files {
		f.Insert(ctx, db, orgId)
	}
	return s
}

func (s *TestSourcePackage) PutObjectInputs() []*s3.PutObjectInput {
	var inputs []*s3.PutObjectInput
	for i, f := range s.Files {
		inputs = append(inputs, &s3.PutObjectInput{
			Bucket: aws.String(f.S3Bucket),
			Key:    aws.String(f.S3Key),
			Body:   strings.NewReader(fmt.Sprintf("object %d content", i)),
		})
	}
	return inputs
}

func (s *TestSourcePackage) DeleteFiles(ctx context.Context, t require.TestingT, s3Client *s3.Client) map[string]string {
	s3KeyToDeleteMarkerVersion := map[string]string{}
	for _, f := range s.Files {
		// Delete the object from S3
		deleteObjectOutput, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(f.S3Bucket), Key: aws.String(f.S3Key)})
		require.NoError(t, err, "error setting up deleted objects")
		require.NotNil(t, deleteObjectOutput.VersionId, "version id of delete is nil; is bucket versioning enabled?")
		s3KeyToDeleteMarkerVersion[f.S3Key] = aws.ToString(deleteObjectOutput.VersionId)
	}
	return s3KeyToDeleteMarkerVersion
}

func (s *TestSourcePackage) PutItemInputs(t require.TestingT, deleteRecordTableName string, keyToDeleteVersionId map[string]string) []*dynamodb.PutItemInput {
	var putItemInputs []*dynamodb.PutItemInput
	for _, f := range s.Files {
		deleteRecord := store.S3ObjectInfo{Bucket: f.S3Bucket, Key: f.S3Key, NodeId: s.Package.NodeId, Size: strconv.FormatInt(f.Size, 10), VersionId: keyToDeleteVersionId[f.S3Key]}
		deleteRecordItem, err := attributevalue.MarshalMap(deleteRecord)
		require.NoError(t, err, "error setting up item in delete record table")
		putItemInputs = append(putItemInputs, &dynamodb.PutItemInput{
			Item:      deleteRecordItem,
			TableName: aws.String(deleteRecordTableName),
		})
	}
	return putItemInputs
}
