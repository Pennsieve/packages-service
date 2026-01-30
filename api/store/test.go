package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dytypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/processingState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/uploadState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
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

func OpenDB(t *testing.T, additionalOptions ...PostgresOption) *TestDB {
	pgConfig := PostgresConfigFromEnv()
	db, err := pgConfig.Open(additionalOptions...)
	if err != nil {
		assert.FailNowf(t, "cannot open database", "config: %s, err: %v", pgConfig, err)
	}
	testDB := &TestDB{
		DB: db,
		t:  t,
	}
	if err = testDB.PingUntilReady(); err != nil {
		assert.FailNow(testDB.t, "cannot ping database", "config: %s, err: %v", pgConfig, err)
	}
	return testDB
}

// WithT returns a new *TestDB with the same underlying *sql.DB as this TestDB, but with the given *testing.T.
// Used in sub-tests to avoid the wrong *testing.T being involved. Gets around poor design that stashes the original
// *testing.T.
func (tdb *TestDB) WithT(t *testing.T) *TestDB {
	return &TestDB{
		DB: tdb.DB,
		t:  t,
	}
}

func (tdb *TestDB) ExecSQLFile(sqlFile string) {
	// Always run base setup first to ensure required data exists
	if sqlFile != "00-base-setup.sql" {
		basePath := filepath.Join("testdata", "00-base-setup.sql")
		if baseBytes, err := os.ReadFile(basePath); err == nil {
			if _, err = tdb.Exec(string(baseBytes)); err != nil {
				// Log but don't fail - base setup is optional
				tdb.t.Logf("Warning: base setup failed: %v", err)
			}
		}
	}

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

func (tdb *TestDB) GetPackageStorage(orgID int, packageID int) (size int64) {
	query := fmt.Sprintf(`SELECT size from "%d".package_storage where package_id = $1`, orgID)
	require.NoError(tdb.t, tdb.QueryRow(query, packageID).Scan(&size))
	return
}

func (tdb *TestDB) GetDatasetStorage(orgID int, datasetID int) (size int64) {
	query := fmt.Sprintf(`SELECT size from "%d".dataset_storage where dataset_id = $1`, orgID)
	require.NoError(tdb.t, tdb.QueryRow(query, datasetID).Scan(&size))
	return
}

func (tdb *TestDB) GetOrganizationStorage(organizationID int) (size int64) {
	query := `SELECT size from pennsieve.organization_storage where organization_id = $1`
	require.NoError(tdb.t, tdb.QueryRow(query, organizationID).Scan(&size))
	return
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

func (tdb *TestDB) Queries(orgId int) *Queries {
	return &Queries{
		db:     tdb.DB,
		OrgId:  orgId,
		Logger: NoLogger{},
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
	// awsKey and awsSecret should match MINIO_ROOT_USER and MINIO_ROOT_PASSWORD respectively.
	awsKey := "awstestkey"
	awsSecret := "awstestsecret"
	awsConfig, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsKey, awsSecret, "")),
	)
	if err != nil {
		assert.FailNow(t, "error creating AWS config", err)
	}
	return awsConfig
}

func GetTestMinioURL() string {
	// when tests are run in Docker on CI, the env vars are set. Otherwise, we are running the tests locally, so use localhost.
	return getEnvOrDefault("MINIO_URL", "http://localhost:9000")
}

func GetTestDynamoDBURL() string {
	// when tests are run in Docker on CI, the env vars are set. Otherwise, we are running the tests locally, so use localhost.
	return getEnvOrDefault("DYNAMODB_URL", "http://localhost:8000")
}

type Fixture struct {
	T *testing.T
}

func waitForEverything[T any](inputs []T, waitFn func(T) error) error {
	var wg sync.WaitGroup
	waitErrors := make([]error, len(inputs))
	for index, input := range inputs {
		wg.Add(1)
		go func(i int, in T) {
			defer wg.Done()
			waitErrors[i] = waitFn(in)
		}(index, input)
	}
	wg.Wait()
	for _, we := range waitErrors {
		if we != nil {
			return we
		}
	}
	return nil
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
	if len(inputs) == 0 {
		return &f
	}
	ctx := context.Background()
	var waitInputs []s3.HeadBucketInput
	for _, input := range inputs {
		bucketName := aws.ToString(input.Bucket)
		if _, err := f.Client.CreateBucket(ctx, input); err != nil {
			assert.FailNow(f.T, "error creating test bucket", "bucket: %s, error: %v", bucketName, err)
		}
		f.Buckets[bucketName] = true
		waitInputs = append(waitInputs, s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	}
	if err := waitForEverything(waitInputs, func(s s3.HeadBucketInput) error {
		return s3.NewBucketExistsWaiter(f.Client).Wait(ctx, &s, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test bucket not created", err)
	}

	return &f
}

func (f *S3Fixture) WithBucketVersioning(versionedBuckets ...string) *S3Fixture {
	status := types.BucketVersioningStatusEnabled
	ctx := context.Background()
	for _, versionedBucket := range versionedBuckets {

		input := &s3.PutBucketVersioningInput{
			Bucket: aws.String(versionedBucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: status,
			},
		}
		_, err := f.Client.PutBucketVersioning(ctx, input)
		require.NoError(f.T, err)
	}
	return f
}

func (f *S3Fixture) WithObjects(objectInputs ...*s3.PutObjectInput) *S3Fixture {
	ctx := context.Background()
	var waitInputs []s3.HeadObjectInput
	for _, input := range objectInputs {
		if _, err := f.Client.PutObject(ctx, input); err != nil {
			assert.FailNow(f.T, "error putting test object", "bucket: %s, key: %s, error: %v", aws.ToString(input.Bucket), aws.ToString(input.Key), err)
		}
		waitInputs = append(waitInputs, s3.HeadObjectInput{Bucket: input.Bucket, Key: input.Key})
	}
	if err := waitForEverything(waitInputs, func(i s3.HeadObjectInput) error {
		return s3.NewObjectExistsWaiter(f.Client).Wait(ctx, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test object not created", err)
	}
	return f
}

func (f *S3Fixture) Teardown() {
	ctx := context.Background()
	var waitInputs []s3.HeadBucketInput
	for name := range f.Buckets {
		listInput := s3.ListObjectVersionsInput{Bucket: aws.String(name)}
		listOutput, err := f.Client.ListObjectVersions(ctx, &listInput)
		if err != nil {
			assert.FailNow(f.T, "error listing test objects", "bucket: %s, error: %v", name, err)
		}
		if aws.ToBool(listOutput.IsTruncated) {
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
		waitInputs = append(waitInputs, s3.HeadBucketInput{Bucket: aws.String(name)})
	}
	if err := waitForEverything(waitInputs, func(i s3.HeadBucketInput) error {
		return s3.NewBucketNotExistsWaiter(f.Client).Wait(ctx, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test bucket not deleted", err)
	}
}

func TestCreateDeleteRecordTableInput(tableName string) dynamodb.CreateTableInput {
	return dynamodb.CreateTableInput{TableName: aws.String(tableName),
		AttributeDefinitions: []dytypes.AttributeDefinition{
			{
				AttributeName: aws.String("NodeId"),
				AttributeType: dytypes.ScalarAttributeTypeS,
			},
		},
		KeySchema: []dytypes.KeySchemaElement{
			{
				AttributeName: aws.String("NodeId"),
				KeyType:       dytypes.KeyTypeHash,
			},
		},
		BillingMode: dytypes.BillingModePayPerRequest}
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
	var waitInputs []dynamodb.DescribeTableInput
	for _, input := range inputs {
		tableName := aws.ToString(input.TableName)
		if _, err := f.Client.CreateTable(ctx, input); err != nil {
			assert.FailNow(f.T, "error creating test table", "table: %s, error: %v", tableName, err)
		}
		f.Tables[tableName] = true
		waitInputs = append(waitInputs, dynamodb.DescribeTableInput{TableName: input.TableName})
	}
	if err := waitForEverything(waitInputs, func(i dynamodb.DescribeTableInput) error {
		return dynamodb.NewTableExistsWaiter(f.Client).Wait(ctx, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test table not created", err)
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
	var waitInputs []dynamodb.DescribeTableInput
	for name := range f.Tables {
		input := dynamodb.DeleteTableInput{TableName: aws.String(name)}
		if _, err := f.Client.DeleteTable(ctx, &input); err != nil {
			assert.FailNow(f.T, "error deleting test table", "table: %s, error: %v", name, err)
		}
		waitInputs = append(waitInputs, dynamodb.DescribeTableInput{TableName: input.TableName})
	}
	if err := waitForEverything(waitInputs, func(i dynamodb.DescribeTableInput) error {
		return dynamodb.NewTableNotExistsWaiter(f.Client).Wait(ctx, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test table not deleted", err)
	}

}

func RandString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type TestPackageAttribute struct {
	packageInfo.PackageAttribute
}

type TestPackageAttributes []TestPackageAttribute

func NewTestPackageAttribute() *TestPackageAttribute {
	return &TestPackageAttribute{packageInfo.PackageAttribute{
		Key:      RandString(10),
		Fixed:    rand.Intn(2) == 1,
		Value:    RandString(10),
		Hidden:   rand.Intn(2) == 1,
		Category: RandString(13),
		DataType: RandString(39),
	}}
}

func NewTestPackageAttributes(len int) TestPackageAttributes {
	attrs := make([]TestPackageAttribute, len)
	for i := range attrs {
		attrs[i] = *NewTestPackageAttribute()
	}
	return attrs
}

func (a *TestPackageAttribute) AsPackageAttribute() packageInfo.PackageAttribute {
	return a.PackageAttribute
}

func (as TestPackageAttributes) AsPackageAttributes() packageInfo.PackageAttributes {
	attrs := make([]packageInfo.PackageAttribute, len(as))
	for i := range attrs {
		attrs[i] = as[i].AsPackageAttribute()
	}
	return attrs
}

type TestPackage struct {
	pgdb.Package
}

func NewTestPackage(id int, datasetId int, ownerId int) *TestPackage {
	pt := RandPackageType()
	nodeId := NewTestPackageNodeId(pt)
	size := sql.NullInt64{}
	if pt != packageType.Collection {
		size.Int64 = 0
		size.Valid = true
	}
	return &TestPackage{pgdb.Package{
		Id:           int64(id),
		Name:         RandString(37),
		PackageType:  pt,
		PackageState: RandPackageState(),
		NodeId:       nodeId,
		ParentId:     sql.NullInt64{},
		DatasetId:    datasetId,
		OwnerId:      ownerId,
		Size:         size,
		ImportId:     sql.NullString{String: uuid.NewString(), Valid: true},
		Attributes:   NewTestPackageAttributes(3).AsPackageAttributes(),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}}
}

func (p *TestPackage) WithParentId(parentId int64) *TestPackage {
	p.ParentId = sql.NullInt64{Int64: parentId, Valid: true}
	return p
}

func (p *TestPackage) WithName(name string) *TestPackage {
	p.Name = name
	return p
}

func (p *TestPackage) WithType(pt packageType.Type) *TestPackage {
	p.PackageType = pt
	return p
}

func (p *TestPackage) WithState(ps packageState.State) *TestPackage {
	p.PackageState = ps
	return p
}

func (p *TestPackage) WithDeletedName() *TestPackage {
	deletedName := fmt.Sprintf("__%s__%s_%s", packageState.Deleted, p.NodeId, p.Name)
	p.Name = deletedName
	return p
}

func (p *TestPackage) Deleted() *TestPackage {
	p.PackageState = packageState.Deleted
	p.WithDeletedName()
	return p
}

func (p *TestPackage) Restoring() *TestPackage {
	p.PackageState = packageState.Restoring
	p.WithDeletedName()
	return p
}

func (p *TestPackage) AsPackage() pgdb.Package {
	return p.Package
}

func (p *TestPackage) Insert(ctx context.Context, db *TestDB, orgId int) *pgdb.Package {
	var pkg pgdb.Package
	query := fmt.Sprintf(`INSERT INTO "%d".packages (%[2]s)
						  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
						  RETURNING %[2]s`, orgId, packageScanner.ColumnNamesString)
	if err := db.QueryRowContext(ctx, query,
		p.Id,
		p.Name,
		p.PackageType,
		p.PackageState,
		p.NodeId,
		p.ParentId,
		p.DatasetId,
		p.OwnerId,
		p.Size,
		p.ImportId,
		p.Attributes,
		p.CreatedAt,
		p.UpdatedAt).Scan(
		&pkg.Id,
		&pkg.Name,
		&pkg.PackageType,
		&pkg.PackageState,
		&pkg.NodeId,
		&pkg.ParentId,
		&pkg.DatasetId,
		&pkg.OwnerId,
		&pkg.Size,
		&pkg.ImportId,
		&pkg.Attributes,
		&pkg.CreatedAt,
		&pkg.UpdatedAt); err != nil {
		assert.FailNow(db.t, "error inserting test package", err)
	}
	return &pkg
}

func NewTestPackageNodeId(pt packageType.Type) string {
	var typeString string
	if pt == packageType.Collection {
		typeString = "collection"
	} else {
		typeString = "package"
	}
	return fmt.Sprintf("N:%s:%s", typeString, uuid.NewString())
}

func RandPackageType() packageType.Type {
	pTypes := []packageType.Type{packageType.Image,
		packageType.MRI,
		packageType.Slide,
		packageType.ExternalFile,
		packageType.MSWord,
		packageType.PDF,
		packageType.CSV,
		packageType.Tabular,
		packageType.TimeSeries,
		packageType.Video,
		packageType.Unknown,
		packageType.Collection,
		packageType.Text,
		packageType.Unsupported,
		packageType.HDF5,
		packageType.ZIP}
	return pTypes[rand.Intn(len(pTypes))]
}

func RandPackageState() packageState.State {
	states := []packageState.State{packageState.Unavailable,
		packageState.Uploaded,
		packageState.Deleting,
		packageState.Infected,
		packageState.UploadFailed,
		packageState.Processing,
		packageState.Ready,
		packageState.ProcessingFailed,
		packageState.Deleted,
		packageState.Restoring}
	return states[rand.Intn(len(states))]
}

type TestFile struct {
	pgdb.File
}

func NewTestFile(packageId int) *TestFile {
	objType := randInt64Type(objectType.Source)
	procState := legalProcessingState(objType)
	file := pgdb.File{
		PackageId:  packageId,
		Name:       RandString(37),
		FileType:   randInt64Type(fileType.ZIP),
		S3Bucket:   RandString(15),
		S3Key:      RandString(64),
		ObjectType: objType,
		// Keep size small, because we will generate objects of this size for Minio.
		Size:            rand.Int63n(1000) + 1,
		CheckSum:        "{}",
		ProcessingState: procState,
		UploadedState:   randInt64Type(uploadState.Uploaded),
		Published:       false,
	}
	return &TestFile{file}
}

func (f *TestFile) WithPublished(published bool) *TestFile {
	f.Published = published
	return f
}

func (f *TestFile) WithObjectType(objType objectType.ObjectType) *TestFile {
	f.ObjectType = objType
	f.ProcessingState = legalProcessingState(objType)
	return f
}

func (f *TestFile) WithBucket(bucketName string) *TestFile {
	f.S3Bucket = bucketName
	return f
}

func (f *TestFile) Insert(ctx context.Context, db *TestDB, orgId int) string {
	query := fmt.Sprintf(`INSERT into "%d".files (package_id, name, file_type, s3_bucket, s3_key, object_type, size, checksum, processing_state, uploaded_state, published) 
                          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) 
                          RETURNING id`, orgId)
	var fileId string
	require.NoError(db.t, db.QueryRowContext(ctx, query,
		f.PackageId,
		f.Name,
		f.FileType.String(),
		f.S3Bucket,
		f.S3Key,
		f.ObjectType.String(),
		f.Size,
		f.CheckSum,
		f.ProcessingState.String(),
		f.UploadedState.String(),
		f.Published).
		Scan(&fileId), "error inserting test file")
	// Why is pgdb.File.Id defined as string?
	f.Id = fileId
	return fileId
}

// IntId returns the string valued pgdb.File.Id as an int instead of a string.
// Seems to be a bug in pbdb that this is defined as a string?
func (f *TestFile) IntId(t require.TestingT) int {
	require.NotEmpty(t, f.Id, "File.Id is empty. Call Insert() to generate Id value")
	intId, err := strconv.Atoi(f.Id)
	require.NoError(t, err, "error converting File.Id to int")
	return intId
}

func randInt64Type[T ~int64](maxValue T) T {
	return T(rand.Int63n(int64(maxValue + 1)))
}

// legalProcessingState returns a processingState.ProcessingState that will satisfy check constraint on object type and processing state.
func legalProcessingState(objType objectType.ObjectType) processingState.ProcessingState {
	procState := processingState.NotProcessable
	if objType == objectType.Source {
		procState = randInt64Type(processingState.Processed)
	}
	return procState
}
