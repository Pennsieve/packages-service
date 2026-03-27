package handler

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dataset"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/organization"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/role"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func editorClaims(orgId int64, datasetNodeId string) *authorizer.Claims {
	return &authorizer.Claims{
		OrgClaim: &organization.Claim{IntId: orgId},
		UserClaim: &user.Claim{
			Id:     101,
			NodeId: "N:user:test-101",
		},
		DatasetClaim: &dataset.Claim{
			Role:   role.Editor,
			NodeId: datasetNodeId,
			IntId:  300,
		},
	}
}

func viewerClaims(orgId int64, datasetNodeId string) *authorizer.Claims {
	return &authorizer.Claims{
		OrgClaim: &organization.Claim{IntId: orgId},
		UserClaim: &user.Claim{
			Id:     101,
			NodeId: "N:user:test-101",
		},
		DatasetClaim: &dataset.Claim{
			Role:   role.Viewer,
			NodeId: datasetNodeId,
			IntId:  300,
		},
	}
}

func setupDownloadTestDB(t *testing.T) *store.TestDB {
	t.Helper()
	db := store.OpenDB(t)
	db.ExecSQLFile("download-manifest-test.sql")

	originalDB := PennsieveDB
	PennsieveDB = db.DB
	t.Cleanup(func() {
		PennsieveDB = originalDB
		db.Truncate(2, "files")
		db.Truncate(2, "packages")
		db.Truncate(2, "datasets")
		db.Close()
	})

	return db
}

func setupS3Client(t *testing.T) {
	t.Helper()
	awsConfig := store.GetTestAWSConfig(t)
	originalS3 := S3Client
	S3Client = s3.NewFromConfig(awsConfig)
	t.Cleanup(func() { S3Client = originalS3 })
}

func setupExternalBucketConfig(t *testing.T, externalBucketConfig ExternalBucketConfig) {
	t.Helper()
	bytes, err := json.Marshal(externalBucketConfig)
	require.NoError(t, err)
	t.Setenv(ExternalBucketsRoleMapKey, string(bytes))
}

func setupAssumeRoleClient(t *testing.T, assumeRoleClient stscreds.AssumeRoleAPIClient) {
	t.Helper()
	originalAssumeRoleClient := AssumeRoleClient
	AssumeRoleClient = assumeRoleClient
	t.Cleanup(func() {
		AssumeRoleClient = originalAssumeRoleClient
	})
}

func TestDownloadManifest_SinglePackage(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)
	setupExternalBucketConfig(t, nil)

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-standalone"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-1",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.DownloadManifestResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &manifest))

	assert.Equal(t, 1, manifest.Header.Count)
	assert.Equal(t, int64(8192), manifest.Header.Size)
	require.Len(t, manifest.Data, 1)

	entry := manifest.Data[0]
	assert.Equal(t, "N:package:dl-standalone", entry.NodeId)
	assert.Equal(t, "image.ome.tiff", entry.FileName)
	assert.Equal(t, "standalone-file", entry.PackageName)
	assert.Equal(t, "ome.tiff", entry.FileExtension)
	presignedURL, err := url.Parse(entry.URL)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(presignedURL.Host, "pennsieve-test-storage"))
	assert.Equal(t, "/org2/image.ome.tiff", presignedURL.Path)
	assert.Empty(t, presignedURL.Query().Get("versionId"))
	assert.Empty(t, presignedURL.Query().Get("x-amz-request-payer"))
	assert.Contains(t, presignedURL.Query().Get("X-Amz-Credential"), "/us-east-1/s3/")
	// Single-file package: path should be empty (no parents)
	assert.Empty(t, entry.Path)
}

func TestDownloadManifest_Collection(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)
	setupExternalBucketConfig(t, nil)

	// Request the collection — should return all child files (not the collection itself)
	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:collection:dl-root"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-2",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.DownloadManifestResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &manifest))

	// Should have 3 files: 1 from child-single-file + 2 from child-multi-file
	assert.Equal(t, 3, manifest.Header.Count)
	assert.Equal(t, int64(1024+2048+4096), manifest.Header.Size)

	// Verify path logic: single-file packages get parent names only,
	// multi-file packages get parent names + package name
	fileNames := map[string]models.DownloadManifestEntry{}
	for _, e := range manifest.Data {
		fileNames[e.FileName] = e
	}

	// child-single-file: 1 source file -> path = [root-collection] (parent only)
	single := fileNames["data.csv"]
	assert.Equal(t, []string{"root-collection"}, single.Path)

	// child-multi-file: 2 source files -> path = [root-collection, child-multi-file]
	multi1 := fileNames["part1.csv"]
	assert.Equal(t, []string{"root-collection", "child-multi-file"}, multi1.Path)
	multi2 := fileNames["part2.csv"]
	assert.Equal(t, []string{"root-collection", "child-multi-file"}, multi2.Path)
}

func TestDownloadManifest_DeletedPackagesExcluded(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)
	setupExternalBucketConfig(t, nil)

	// Request both a valid and a deleted package
	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-standalone", "N:package:dl-deleted"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-3",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.DownloadManifestResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &manifest))

	// Only the non-deleted file should appear
	assert.Equal(t, 1, manifest.Header.Count)
	assert.Equal(t, "N:package:dl-standalone", manifest.Data[0].NodeId)
}

func TestDownloadManifest_PublishedPackage(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)
	setupExternalBucketConfig(t, nil)

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-published"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-40",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.DownloadManifestResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &manifest))

	assert.Equal(t, 1, manifest.Header.Count)
	assert.Equal(t, int64(8192), manifest.Header.Size)
	require.Len(t, manifest.Data, 1)

	entry := manifest.Data[0]
	assert.Equal(t, "N:package:dl-published", entry.NodeId)
	assert.Equal(t, "published-image.ome.tiff", entry.FileName)
	assert.Equal(t, "published-file", entry.PackageName)
	assert.Equal(t, "ome.tiff", entry.FileExtension)
	presignedURL, err := url.Parse(entry.URL)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(presignedURL.Host, "pennsieve-test-publish"))
	assert.Equal(t, "/14/files/published-image.ome.tiff", presignedURL.Path)
	assert.Equal(t, "Pu_BlishedVersionId", presignedURL.Query().Get("versionId"))
	assert.Empty(t, presignedURL.Query().Get("x-amz-request-payer"))
	assert.Contains(t, presignedURL.Query().Get("X-Amz-Credential"), "/us-east-1/s3/")
	// Single-file package: path should be empty (no parents)
	assert.Empty(t, entry.Path)
}

// TestDownloadManifest_NonUSPackage indirectly tests that we are setting the
// region based on bucket name suffix by looking at a query param in the presigned URL.
// Best option without a mockable S3 or presign client.
func TestDownloadManifest_NonUSPackage(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)
	setupExternalBucketConfig(t, nil)

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-non-us"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-40",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.DownloadManifestResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &manifest))

	assert.Equal(t, 1, manifest.Header.Count)
	assert.Equal(t, int64(8192), manifest.Header.Size)
	require.Len(t, manifest.Data, 1)

	entry := manifest.Data[0]
	assert.Equal(t, "N:package:dl-non-us", entry.NodeId)
	assert.Equal(t, "non-us-image.ome.tiff", entry.FileName)
	assert.Equal(t, "non-us-file", entry.PackageName)
	assert.Equal(t, "ome.tiff", entry.FileExtension)
	u, err := url.Parse(entry.URL)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(u.Host, "pennsieve-test-storage-afs1"))
	assert.Contains(t, u.Query().Get("X-Amz-Credential"), "/af-south-1/s3/")
	assert.Equal(t, "/15/files/non-us-image.ome.tiff", u.Path)
	assert.Empty(t, u.Query().Get("versionId"))
	assert.Empty(t, u.Query().Get("x-amz-request-payer"))
	// Single-file package: path should be empty (no parents)
	assert.Empty(t, entry.Path)
}

func TestDownloadManifest_ExternalPublishBucket(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)
	// treat pennsieve-test-publish as an external bucket
	bucketConfig := ExternalBucketConfig(map[string]string{"pennsieve-test-publish": "pennsieve-test-role-arn"})
	setupExternalBucketConfig(t, bucketConfig)

	expectedDuration := 180 * time.Minute
	mockAssumeRoleClient := new(MockAssumeRoleClient)
	mockAssumeRoleClient.On("AssumeRole", mock.Anything, mock.MatchedBy(func(input *sts.AssumeRoleInput) bool {
		return assert.Equal(t, "pennsieve-test-role-arn", *input.RoleArn) &&
			assert.Equal(t, "packages-service-presign-session", *input.RoleSessionName) &&
			assert.Equal(t, int32(expectedDuration.Seconds()), *input.DurationSeconds) &&
			assert.NotNil(t, input.Policy) &&
			assert.Contains(t, *input.Policy, "s3:GetObject") &&
			assert.Contains(t, *input.Policy, "s3:GetObjectVersion") &&
			assert.NotContains(t, *input.Policy, "s3:PutObject")
	}), mock.Anything).Return(&sts.AssumeRoleOutput{
		AssumedRoleUser: &types.AssumedRoleUser{},
		Credentials: &types.Credentials{
			AccessKeyId:     aws.String(uuid.NewString()),
			Expiration:      aws.Time(time.Now().Add(expectedDuration)),
			SecretAccessKey: aws.String(uuid.NewString()),
			SessionToken:    aws.String(uuid.NewString()),
		},
		PackedPolicySize: nil,
		SourceIdentity:   nil,
	}, nil)
	setupAssumeRoleClient(t, mockAssumeRoleClient)

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-published", "N:package:dl-standalone"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-400",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.DownloadManifestResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &manifest))

	// Assume role should only be called once since only one package with external bucket
	mockAssumeRoleClient.AssertNumberOfCalls(t, "AssumeRole", 1)

	assert.Equal(t, 2, manifest.Header.Count)
	assert.Equal(t, int64(8192+8192), manifest.Header.Size)
	require.Len(t, manifest.Data, 2)

	publishedEntryIdx := slices.IndexFunc(manifest.Data, func(entry models.DownloadManifestEntry) bool {
		return entry.NodeId == "N:package:dl-published"
	})
	require.True(t, publishedEntryIdx > -1)

	publishedEntry := manifest.Data[publishedEntryIdx]
	assert.Equal(t, "N:package:dl-published", publishedEntry.NodeId)
	assert.Equal(t, "published-image.ome.tiff", publishedEntry.FileName)
	assert.Equal(t, "published-file", publishedEntry.PackageName)
	assert.Equal(t, "ome.tiff", publishedEntry.FileExtension)
	publishedURL, err := url.Parse(publishedEntry.URL)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(publishedURL.Host, "pennsieve-test-publish"))
	assert.Equal(t, "/14/files/published-image.ome.tiff", publishedURL.Path)
	assert.Equal(t, "Pu_BlishedVersionId", publishedURL.Query().Get("versionId"))
	assert.Equal(t, "requester", publishedURL.Query().Get("x-amz-request-payer"))
	assert.Contains(t, publishedURL.Query().Get("X-Amz-Credential"), "/us-east-1/s3/")
	// Single-file package: path should be empty (no parents)
	assert.Empty(t, publishedEntry.Path)

	standaloneEntryIdx := slices.IndexFunc(manifest.Data, func(entry models.DownloadManifestEntry) bool {
		return entry.NodeId == "N:package:dl-standalone"
	})
	require.True(t, standaloneEntryIdx > -1)
	standaloneEntry := manifest.Data[standaloneEntryIdx]
	assert.Equal(t, "N:package:dl-standalone", standaloneEntry.NodeId)
	assert.Equal(t, "image.ome.tiff", standaloneEntry.FileName)
	assert.Equal(t, "standalone-file", standaloneEntry.PackageName)
	assert.Equal(t, "ome.tiff", standaloneEntry.FileExtension)
	standaloneURL, err := url.Parse(standaloneEntry.URL)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(standaloneURL.Host, "pennsieve-test-storage"))
	assert.Equal(t, "/org2/image.ome.tiff", standaloneURL.Path)
	assert.Empty(t, standaloneURL.Query().Get("versionId"))
	assert.Empty(t, standaloneURL.Query().Get("x-amz-request-payer"))
	assert.Contains(t, standaloneURL.Query().Get("X-Amz-Credential"), "/us-east-1/s3/")
	// Single-file package: path should be empty (no parents)
	assert.Empty(t, standaloneEntry.Path)

}

func TestDownloadManifest_EmptyNodeIds(t *testing.T) {
	setupExternalBucketConfig(t, nil)
	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{}})
	req := newTestRequest("POST", "/download-manifest", "test-req-4",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDownloadManifest_MissingDatasetId(t *testing.T) {
	setupExternalBucketConfig(t, nil)

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-standalone"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-5",
		map[string]string{}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDownloadManifest_Unauthorized(t *testing.T) {
	setupExternalBucketConfig(t, nil)

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-standalone"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-6",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))

	// Viewer doesn't have ViewFiles? Actually Viewer does have ViewFiles.
	// Use a nil DatasetClaim to simulate no dataset access
	claims := &authorizer.Claims{
		OrgClaim: &organization.Claim{IntId: 2},
		UserClaim: &user.Claim{
			Id:     101,
			NodeId: "N:user:test-101",
		},
		DatasetClaim: nil, // no dataset claim = unauthorized
	}
	handler := NewHandler(req, claims).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestDownloadManifest_NonexistentPackage(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)
	setupExternalBucketConfig(t, nil)

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:does-not-exist"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-7",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.DownloadManifestResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &manifest))
	assert.Equal(t, 0, manifest.Header.Count)
	assert.Empty(t, manifest.Data)
}

func TestGetFullExtension(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"image.ome.tiff", "ome.tiff"},
		{"brain.nii.gz", "nii.gz"},
		{"archive.tar.gz", "tar.gz"},
		{"document.pdf", "pdf"},
		{"data.csv", "csv"},
		{"noext", ""},
		{"file.ome.tif", "ome.tif"},
		{"FILE.OME.TIFF", "ome.tiff"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, getFullExtension(tt.input))
		})
	}
}

type MockAssumeRoleClient struct {
	mock.Mock
}

func (m *MockAssumeRoleClient) AssumeRole(ctx context.Context, params *sts.AssumeRoleInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*sts.AssumeRoleOutput), args.Error(1)
}
