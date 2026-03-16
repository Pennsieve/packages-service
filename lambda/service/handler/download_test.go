package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

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
	awsConfig := store.GetTestAWSConfig(t)
	originalS3 := S3Client
	S3Client = s3.NewFromConfig(awsConfig)
	t.Cleanup(func() { S3Client = originalS3 })
}

func TestDownloadManifest_SinglePackage(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)

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
	assert.Contains(t, entry.URL, "pennsieve-test-storage")
	// Single-file package: path should be empty (no parents)
	assert.Empty(t, entry.Path)
}

func TestDownloadManifest_Collection(t *testing.T) {
	setupDownloadTestDB(t)
	setupS3Client(t)

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

	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-published"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-4",
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
	assert.Contains(t, entry.URL, "pennsieve-test-publish")
	assert.Contains(t, entry.URL, "versionId=Pu_BlishedVersionId")
	// Single-file package: path should be empty (no parents)
	assert.Empty(t, entry.Path)
}

func TestDownloadManifest_EmptyNodeIds(t *testing.T) {
	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{}})
	req := newTestRequest("POST", "/download-manifest", "test-req-4",
		map[string]string{"dataset_id": "N:dataset:dl-test"}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDownloadManifest_MissingDatasetId(t *testing.T) {
	body, _ := json.Marshal(models.DownloadRequest{NodeIds: []string{"N:package:dl-standalone"}})
	req := newTestRequest("POST", "/download-manifest", "test-req-5",
		map[string]string{}, string(body))
	handler := NewHandler(req, editorClaims(2, "N:dataset:dl-test")).WithDefaultService()

	resp, err := handler.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDownloadManifest_Unauthorized(t *testing.T) {
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
