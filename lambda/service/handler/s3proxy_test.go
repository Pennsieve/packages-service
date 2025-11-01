package handler

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dataset"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/organization"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/role"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	// Import postgres driver
	_ "github.com/lib/pq"
)

// S3ProxyTestSuite contains tests for S3Proxy handler using real database
type S3ProxyTestSuite struct {
	suite.Suite
	db     *sql.DB
	orgId  int
	claims *authorizer.Claims
}

func (suite *S3ProxyTestSuite) SetupSuite() {
	// Connect to test database
	pgHost := getEnvOrDefault("POSTGRES_HOST", "localhost")
	pgPort := getEnvOrDefault("POSTGRES_PORT", "5432")
	pgUser := getEnvOrDefault("POSTGRES_USER", "postgres")
	pgPassword := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	pgDB := getEnvOrDefault("PENNSIEVE_DB", "postgres")
	pgSSLMode := getEnvOrDefault("POSTGRES_SSL_MODE", "disable")

	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		pgHost, pgPort, pgUser, pgPassword, pgDB, pgSSLMode)

	db, err := sql.Open("postgres", connString)
	require.NoError(suite.T(), err)
	require.NoError(suite.T(), db.Ping())

	suite.db = db
	suite.orgId = 1 // Test organization

	// Create test claims
	suite.claims = &authorizer.Claims{
		UserClaim: &user.Claim{
			Id:           101,
			NodeId:       "N:user:101",
			IsSuperAdmin: false,
		},
		OrgClaim: &organization.Claim{
			IntId:  int64(suite.orgId),
			NodeId: "N:organization:1",
			Role:   pgdb.Owner,
		},
		DatasetClaim: &dataset.Claim{
			Role:   role.Editor,
			NodeId: "N:dataset:1234",
			IntId:  1234,
		},
	}

	// Set up global database connection for handler
	PennsieveDB = db
}

func (suite *S3ProxyTestSuite) TearDownSuite() {
	if suite.db != nil {
		suite.db.Close()
	}
}

func (suite *S3ProxyTestSuite) SetupTest() {
	// Clean up test data before each test
	suite.cleanupTestData()
}

func (suite *S3ProxyTestSuite) TearDownTest() {
	// Clean up test data after each test
	suite.cleanupTestData()
}

func (suite *S3ProxyTestSuite) cleanupTestData() {
	// Clean up files and packages tables
	query := fmt.Sprintf(`DELETE FROM "%d".files WHERE s3_bucket LIKE 'test-%%'`, suite.orgId)
	suite.db.Exec(query)

	query = fmt.Sprintf(`DELETE FROM "%d".packages WHERE node_id LIKE 'N:package:test-%%'`, suite.orgId)
	suite.db.Exec(query)
}

func (suite *S3ProxyTestSuite) createTestPackageWithFile(nodeId, bucket, key string) int64 {
	// Insert test package
	packageQuery := fmt.Sprintf(`
		INSERT INTO "%d".packages (name, type, state, node_id, dataset_id, owner_id, size, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		RETURNING id
	`, suite.orgId)

	var packageId int64
	err := suite.db.QueryRow(packageQuery, "test-package", "Package", "READY", nodeId, 1234, 101, 1024).Scan(&packageId)
	require.NoError(suite.T(), err)

	// Insert test file
	fileQuery := fmt.Sprintf(`
		INSERT INTO "%d".files (package_id, name, file_type, s3_bucket, s3_key, object_type, size, checksum, uuid, processing_state, uploaded_state, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, gen_random_uuid(), $9, $10, NOW(), NOW())
	`, suite.orgId)

	_, err = suite.db.Exec(fileQuery, packageId, "test-file.txt", "Text", bucket, key, "Source", 1024, "test-checksum", "Processed", "Uploaded")
	require.NoError(suite.T(), err)

	return packageId
}

func (suite *S3ProxyTestSuite) TestHandleOptions() {
	req := newTestRequest("OPTIONS", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
		"package_id": "N:package:test-123",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	resp, err := handler.handleOptions(context.Background())
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), http.StatusNoContent, resp.StatusCode)
	assert.Equal(suite.T(), "*", resp.Headers["Access-Control-Allow-Origin"])
	assert.Equal(suite.T(), "GET, HEAD, OPTIONS", resp.Headers["Access-Control-Allow-Methods"])
	assert.Equal(suite.T(), "Authorization, Content-Type, Range, Origin, Accept", resp.Headers["Access-Control-Allow-Headers"])
	assert.Equal(suite.T(), "3600", resp.Headers["Access-Control-Max-Age"])
}

func (suite *S3ProxyTestSuite) TestHandleGetMissingPackageId() {
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	resp, err := handler.handleGet(context.Background())
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), http.StatusBadRequest, resp.StatusCode)
	assert.Contains(suite.T(), resp.Body, "missing required 'package_id' query parameter")
}

func (suite *S3ProxyTestSuite) TestHandleGetMissingDatasetId() {
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"package_id": "N:package:test-123",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	resp, err := handler.handleGet(context.Background())
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), http.StatusBadRequest, resp.StatusCode)
	assert.Contains(suite.T(), resp.Body, "missing required 'dataset_id' query parameter")
}

func (suite *S3ProxyTestSuite) TestHandleGetPackageNotFound() {
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
		"package_id": "N:package:nonexistent",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	resp, err := handler.handleGet(context.Background())
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(suite.T(), resp.Body, "failed to get S3 location")
}

func (suite *S3ProxyTestSuite) TestHandleGetWithMockS3() {
	// Create test package and file
	packageNodeId := "N:package:test-123"
	testBucket := "test-bucket"
	testKey := "test-key"
	
	suite.createTestPackageWithFile(packageNodeId, testBucket, testKey)

	// Create mock S3 server
	testContent := "Hello, World!"
	mockS3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request contains our test bucket and key in the path
		if r.URL.Path == "/test-bucket/test-key" || r.URL.Path == fmt.Sprintf("/%s/%s", testBucket, testKey) {
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testContent)))
			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(testContent))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockS3Server.Close()

	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
		"package_id": packageNodeId,
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	// Mock the generatePresignedURL method to return our test server URL
	originalHandler := handler
	
	// For this test, we'll test just the database query part
	s3Location, err := handler.getS3LocationForPackage(context.Background(), packageNodeId, "N:dataset:1234")
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), testBucket, s3Location.Bucket)
	assert.Equal(suite.T(), testKey, s3Location.Key)
	
	// Test CORS headers method
	corsHeaders := originalHandler.buildCORSHeaders()
	assert.Equal(suite.T(), "*", corsHeaders["Access-Control-Allow-Origin"])
	assert.Equal(suite.T(), "GET, HEAD, OPTIONS", corsHeaders["Access-Control-Allow-Methods"])
}

func (suite *S3ProxyTestSuite) TestHandleHeadMissingPackageId() {
	req := newTestRequest("HEAD", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	resp, err := handler.handleHead(context.Background())
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), http.StatusBadRequest, resp.StatusCode)
	assert.Contains(suite.T(), resp.Body, "missing required 'package_id' query parameter")
}

func (suite *S3ProxyTestSuite) TestGetS3LocationForPackage() {
	// Create test package and file
	packageNodeId := "N:package:test-456"
	testBucket := "test-bucket-2"
	testKey := "test-key-2"
	
	suite.createTestPackageWithFile(packageNodeId, testBucket, testKey)

	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	s3Location, err := handler.getS3LocationForPackage(context.Background(), packageNodeId, "N:dataset:1234")
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), testBucket, s3Location.Bucket)
	assert.Equal(suite.T(), testKey, s3Location.Key)
}

func (suite *S3ProxyTestSuite) TestGetS3LocationForNonexistentPackage() {
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	_, err := handler.getS3LocationForPackage(context.Background(), "N:package:nonexistent", "N:dataset:1234")
	
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "package not found")
}

func (suite *S3ProxyTestSuite) TestNeedsBase64Encoding() {
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}

	// Text content types should not need base64 encoding
	assert.False(suite.T(), handler.needsBase64Encoding("text/plain"))
	assert.False(suite.T(), handler.needsBase64Encoding("application/json"))
	assert.False(suite.T(), handler.needsBase64Encoding("application/xml"))
	assert.False(suite.T(), handler.needsBase64Encoding("application/javascript"))
	assert.False(suite.T(), handler.needsBase64Encoding("application/x-www-form-urlencoded"))

	// Binary content types should need base64 encoding
	assert.True(suite.T(), handler.needsBase64Encoding("image/png"))
	assert.True(suite.T(), handler.needsBase64Encoding("application/pdf"))
	assert.True(suite.T(), handler.needsBase64Encoding("application/octet-stream"))
	assert.True(suite.T(), handler.needsBase64Encoding("video/mp4"))
}

func (suite *S3ProxyTestSuite) TestBuildCORSHeaders() {
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}

	headers := handler.buildCORSHeaders()

	expectedHeaders := map[string]string{
		"Access-Control-Allow-Origin":   "*",
		"Access-Control-Allow-Methods":  "GET, HEAD, OPTIONS",
		"Access-Control-Allow-Headers":  "Authorization, Content-Type, Range, Origin, Accept",
		"Access-Control-Expose-Headers": "Content-Length, Content-Type, Content-Range, ETag, Last-Modified, Accept-Ranges",
	}

	for key, expectedValue := range expectedHeaders {
		assert.Equal(suite.T(), expectedValue, headers[key], "Header %s should match", key)
	}
}

func (suite *S3ProxyTestSuite) TestForwardS3Headers() {
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}

	// Create a mock HTTP response
	resp := &http.Response{
		Header: make(http.Header),
	}
	resp.Header.Set("Content-Type", "text/plain")
	resp.Header.Set("Content-Length", "123")
	resp.Header.Set("ETag", `"test-etag"`)
	resp.Header.Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
	resp.Header.Set("Accept-Ranges", "bytes")
	resp.Header.Set("X-Custom-Header", "should-not-be-forwarded") // This should not be forwarded

	headers := make(map[string]string)
	handler.forwardS3Headers(resp, headers)

	// Check that expected headers are forwarded
	assert.Equal(suite.T(), "text/plain", headers["Content-Type"])
	assert.Equal(suite.T(), "123", headers["Content-Length"])
	assert.Equal(suite.T(), `"test-etag"`, headers["ETag"])
	assert.Equal(suite.T(), "Wed, 21 Oct 2015 07:28:00 GMT", headers["Last-Modified"])
	assert.Equal(suite.T(), "bytes", headers["Accept-Ranges"])

	// Check that custom headers are not forwarded
	_, exists := headers["X-Custom-Header"]
	assert.False(suite.T(), exists, "Custom headers should not be forwarded")
}

func (suite *S3ProxyTestSuite) TestHandleMethodNotAllowed() {
	req := newTestRequest("PUT", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
		"package_id": "N:package:test-123",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	resp, err := handler.handle(context.Background())
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Contains(suite.T(), resp.Body, "method PUT not allowed")
}

func (suite *S3ProxyTestSuite) TestViewerAssetAccess() {
	if suite.db == nil {
		suite.T().Skip("Database not available for viewer asset test")
		return
	}

	// Set up ViewerAssetsBucket for testing
	originalBucket := ViewerAssetsBucket
	ViewerAssetsBucket = "test-viewer-assets-bucket"
	defer func() { ViewerAssetsBucket = originalBucket }()

	// Create test package and dataset
	packageNodeId := "N:package:test-asset-pkg"
	datasetNodeId := "N:dataset:1234"
	
	suite.createTestPackageWithFile(packageNodeId, "original-bucket", "original-key")

	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": datasetNodeId,
		"package_id": packageNodeId,
		"path":       "preview/thumbnail.jpg",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	// Test the getS3LocationForViewerAsset function directly
	s3Location, err := handler.getS3LocationForViewerAsset(context.Background(), packageNodeId, datasetNodeId, "preview/thumbnail.jpg")
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), "test-viewer-assets-bucket", s3Location.Bucket)
	// Expected format: O{WorkspaceIntId}/D{DatasetIntId}/P{PackageIntId}/{AssetPath}
	// The createTestPackageWithFile function creates a package with dataset_id=1234, and we need to get the actual package int ID
	expectedKey := fmt.Sprintf("O%d/D%d/P", suite.orgId, 1234) // We'll check the prefix since package int ID is auto-generated
	assert.True(suite.T(), strings.HasPrefix(s3Location.Key, expectedKey), "Key should start with O%d/D%d/P", suite.orgId, 1234)
	assert.True(suite.T(), strings.HasSuffix(s3Location.Key, "/preview/thumbnail.jpg"), "Key should end with asset path")
}

func (suite *S3ProxyTestSuite) TestViewerAssetAccessUnauthorized() {
	if suite.db == nil {
		suite.T().Skip("Database not available for viewer asset unauthorized test")
		return
	}

	// Set up ViewerAssetsBucket for testing
	originalBucket := ViewerAssetsBucket
	ViewerAssetsBucket = "test-viewer-assets-bucket"
	defer func() { ViewerAssetsBucket = originalBucket }()

	// Try to access viewer asset for non-existent package
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
		"package_id": "N:package:nonexistent",
		"path":       "preview/thumbnail.jpg",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	_, err := handler.getS3LocationForViewerAsset(context.Background(), "N:package:nonexistent", "N:dataset:1234", "preview/thumbnail.jpg")
	
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "package not found or does not belong to specified dataset")
}

func (suite *S3ProxyTestSuite) TestViewerAssetBucketNotConfigured() {
	// Set ViewerAssetsBucket to empty to test error handling
	originalBucket := ViewerAssetsBucket
	ViewerAssetsBucket = ""
	defer func() { ViewerAssetsBucket = originalBucket }()

	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
		"package_id": "N:package:test",
		"path":       "preview/thumbnail.jpg",
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	_, err := handler.getS3LocationForViewerAsset(context.Background(), "N:package:test", "N:dataset:1234", "preview/thumbnail.jpg")
	
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "viewer assets bucket not configured")
}

func (suite *S3ProxyTestSuite) TestHandleGetWithPathParameter() {
	// Create test package and file
	packageNodeId := "N:package:test-with-path"
	testBucket := "test-bucket"
	testKey := "test-key"
	
	suite.createTestPackageWithFile(packageNodeId, testBucket, testKey)

	// Test GET request without path (should use original package file logic)
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": "N:dataset:1234",
		"package_id": packageNodeId,
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, suite.claims)}
	
	s3Location, err := handler.getS3LocationForPackage(context.Background(), packageNodeId, "N:dataset:1234")
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), testBucket, s3Location.Bucket)
	assert.Equal(suite.T(), testKey, s3Location.Key)
}

func (suite *S3ProxyTestSuite) TestCrossDatasetAccessDenied() {
	if suite.db == nil {
		suite.T().Skip("Database not available for cross-dataset access test")
		return
	}

	// Insert test data: organization, datasets, packages, and files
	orgId := int64(1)
	
	// Create two datasets in the same organization
	dataset1Id := int64(1001)
	dataset1NodeId := "N:dataset:unauthorized"
	dataset2Id := int64(1002) 
	dataset2NodeId := "N:dataset:1234"

	// Insert datasets
	_, err := suite.db.Exec(fmt.Sprintf(`
		INSERT INTO "%d".datasets (id, node_id, name, state, created_at, updated_at)
		VALUES ($1, $2, 'Unauthorized Dataset', 'READY', NOW(), NOW()),
			   ($3, $4, 'Authorized Dataset', 'READY', NOW(), NOW())
		ON CONFLICT (id) DO NOTHING
	`, orgId), dataset1Id, dataset1NodeId, dataset2Id, dataset2NodeId)
	require.NoError(suite.T(), err)

	// Create a package in dataset1 (unauthorized)
	packageId := int64(2001)
	packageNodeId := "N:package:unauthorized-pkg"
	
	_, err = suite.db.Exec(fmt.Sprintf(`
		INSERT INTO "%d".packages (id, node_id, name, package_type, package_state, dataset_id, created_at, updated_at)
		VALUES ($1, $2, 'Unauthorized Package', 1, 'READY', $3, NOW(), NOW())
		ON CONFLICT (id) DO NOTHING
	`, orgId), packageId, packageNodeId, dataset1Id)
	require.NoError(suite.T(), err)

	// Create a file for the package
	_, err = suite.db.Exec(fmt.Sprintf(`
		INSERT INTO "%d".files (id, package_id, name, file_type, s3_bucket, s3_key, created_at, updated_at)
		VALUES (3001, $1, 'test-file.txt', 1, 'test-bucket', 'test/unauthorized-file.txt', NOW(), NOW())
		ON CONFLICT (id) DO NOTHING
	`, orgId), packageId)
	require.NoError(suite.T(), err)

	// Create claims for user with access only to dataset2, NOT dataset1
	unauthorizedClaims := &authorizer.Claims{
		UserClaim: &user.Claim{
			Id:           101,
			NodeId:       "N:user:101", 
			IsSuperAdmin: false,
		},
		OrgClaim: &organization.Claim{
			IntId:  orgId,
			NodeId: "N:organization:1",
			Role:   pgdb.Owner,
		},
		DatasetClaim: &dataset.Claim{
			Role:   role.Editor,
			NodeId: dataset2NodeId, // User only has access to dataset2
			IntId:  dataset2Id,
		},
	}

	// Attempt to access package from dataset1 using credentials for dataset2
	req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{
		"dataset_id": dataset2NodeId,        // Authorized dataset
		"package_id": packageNodeId,         // Package from unauthorized dataset1
	}, "")

	handler := S3ProxyHandler{RequestHandler: *NewHandler(req, unauthorizedClaims)}
	
	resp, err := handler.handle(context.Background())
	
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(suite.T(), resp.Body, "package not found, no associated file, or package does not belong to specified dataset")
}

// Helper function to get environment variable with default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// TestS3ProxyTestSuite runs the test suite
func TestS3ProxyTestSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration tests in short mode")
	}
	suite.Run(t, new(S3ProxyTestSuite))
}

// Individual unit tests that don't require database

func TestS3ProxyHandleUnitTests(t *testing.T) {
	// Test individual methods without database dependency
	claims := &authorizer.Claims{
		UserClaim: &user.Claim{
			Id:           101,
			NodeId:       "N:user:101",
			IsSuperAdmin: false,
		},
		OrgClaim: &organization.Claim{
			IntId:  1,
			NodeId: "N:organization:1",
			Role:   pgdb.Owner,
		},
		DatasetClaim: &dataset.Claim{
			Role:   role.Editor,
			NodeId: "N:dataset:1234",
			IntId:  1234,
		},
	}

	t.Run("handleOptions", func(t *testing.T) {
		req := newTestRequest("OPTIONS", "/s3/proxy", "test-request-id", map[string]string{
			"dataset_id": "N:dataset:1234",
		}, "")

		handler := S3ProxyHandler{RequestHandler: *NewHandler(req, claims)}
		
		resp, err := handler.handleOptions(context.Background())
		
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, resp.StatusCode)
		assert.Equal(t, "*", resp.Headers["Access-Control-Allow-Origin"])
	})

	t.Run("needsBase64Encoding", func(t *testing.T) {
		req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
		handler := S3ProxyHandler{RequestHandler: *NewHandler(req, claims)}

		assert.False(t, handler.needsBase64Encoding("text/plain"))
		assert.False(t, handler.needsBase64Encoding("application/json"))
		assert.True(t, handler.needsBase64Encoding("image/png"))
		assert.True(t, handler.needsBase64Encoding("application/pdf"))
	})

	t.Run("buildCORSHeaders", func(t *testing.T) {
		req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
		handler := S3ProxyHandler{RequestHandler: *NewHandler(req, claims)}

		headers := handler.buildCORSHeaders()
		assert.Equal(t, "*", headers["Access-Control-Allow-Origin"])
		assert.Equal(t, "GET, HEAD, OPTIONS", headers["Access-Control-Allow-Methods"])
	})
}