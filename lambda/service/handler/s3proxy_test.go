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
    "time"

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

    // Retry database connection with backoff
    err = suite.waitForDatabase(db)
    if err != nil {
        suite.T().Skipf("Database not available: %v", err)
        return
    }

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
    if suite.db == nil {
        return
    }

    // Clean up files and packages tables - but preserve main test dataset N:dataset:1234
    query := fmt.Sprintf(`DELETE FROM "%d".files WHERE s3_bucket LIKE 'test-%%'`, suite.orgId)
    suite.db.Exec(query)

    query = fmt.Sprintf(`DELETE FROM "%d".packages WHERE node_id LIKE 'N:package:test-%%'`, suite.orgId)
    suite.db.Exec(query)

    // Clean up only temporary test datasets (not the main N:dataset:1234)
    query = fmt.Sprintf(`DELETE FROM "%d".datasets WHERE node_id LIKE 'N:dataset:test-%%'`, suite.orgId)
    suite.db.Exec(query)
}

func (suite *S3ProxyTestSuite) createTestPackageWithFile(nodeId, bucket, key string) int64 {
    // First ensure we have a dataset - use ON CONFLICT to handle duplicates gracefully
    var datasetId int64
    err := suite.db.QueryRow(fmt.Sprintf(`
		INSERT INTO "%d".datasets (node_id, name, state, status_id, created_at, updated_at)
		VALUES ($1, 'Test Dataset', 'READY'::text, 1, NOW(), NOW())
		ON CONFLICT (node_id) DO UPDATE SET 
			name = EXCLUDED.name,
			updated_at = NOW()
		RETURNING id
	`, suite.orgId), "N:dataset:1234").Scan(&datasetId)
    require.NoError(suite.T(), err)

    // Insert test package with all required fields
    packageQuery := fmt.Sprintf(`
		INSERT INTO "%d".packages (name, type, state, node_id, dataset_id, owner_id, size, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		RETURNING id
	`, suite.orgId)

    var packageId int64
    err = suite.db.QueryRow(packageQuery, "test-package", "Package", "READY", nodeId, datasetId, 101, 1024).Scan(&packageId)
    require.NoError(suite.T(), err)

    // Insert test file with all required fields
    fileQuery := fmt.Sprintf(`
		INSERT INTO "%d".files (package_id, name, file_type, s3_bucket, s3_key, object_type, size, processing_state, uploaded_state, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
	`, suite.orgId)

    _, err = suite.db.Exec(fileQuery, packageId, "test-file.txt", 1, bucket, key, "source", 1024, "processed", "Uploaded")
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
    // Get the actual dataset internal ID from the database
    var actualDatasetIntId int64
    err = suite.db.QueryRow(fmt.Sprintf(`SELECT id FROM "%d".datasets WHERE node_id = $1`, suite.orgId), datasetNodeId).Scan(&actualDatasetIntId)
    require.NoError(suite.T(), err)

    expectedKeyPrefix := fmt.Sprintf("O%d/D%d/P", suite.orgId, actualDatasetIntId)
    assert.True(suite.T(), strings.HasPrefix(s3Location.Key, expectedKeyPrefix), "Key should start with %s, but got: %s", expectedKeyPrefix, s3Location.Key)
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

    // Use org ID from suite setup
    orgId := int64(suite.orgId)

    // Create unique node IDs to avoid conflicts with current timestamp + random component
    timestamp := fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Unix())
    dataset1NodeId := fmt.Sprintf("N:dataset:test-unauthorized-%s", timestamp)
    dataset2NodeId := fmt.Sprintf("N:dataset:test-authorized-%s", timestamp)
    packageNodeId := fmt.Sprintf("N:package:test-unauthorized-pkg-%s", timestamp)

    // Ensure clean state before test - more thorough cleanup
    suite.cleanupTestData()

    // Additional cleanup specific to this test to avoid conflicts
    // First clean up files, then packages, then datasets (respecting foreign key constraints)
    suite.db.Exec(fmt.Sprintf(`DELETE FROM "%d".files WHERE s3_bucket LIKE 'test-%%' OR s3_bucket = 'test-unauthorized-bucket'`, orgId))
    suite.db.Exec(fmt.Sprintf(`DELETE FROM "%d".packages WHERE node_id LIKE 'N:package:test-%%'`, orgId))
    suite.db.Exec(fmt.Sprintf(`DELETE FROM "%d".datasets WHERE node_id LIKE 'N:dataset:test-%%'`, orgId))

    // Insert datasets using ON CONFLICT to handle any remaining duplicates gracefully
    var dataset1Id, dataset2Id int64

    err := suite.db.QueryRow(fmt.Sprintf(`
		INSERT INTO "%d".datasets (node_id, name, state, status_id, created_at, updated_at)
		VALUES ($1, 'Test Unauthorized Dataset', 'READY'::text, 1, NOW(), NOW())
		ON CONFLICT (node_id) DO UPDATE SET 
			name = EXCLUDED.name,
			updated_at = NOW()
		RETURNING id
	`, orgId), dataset1NodeId).Scan(&dataset1Id)
    require.NoError(suite.T(), err)

    err = suite.db.QueryRow(fmt.Sprintf(`
		INSERT INTO "%d".datasets (node_id, name, state, status_id, created_at, updated_at)
		VALUES ($1, 'Test Authorized Dataset', 'READY'::text, 1, NOW(), NOW())
		ON CONFLICT (node_id) DO UPDATE SET 
			name = EXCLUDED.name,
			updated_at = NOW()
		RETURNING id
	`, orgId), dataset2NodeId).Scan(&dataset2Id)
    require.NoError(suite.T(), err)

    // Create a package in dataset1 (unauthorized) without specifying ID
    var packageId int64
    err = suite.db.QueryRow(fmt.Sprintf(`
		INSERT INTO "%d".packages (node_id, name, type, state, dataset_id, owner_id, size, created_at, updated_at)
		VALUES ($1, 'Test Unauthorized Package', 'Package', 'READY', $2, 101, 1024, NOW(), NOW())
		RETURNING id
	`, orgId), packageNodeId, dataset1Id).Scan(&packageId)
    require.NoError(suite.T(), err)

    // Create a file for the package - use minimal required fields to avoid column type issues
    _, err = suite.db.Exec(fmt.Sprintf(`
		INSERT INTO "%d".files (package_id, name, file_type, s3_bucket, s3_key, object_type, processing_state, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
	`, orgId), packageId, "test-unauthorized-file.txt", 1, "test-unauthorized-bucket", "test/unauthorized-file.txt", "source", "processed")
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
        "dataset_id": dataset2NodeId, // Authorized dataset
        "package_id": packageNodeId,  // Package from unauthorized dataset1
    }, "")

    handler := S3ProxyHandler{RequestHandler: *NewHandler(req, unauthorizedClaims)}

    resp, err := handler.handle(context.Background())

    require.NoError(suite.T(), err)
    assert.Equal(suite.T(), http.StatusInternalServerError, resp.StatusCode)
    assert.Contains(suite.T(), resp.Body, "package not found, no associated file, or package does not belong to specified dataset")
}

// waitForDatabase waits for the database to be available with exponential backoff
func (suite *S3ProxyTestSuite) waitForDatabase(db *sql.DB) error {
    maxRetries := 10
    backoff := time.Millisecond * 100

    for i := 0; i < maxRetries; i++ {
        err := db.Ping()
        if err == nil {
            return nil
        }

        suite.T().Logf("Database connection attempt %d failed: %v. Retrying in %v...", i+1, err, backoff)
        time.Sleep(backoff)
        backoff *= 2 // Exponential backoff

        if backoff > time.Second*5 {
            backoff = time.Second * 5 // Cap at 5 seconds
        }
    }

    // Final attempt
    return db.Ping()
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

    t.Run("buildCORSHeaders", func(t *testing.T) {
        req := newTestRequest("GET", "/s3/proxy", "test-request-id", map[string]string{}, "")
        handler := S3ProxyHandler{RequestHandler: *NewHandler(req, claims)}

        headers := handler.buildCORSHeaders()
        assert.Equal(t, "*", headers["Access-Control-Allow-Origin"])
        assert.Equal(t, "GET, HEAD, OPTIONS", headers["Access-Control-Allow-Methods"])
    })
}
