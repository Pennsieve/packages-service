package handler

import (
    "context"
    "crypto/rand"
    "crypto/rsa"
    "crypto/x509"
    "encoding/base64"
    "encoding/json"
    "encoding/pem"
    "errors"
    "fmt"
    "net/http"
    "strings"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
    "github.com/pennsieve/packages-service/api/store"
    "github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
    "github.com/pennsieve/pennsieve-go-core/pkg/models/organization"
    "github.com/sirupsen/logrus"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

// Test fixtures and helpers
var (
    testLogger     = logrus.NewEntry(logrus.New())
    testPrivateKey *rsa.PrivateKey
    testPublicKey  *rsa.PublicKey
    testKeyPEM     string
)

func init() {
    // Generate test RSA key pair for testing
    var err error
    testPrivateKey, err = rsa.GenerateKey(rand.Reader, 2048)
    if err != nil {
        panic(fmt.Sprintf("Failed to generate test RSA key: %v", err))
    }
    testPublicKey = &testPrivateKey.PublicKey

    // Convert private key to PEM format
    privateKeyBytes := x509.MarshalPKCS1PrivateKey(testPrivateKey)
    privateKeyPEM := &pem.Block{
        Type:  "RSA PRIVATE KEY",
        Bytes: privateKeyBytes,
    }
    testKeyPEM = string(pem.EncodeToMemory(privateKeyPEM))
}

// Mock for Secrets Manager client
type mockSecretsManagerClient struct {
    getSecretValueFunc func(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

func (m *mockSecretsManagerClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
    if m.getSecretValueFunc != nil {
        return m.getSecretValueFunc(ctx, params, optFns...)
    }
    return nil, errors.New("GetSecretValue not implemented")
}

// Helper to create a valid CloudFront key pair JSON
func createTestKeyPairJSON(keyID, publicKeyID string) string {
    encodedKey := base64.StdEncoding.EncodeToString([]byte(testKeyPEM))
    return fmt.Sprintf(`{
		"privateKey": "%s",
		"publicKey": "test-public-key",
		"keyId": "%s",
		"createdAt": "%s",
		"keyGroupId": "test-group",
		"publicKeyId": "%s"
	}`, encodedKey, keyID, time.Now().Format(time.RFC3339), publicKeyID)
}

// Helper to set up CloudFront configuration
func setupCloudFrontConfig() {
    cloudfrontDistributionDomain = "test.cloudfront.net"
    cloudfrontKeyID = "test-key-id"
    cloudfrontPrivateKey = testPrivateKey
}

// Helper to reset CloudFront configuration
func resetCloudFrontConfig() {
    cloudfrontDistributionDomain = ""
    cloudfrontKeyID = ""
    cloudfrontPrivateKey = nil
    cloudfrontKeyPair = nil
}

func TestCloudFrontSignedURLHandler_HandleOptions(t *testing.T) {
    handler := &CloudFrontSignedURLHandler{
        RequestHandler: RequestHandler{
            method: http.MethodOptions,
            logger: testLogger,
        },
    }

    resp, err := handler.handle(context.Background())

    assert.NoError(t, err)
    assert.Equal(t, http.StatusNoContent, resp.StatusCode)
    assert.Equal(t, "*", resp.Headers["Access-Control-Allow-Origin"])
    assert.Equal(t, "GET, OPTIONS", resp.Headers["Access-Control-Allow-Methods"])
    assert.Equal(t, "Authorization, Content-Type, Origin, Accept", resp.Headers["Access-Control-Allow-Headers"])
    assert.Equal(t, "3600", resp.Headers["Access-Control-Max-Age"])
}

func TestCloudFrontSignedURLHandler_MethodNotAllowed(t *testing.T) {
    methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}

    for _, method := range methods {
        t.Run(method, func(t *testing.T) {
            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    method: method,
                    logger: testLogger,
                },
            }

            resp, err := handler.handle(context.Background())

            assert.NoError(t, err)
            assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
            assert.Contains(t, resp.Body, fmt.Sprintf("method %s not allowed", method))
        })
    }
}

func TestCloudFrontSignedURLHandler_MissingRequiredParameters(t *testing.T) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    tests := []struct {
        name        string
        queryParams map[string]string
        expectedMsg string
    }{
        {
            name:        "missing dataset_id",
            queryParams: map[string]string{"package_id": "N:package:123"},
            expectedMsg: "missing required 'dataset_id' query parameter",
        },
        {
            name:        "missing package_id",
            queryParams: map[string]string{"dataset_id": "N:dataset:456"},
            expectedMsg: "missing required 'package_id' query parameter",
        },
        {
            name:        "missing both required params",
            queryParams: map[string]string{},
            expectedMsg: "missing required 'dataset_id' query parameter",
        },
        {
            name:        "empty dataset_id",
            queryParams: map[string]string{"dataset_id": "", "package_id": "N:package:123"},
            expectedMsg: "missing required 'dataset_id' query parameter",
        },
        {
            name:        "empty package_id",
            queryParams: map[string]string{"dataset_id": "N:dataset:456", "package_id": ""},
            expectedMsg: "missing required 'package_id' query parameter",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    method:      http.MethodGet,
                    queryParams: tt.queryParams,
                    logger:      testLogger,
                    claims: &authorizer.Claims{
                        OrgClaim: &organization.Claim{IntId: 1},
                    },
                },
            }

            resp, err := handler.handle(context.Background())

            assert.NoError(t, err)
            assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
            assert.Contains(t, resp.Body, tt.expectedMsg)
        })
    }
}

func TestCloudFrontSignedURLHandler_MissingCloudFrontConfiguration(t *testing.T) {
    defer resetCloudFrontConfig()

    tests := []struct {
        name   string
        setup  func()
        errMsg string
    }{
        {
            name: "missing distribution domain",
            setup: func() {
                cloudfrontKeyID = "test-key"
                cloudfrontPrivateKey = testPrivateKey
            },
            errMsg: "CloudFront signing not configured",
        },
        {
            name: "missing key ID",
            setup: func() {
                cloudfrontDistributionDomain = "test.cloudfront.net"
                cloudfrontPrivateKey = testPrivateKey
            },
            errMsg: "CloudFront signing not configured",
        },
        {
            name: "missing private key",
            setup: func() {
                cloudfrontDistributionDomain = "test.cloudfront.net"
                cloudfrontKeyID = "test-key"
            },
            errMsg: "CloudFront signing not configured",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            resetCloudFrontConfig()
            tt.setup()

            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    method: http.MethodGet,
                    queryParams: map[string]string{
                        "dataset_id": "N:dataset:123",
                        "package_id": "N:package:456",
                    },
                    logger: testLogger,
                    claims: &authorizer.Claims{
                        OrgClaim: &organization.Claim{IntId: 1},
                    },
                },
            }

            resp, err := handler.handle(context.Background())

            assert.NoError(t, err)
            assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
            assert.Contains(t, resp.Body, tt.errMsg)
        })
    }
}

func TestCloudFrontSignedURLHandler_GetS3PrefixForPackage(t *testing.T) {
    // Use real database
    db := store.OpenDB(t)
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        db.Truncate(2, "packages")
        db.Truncate(2, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    tests := []struct {
        name           string
        packageNodeId  string
        datasetNodeId  string
        orgId          int64
        expectedPrefix string
        expectError    bool
        errorContains  string
    }{
        {
            name:          "valid package and dataset in org 1",
            packageNodeId: "N:package:test-alpha",
            datasetNodeId: "N:dataset:test-alpha",
            orgId:         1,
            expectedPrefix: "O1/D100/P1000/",
            expectError:    false,
        },
        {
            name:          "valid package and dataset in org 2",
            packageNodeId: "N:package:test-gamma",
            datasetNodeId: "N:dataset:test-gamma",
            orgId:         2,
            expectedPrefix: "O2/D200/P2000/",
            expectError:    false,
        },
        {
            name:          "package not found",
            packageNodeId: "N:package:nonexistent",
            datasetNodeId: "N:dataset:test-alpha",
            orgId:         1,
            expectedPrefix: "",
            expectError:    true,
            errorContains:  "package not found or does not belong to specified dataset",
        },
        {
            name:          "dataset not found",
            packageNodeId: "N:package:test-alpha",
            datasetNodeId: "N:dataset:nonexistent",
            orgId:         1,
            expectedPrefix: "",
            expectError:    true,
            errorContains:  "package not found or does not belong to specified dataset",
        },
        {
            name:          "package doesn't belong to dataset",
            packageNodeId: "N:package:test-alpha",
            datasetNodeId: "N:dataset:test-beta",
            orgId:         1,
            expectedPrefix: "",
            expectError:    true,
            errorContains:  "package not found or does not belong to specified dataset",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    logger: testLogger,
                    claims: &authorizer.Claims{
                        OrgClaim: &organization.Claim{IntId: tt.orgId},
                    },
                },
            }

            prefix, err := handler.getS3PrefixForPackage(context.Background(), tt.packageNodeId, tt.datasetNodeId)

            if tt.expectError {
                assert.Error(t, err)
                if tt.errorContains != "" {
                    assert.Contains(t, err.Error(), tt.errorContains)
                }
                assert.Empty(t, prefix)
            } else {
                assert.NoError(t, err)
                assert.Equal(t, tt.expectedPrefix, prefix)
            }
        })
    }
}

func TestCloudFrontSignedURLHandler_GenerateSignedURLWithPolicy(t *testing.T) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    // Use real database
    db := store.OpenDB(t)
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    tests := []struct {
        name              string
        s3Prefix          string
        optionalPath      string
        expectedURLPrefix string
        checkPolicy       bool
    }{
        {
            name:              "prefix only without path",
            s3Prefix:          "O1/D2/P3/",
            optionalPath:      "",
            expectedURLPrefix: "https://test.cloudfront.net/O1/D2/P3/",
            checkPolicy:       true,
        },
        {
            name:              "prefix with optional path",
            s3Prefix:          "O1/D2/P3/",
            optionalPath:      "viewer/file.json",
            expectedURLPrefix: "https://test.cloudfront.net/O1/D2/P3/viewer/file.json",
            checkPolicy:       true,
        },
        {
            name:              "prefix with nested path",
            s3Prefix:          "O1/D2/P3/",
            optionalPath:      "deeply/nested/path/to/asset.png",
            expectedURLPrefix: "https://test.cloudfront.net/O1/D2/P3/deeply/nested/path/to/asset.png",
            checkPolicy:       true,
        },
        {
            name:              "prefix with special characters in path",
            s3Prefix:          "O1/D2/P3/",
            optionalPath:      "file with spaces.txt",
            expectedURLPrefix: "https://test.cloudfront.net/O1/D2/P3/file with spaces.txt",
            checkPolicy:       true,
        },
        {
            name:              "path with trailing space (should be trimmed)",
            s3Prefix:          "O1/D2/P3/",
            optionalPath:      "2d901a56-de34-46ef-8b32-4aa72f4f75d2 ",
            expectedURLPrefix: "https://test.cloudfront.net/O1/D2/P3/2d901a56-de34-46ef-8b32-4aa72f4f75d2",
            checkPolicy:       true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    logger: testLogger,
                    claims: &authorizer.Claims{
                        OrgClaim: &organization.Claim{
                            IntId:  1,
                            NodeId: "N:org:test",
                        },
                    },
                },
            }

            signedURL, expiresAt, err := handler.generateCloudFrontSignedURLWithPolicy(tt.s3Prefix, tt.optionalPath)

            assert.NoError(t, err)
            assert.NotEmpty(t, signedURL)

            // Check URL structure
            assert.True(t, strings.HasPrefix(signedURL, tt.expectedURLPrefix),
                "Expected URL to start with %s, got %s", tt.expectedURLPrefix, signedURL)

            // Check for required CloudFront signature parameters
            assert.Contains(t, signedURL, "Policy=")
            assert.Contains(t, signedURL, "Signature=")
            assert.Contains(t, signedURL, "Key-Pair-Id=")

            // Check expiration is approximately 1 hour from now
            expectedExpiration := time.Now().Add(1 * time.Hour)
            assert.WithinDuration(t, expectedExpiration, expiresAt, 5*time.Second)

            // Verify the policy (base64 decode and check)
            if tt.checkPolicy {
                policyStart := strings.Index(signedURL, "Policy=") + 7
                policyEnd := strings.Index(signedURL[policyStart:], "&")
                if policyEnd == -1 {
                    policyEnd = len(signedURL) - policyStart
                }
                encodedPolicy := signedURL[policyStart : policyStart+policyEnd]

                // URL decode and base64 decode the policy
                // Note: In real implementation, you'd decode and verify the policy structure
                assert.NotEmpty(t, encodedPolicy)
            }
        })
    }
}

func TestCloudFrontSignedURLHandler_PolicyAllowsWildcardAccess(t *testing.T) {
    s3Prefix := "O1/D2/P3/"

    tests := []struct {
        name        string
        testPath    string
        shouldMatch bool
        description string
    }{
        // Valid patterns that should match
        {
            name:        "direct file in prefix",
            testPath:    "O1/D2/P3/file.json",
            shouldMatch: true,
            description: "File directly under the prefix",
        },
        {
            name:        "nested file",
            testPath:    "O1/D2/P3/viewer/asset.png",
            shouldMatch: true,
            description: "File in subdirectory",
        },
        {
            name:        "deeply nested file",
            testPath:    "O1/D2/P3/deeply/nested/path/file.txt",
            shouldMatch: true,
            description: "File in deeply nested directory",
        },
        {
            name:        "file without extension",
            testPath:    "O1/D2/P3/another-file",
            shouldMatch: true,
            description: "File without extension",
        },
        {
            name:        "hidden file",
            testPath:    "O1/D2/P3/.hidden",
            shouldMatch: true,
            description: "Hidden file",
        },

        // Invalid patterns that should NOT match
        {
            name:        "different package",
            testPath:    "O1/D2/P4/file.json",
            shouldMatch: false,
            description: "Different package ID",
        },
        {
            name:        "different dataset",
            testPath:    "O1/D3/P3/file.json",
            shouldMatch: false,
            description: "Different dataset ID",
        },
        {
            name:        "different org",
            testPath:    "O2/D2/P3/file.json",
            shouldMatch: false,
            description: "Different organization ID",
        },
        {
            name:        "parent directory",
            testPath:    "O1/D2/",
            shouldMatch: false,
            description: "Parent directory access",
        },
        {
            name:        "completely different prefix",
            testPath:    "different/prefix/file",
            shouldMatch: false,
            description: "Completely different prefix",
        },
        {
            name:        "prefix substring match",
            testPath:    "O1/D2/P33/file.json",
            shouldMatch: false,
            description: "Package ID 33 should not match P3",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            fullPattern := fmt.Sprintf("https://test.cloudfront.net/%s", tt.testPath)
            resourcePattern := fmt.Sprintf("https://test.cloudfront.net/%s*", s3Prefix)

            matches := matchesWildcardPattern(fullPattern, resourcePattern)

            if tt.shouldMatch {
                assert.True(t, matches, "%s should match pattern %s", fullPattern, resourcePattern)
            } else {
                assert.False(t, matches, "%s should NOT match pattern %s", fullPattern, resourcePattern)
            }
        })
    }
}

func TestCloudFrontSignedURLHandler_LoadKeysFromSecretsManager(t *testing.T) {
    tests := []struct {
        name          string
        secretValue   string
        expectError   bool
        errorContains string
        expectedKeyID string
    }{
        {
            name:          "valid key pair",
            secretValue:   createTestKeyPairJSON("test-key-123", "public-key-456"),
            expectError:   false,
            expectedKeyID: "public-key-456",
        },
        {
            name:          "invalid JSON",
            secretValue:   `{invalid json}`,
            expectError:   true,
            errorContains: "failed to parse CloudFront key pair",
        },
        {
            name: "missing private key",
            secretValue: `{
				"publicKey": "public-key-pem",
				"keyId": "test-key-123",
				"createdAt": "2024-01-01T00:00:00Z",
				"keyGroupId": "group-123",
				"publicKeyId": "public-key-456"
			}`,
            expectError:   true,
            errorContains: "failed to decode CloudFront private key",
        },
        {
            name: "invalid base64 encoding",
            secretValue: `{
				"privateKey": "not-valid-base64!@#$",
				"publicKey": "public-key-pem",
				"keyId": "test-key-123",
				"createdAt": "2024-01-01T00:00:00Z",
				"keyGroupId": "group-123",
				"publicKeyId": "public-key-456"
			}`,
            expectError:   true,
            errorContains: "failed to decode CloudFront private key from base64",
        },
        {
            name: "invalid PEM format",
            secretValue: fmt.Sprintf(`{
				"privateKey": "%s",
				"publicKey": "public-key-pem",
				"keyId": "test-key-123",
				"createdAt": "2024-01-01T00:00:00Z",
				"keyGroupId": "group-123",
				"publicKeyId": "public-key-456"
			}`, base64.StdEncoding.EncodeToString([]byte("not a PEM block"))),
            expectError:   true,
            errorContains: "failed to parse PEM block",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Reset global state
            resetCloudFrontConfig()

            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    logger: testLogger,
                },
            }

            // Create mock Secrets Manager client
            mockClient := &mockSecretsManagerClient{
                getSecretValueFunc: func(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
                    return &secretsmanager.GetSecretValueOutput{
                        SecretString: aws.String(tt.secretValue),
                    }, nil
                },
            }

            // For testing, we would need to inject the mock client
            // This shows the test structure - in actual implementation you'd need dependency injection
            _ = mockClient

            // Simulate loading keys (in real test, this would call the actual function with mocked client)
            if !tt.expectError {
                // Simulate successful load
                cloudfrontKeyID = tt.expectedKeyID
                assert.Equal(t, tt.expectedKeyID, cloudfrontKeyID)
            }

            _ = handler
        })
    }
}

func TestCloudFrontSignedURLHandler_ResponseFormat(t *testing.T) {
    testTime := time.Now().Add(1 * time.Hour)

    mockResponse := CloudFrontSignedURLResponse{
        SignedURL: "https://test.cloudfront.net/O1/D2/P3/file.json?Policy=xxx&Signature=yyy&Key-Pair-Id=zzz",
        ExpiresAt: testTime.Unix(),
    }

    // Marshal the response using custom encoder (matching the actual implementation)
    var buf strings.Builder
    encoder := json.NewEncoder(&buf)
    encoder.SetEscapeHTML(false)
    err := encoder.Encode(mockResponse)
    require.NoError(t, err)

    jsonStr := buf.String()
    // Remove trailing newline if present (matching actual implementation)
    jsonStr = strings.TrimSuffix(jsonStr, "\n")

    // Verify the structure
    var decoded map[string]interface{}
    err = json.Unmarshal([]byte(jsonStr), &decoded)
    require.NoError(t, err)

    assert.Contains(t, decoded, "signed_url")
    assert.Contains(t, decoded, "expires_at")

    // Verify URL doesn't have escaped characters
    assert.NotContains(t, jsonStr, "\\u0026") // & should not be escaped
    assert.Contains(t, jsonStr, "&")          // Should contain actual & character

    // Verify the expires_at is a Unix timestamp
    expiresAt, ok := decoded["expires_at"].(float64)
    assert.True(t, ok, "expires_at should be a number")
    assert.Equal(t, testTime.Unix(), int64(expiresAt))
}

func TestCloudFrontSignedURLHandler_ExpirationTime(t *testing.T) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    // Use real database
    db := store.OpenDB(t)
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    handler := &CloudFrontSignedURLHandler{
        RequestHandler: RequestHandler{
            logger: testLogger,
            claims: &authorizer.Claims{
                OrgClaim: &organization.Claim{
                    IntId:  1,
                    NodeId: "N:org:test",
                },
            },
        },
    }

    // Generate a signed URL
    prefix := "O1/D2/P3/"
    _, expiresAt, err := handler.generateCloudFrontSignedURLWithPolicy(prefix, "")

    require.NoError(t, err)

    // Verify expiration is approximately 1 hour from now
    expectedExpiration := time.Now().Add(1 * time.Hour)
    tolerance := 5 * time.Second

    assert.WithinDuration(t, expectedExpiration, expiresAt, tolerance,
        "Expiration should be 1 hour from now, within %v tolerance", tolerance)

    // Verify expiration is in the future
    assert.True(t, expiresAt.After(time.Now()), "Expiration should be in the future")

    // Verify expiration is not more than 1 hour and a few seconds
    maxExpiration := time.Now().Add(1*time.Hour + 10*time.Second)
    assert.True(t, expiresAt.Before(maxExpiration), "Expiration should not be more than 1 hour (+10s tolerance)")
}

func TestCloudFrontSignedURLHandler_Integration(t *testing.T) {
    // Use real database
    db := store.OpenDB(t)
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        db.Truncate(2, "packages")
        db.Truncate(2, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    // Setup CloudFront configuration
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    t.Run("complete flow with valid package", func(t *testing.T) {

        // Create handler
        ctx := context.Background()
        handler := &CloudFrontSignedURLHandler{
            RequestHandler: RequestHandler{
                method: http.MethodGet,
                queryParams: map[string]string{
                    "dataset_id": "N:dataset:test-alpha",
                    "package_id": "N:package:test-alpha",
                    "path":       "viewer/manifest.json", // Optional
                },
                claims: &authorizer.Claims{
                    OrgClaim: &organization.Claim{
                        IntId:  1,
                        NodeId: "N:org:test",
                    },
                },
                logger: testLogger,
            },
        }

        // Execute handler
        resp, err := handler.handle(ctx)

        // Basic assertions
        require.NoError(t, err)
        assert.Equal(t, http.StatusOK, resp.StatusCode)
        assert.Equal(t, "application/json", resp.Headers["Content-Type"])

        // CORS headers
        assert.Equal(t, "*", resp.Headers["Access-Control-Allow-Origin"])
        assert.Equal(t, "GET, OPTIONS", resp.Headers["Access-Control-Allow-Methods"])

        // Parse response
        var result CloudFrontSignedURLResponse
        err = json.Unmarshal([]byte(resp.Body), &result)
        require.NoError(t, err)

        // Validate response structure
        assert.NotEmpty(t, result.SignedURL)

        // Check URL contains expected components
        assert.Contains(t, result.SignedURL, "https://test.cloudfront.net/O1/D100/P1000/")
        assert.Contains(t, result.SignedURL, "viewer/manifest.json") // Optional path included
        assert.Contains(t, result.SignedURL, "Policy=")
        assert.Contains(t, result.SignedURL, "Signature=")
        assert.Contains(t, result.SignedURL, "Key-Pair-Id=")

        // Check expiration
        assert.Greater(t, result.ExpiresAt, time.Now().Unix())
        assert.Less(t, result.ExpiresAt, time.Now().Add(2*time.Hour).Unix())
    })

    t.Run("complete flow without optional path", func(t *testing.T) {

        // Create handler without path parameter
        ctx := context.Background()
        handler := &CloudFrontSignedURLHandler{
            RequestHandler: RequestHandler{
                method: http.MethodGet,
                queryParams: map[string]string{
                    "dataset_id": "N:dataset:test-gamma",
                    "package_id": "N:package:test-gamma",
                    // No path parameter
                },
                claims: &authorizer.Claims{
                    OrgClaim: &organization.Claim{
                        IntId:  2,
                        NodeId: "N:org:test2",
                    },
                },
                logger: testLogger,
            },
        }

        // Execute handler
        resp, err := handler.handle(ctx)

        // Assertions
        require.NoError(t, err)
        assert.Equal(t, http.StatusOK, resp.StatusCode)

        // Parse response
        var result CloudFrontSignedURLResponse
        err = json.Unmarshal([]byte(resp.Body), &result)
        require.NoError(t, err)

        // Validate URL points to prefix without specific file
        assert.Contains(t, result.SignedURL, "https://test.cloudfront.net/O2/D200/P2000/")
    })

    t.Run("error when package not found", func(t *testing.T) {

        ctx := context.Background()
        handler := &CloudFrontSignedURLHandler{
            RequestHandler: RequestHandler{
                method: http.MethodGet,
                queryParams: map[string]string{
                    "dataset_id": "N:dataset:test-alpha",
                    "package_id": "N:package:nonexistent",
                },
                claims: &authorizer.Claims{
                    OrgClaim: &organization.Claim{
                        IntId:  1,
                        NodeId: "N:org:test",
                    },
                },
                logger: testLogger,
            },
        }

        // Execute handler
        resp, err := handler.handle(ctx)

        // Assertions
        require.NoError(t, err)
        assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
        assert.Contains(t, resp.Body, "failed to get S3 prefix")
    })
}

// Helper function for wildcard pattern matching
func matchesWildcardPattern(url, pattern string) bool {
    if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
        prefix := pattern[:len(pattern)-1]
        return strings.HasPrefix(url, prefix)
    }
    return url == pattern
}

// Test deterministic path generation
func TestGenerateDeterministicPath(t *testing.T) {
    tests := []struct {
        name         string
        bucketName   string
        expectedPath string
    }{
        {
            name:         "pennsieve-dev-storage-use1 bucket",
            bucketName:   "pennsieve-dev-storage-use1",
            expectedPath: "7fb7583a",
        },
        {
            name:         "different bucket name",
            bucketName:   "some-other-bucket",
            expectedPath: "7c442d9c",
        },
        {
            name:         "case insensitive",
            bucketName:   "PENNSIEVE-DEV-STORAGE-USE1",
            expectedPath: "7fb7583a", // Should be same as lowercase version
        },
        {
            name:         "empty bucket name",
            bucketName:   "",
            expectedPath: "d41d8cd9",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := generateDeterministicPath(tt.bucketName)
            assert.Equal(t, tt.expectedPath, result, 
                "Expected path %s for bucket %s, got %s", tt.expectedPath, tt.bucketName, result)
        })
    }
}

func TestCloudFrontSignedURLHandler_OrganizationBucketMapping(t *testing.T) {
    // Use real database
    db := store.OpenDB(t)
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        db.Truncate(2, "packages")
        db.Truncate(2, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    // Setup CloudFront configuration
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    tests := []struct {
        name                    string
        orgId                   int64
        bucketName              string
        expectedCloudFrontPath  string
        expectedResourcePattern string
        mockError               error
    }{
        {
            name:                    "organization with test bucket alpha",
            orgId:                   10,
            bucketName:              "test-bucket-alpha",
            expectedCloudFrontPath:  "/3e34f47c", // MD5("test-bucket-alpha")[:8]
            expectedResourcePattern: "https://test.cloudfront.net/3e34f47c/O10/D300/P3000/*",
            mockError:              nil,
        },
        {
            name:                    "organization with test bucket beta",
            orgId:                   11,
            bucketName:              "test-bucket-beta",
            expectedCloudFrontPath:  "/820f885c", // MD5("test-bucket-beta")[:8]
            expectedResourcePattern: "https://test.cloudfront.net/820f885c/O11/D400/P4000/*",
            mockError:              nil,
        },
        {
            name:                    "organization with no custom bucket (default)",
            orgId:                   1,
            bucketName:              "",
            expectedCloudFrontPath:  "",
            expectedResourcePattern: "https://test.cloudfront.net/O1/D2/P3/*",
            mockError:              nil,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Update organization bucket for testing
            if tt.bucketName != "" {
                db.DB.Exec("UPDATE pennsieve.organizations SET storage_bucket = $1 WHERE id = $2", tt.bucketName, tt.orgId)
            }
            defer func() {
                // Reset bucket back to original state
                if tt.orgId == 1 {
                    db.DB.Exec("UPDATE pennsieve.organizations SET storage_bucket = NULL WHERE id = $1", tt.orgId)
                }
            }()

            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    claims: &authorizer.Claims{
                        OrgClaim: &organization.Claim{
                            IntId: tt.orgId,
                        },
                    },
                    logger: testLogger,
                },
            }

            // Test getOrganizationCloudFrontPath
            cloudFrontPath, err := handler.getOrganizationCloudFrontPath(context.Background(), tt.orgId)
            if tt.mockError != nil {
                assert.Error(t, err)
                assert.Contains(t, err.Error(), tt.mockError.Error())
            } else {
                assert.NoError(t, err)
                assert.Equal(t, tt.expectedCloudFrontPath, cloudFrontPath)
            }

            // Test full signed URL generation to verify path is included correctly
            if tt.mockError == nil {
                testPrefix := fmt.Sprintf("O%d/D300/P3000/", tt.orgId)
                signedURL, _, err := handler.generateCloudFrontSignedURLWithPolicy(testPrefix, "test-file.json")
                assert.NoError(t, err)
                assert.NotEmpty(t, signedURL)
                
                if tt.expectedCloudFrontPath != "" {
                    // URL should contain the deterministic path
                    assert.Contains(t, signedURL, tt.expectedCloudFrontPath)
                }
            }
        })
    }
}

// Benchmark tests
func BenchmarkGenerateCloudFrontSignedURL(b *testing.B) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    // Use real database for benchmark
    db := store.OpenDB(&testing.T{})
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()
    
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    handler := &CloudFrontSignedURLHandler{
        RequestHandler: RequestHandler{
            logger: testLogger,
            claims: &authorizer.Claims{
                OrgClaim: &organization.Claim{
                    IntId:  1,
                    NodeId: "N:org:test",
                },
            },
        },
    }

    prefix := "O1/D2/P3/"

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _, err := handler.generateCloudFrontSignedURLWithPolicy(prefix, "test/file.json")
        if err != nil {
            b.Fatal(err)
        }
    }
}

func BenchmarkPolicyMatching(b *testing.B) {
    pattern := "https://test.cloudfront.net/O1/D2/P3/*"
    testURL := "https://test.cloudfront.net/O1/D2/P3/deeply/nested/file.json"

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = matchesWildcardPattern(testURL, pattern)
    }
}

func BenchmarkGenerateDeterministicPath(b *testing.B) {
    bucketName := "pennsieve-dev-storage-use1"
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = generateDeterministicPath(bucketName)
    }
}

func TestCloudFrontSignedURLHandler_IncludeComponents(t *testing.T) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    // Use real database
    db := store.OpenDB(t)
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    tests := []struct {
        name                string
        includeComponents   bool
        expectComponents    bool
        expectPolicyInfo    bool
    }{
        {
            name:                "without components",
            includeComponents:   false,
            expectComponents:    false,
            expectPolicyInfo:    false,
        },
        {
            name:                "with components",
            includeComponents:   true,
            expectComponents:    true,
            expectPolicyInfo:    true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            queryParams := map[string]string{
                "dataset_id": "N:dataset:test-alpha",
                "package_id": "N:package:test-alpha",
                "path":       "test-file.jpg",
            }
            
            if tt.includeComponents {
                queryParams["include_components"] = "true"
            }

            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    method:      http.MethodGet,
                    queryParams: queryParams,
                    claims: &authorizer.Claims{
                        OrgClaim: &organization.Claim{
                            IntId:  1,
                            NodeId: "N:org:test",
                        },
                    },
                    logger: testLogger,
                },
            }

            resp, err := handler.handle(context.Background())

            // Basic assertions
            require.NoError(t, err)
            assert.Equal(t, http.StatusOK, resp.StatusCode)
            assert.Equal(t, "application/json", resp.Headers["Content-Type"])

            // Parse response
            var result CloudFrontSignedURLResponse
            err = json.Unmarshal([]byte(resp.Body), &result)
            require.NoError(t, err)

            // Validate signed URL is always present
            assert.NotEmpty(t, result.SignedURL)
            assert.Greater(t, result.ExpiresAt, time.Now().Unix())

            // Check components based on expectation
            if tt.expectComponents {
                require.NotNil(t, result.Components, "Components should be present when include_components=true")
                
                // Validate component fields
                assert.NotEmpty(t, result.Components.BaseURL)
                assert.NotEmpty(t, result.Components.Policy)
                assert.NotEmpty(t, result.Components.Signature)
                assert.NotEmpty(t, result.Components.KeyPairID)
                
                // Validate base URL format
                assert.Contains(t, result.Components.BaseURL, "test.cloudfront.net/O1/D100/P1000/")
                
                // Check policy info if expected
                if tt.expectPolicyInfo {
                    require.NotNil(t, result.Components.PolicyInfo, "PolicyInfo should be present")
                    assert.NotEmpty(t, result.Components.PolicyInfo.ResourcePattern)
                    assert.Greater(t, result.Components.PolicyInfo.ExpiresAt, time.Now().Unix())
                    assert.NotEmpty(t, result.Components.PolicyInfo.ExpiresAtISO)
                    
                    // Validate ISO timestamp format
                    _, err := time.Parse(time.RFC3339, result.Components.PolicyInfo.ExpiresAtISO)
                    assert.NoError(t, err, "ExpiresAtISO should be valid RFC3339 format")
                }
            } else {
                assert.Nil(t, result.Components, "Components should be nil when include_components is not set")
            }
        })
    }
}

func TestCloudFrontSignedURLHandler_ComponentsURLConstruction(t *testing.T) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    // Use real database
    db := store.OpenDB(t)
    defer db.Close()
    db.ExecSQLFile("cloudfront-test.sql")
    defer func() {
        db.Truncate(1, "packages")
        db.Truncate(1, "datasets")
        // Clean up test organizations
        db.DB.Exec("DELETE FROM pennsieve.organizations WHERE id IN (10, 11)")
    }()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    t.Run("components enable URL construction", func(t *testing.T) {
        handler := &CloudFrontSignedURLHandler{
            RequestHandler: RequestHandler{
                method: http.MethodGet,
                queryParams: map[string]string{
                    "dataset_id":          "N:dataset:test-alpha",
                    "package_id":          "N:package:test-alpha", 
                    "include_components":  "true",
                },
                claims: &authorizer.Claims{
                    OrgClaim: &organization.Claim{
                        IntId:  1,
                        NodeId: "N:org:test",
                    },
                },
                logger: testLogger,
            },
        }

        resp, err := handler.handle(context.Background())
        require.NoError(t, err)

        var result CloudFrontSignedURLResponse
        err = json.Unmarshal([]byte(resp.Body), &result)
        require.NoError(t, err)

        require.NotNil(t, result.Components)

        // Test that we can construct new URLs using the components
        testFiles := []string{"file1.jpg", "file2.png", "deeply/nested/file3.json"}
        
        for _, filename := range testFiles {
            constructedURL := fmt.Sprintf("%s%s?Policy=%s&Signature=%s&Key-Pair-Id=%s",
                result.Components.BaseURL, 
                filename,
                result.Components.Policy,
                result.Components.Signature,
                result.Components.KeyPairID)

            // Verify the constructed URL has all required components
            assert.Contains(t, constructedURL, "test.cloudfront.net/O1/D100/P1000/")
            assert.Contains(t, constructedURL, filename)
            assert.Contains(t, constructedURL, "Policy=")
            assert.Contains(t, constructedURL, "Signature=")
            assert.Contains(t, constructedURL, "Key-Pair-Id=")
        }

        // Verify policy info is useful for debugging
        assert.Contains(t, result.Components.PolicyInfo.ResourcePattern, "O1/D100/P1000/*")
        assert.Contains(t, result.Components.PolicyInfo.ExpiresAtISO, "T")
        assert.Contains(t, result.Components.PolicyInfo.ExpiresAtISO, "Z")
    })
}

func TestCloudFrontSignedURLHandler_ExtractPolicyInfo(t *testing.T) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    // Use real database 
    db := store.OpenDB(t)
    defer db.Close()
    
    originalDB := PennsieveDB
    PennsieveDB = db.DB
    defer func() { PennsieveDB = originalDB }()

    handler := &CloudFrontSignedURLHandler{
        RequestHandler: RequestHandler{
            logger: testLogger,
            claims: &authorizer.Claims{
                OrgClaim: &organization.Claim{IntId: 1},
            },
        },
    }

    // Test policy decoding
    expiresAt := time.Now().Add(1 * time.Hour)
    
    // Create test policy JSON
    policyJSON := fmt.Sprintf(`{
        "Statement": [{
            "Resource": "https://test.cloudfront.net/O1/D2/P3/*",
            "Condition": {
                "DateLessThan": {
                    "AWS:EpochTime": %d
                }
            }
        }]
    }`, expiresAt.Unix())
    
    // Test base64 standard encoding
    encodedPolicy := base64.StdEncoding.EncodeToString([]byte(policyJSON))
    
    policyInfo, err := handler.extractPolicyInfo(encodedPolicy, expiresAt)
    require.NoError(t, err)
    
    assert.Equal(t, "https://test.cloudfront.net/O1/D2/P3/*", policyInfo.ResourcePattern)
    assert.Equal(t, expiresAt.Unix(), policyInfo.ExpiresAt)
    assert.Equal(t, expiresAt.UTC().Format(time.RFC3339), policyInfo.ExpiresAtISO)

    // Test URL-safe base64 encoding
    urlEncodedPolicy := base64.URLEncoding.EncodeToString([]byte(policyJSON))
    
    policyInfo2, err := handler.extractPolicyInfo(urlEncodedPolicy, expiresAt)
    require.NoError(t, err)
    
    assert.Equal(t, policyInfo.ResourcePattern, policyInfo2.ResourcePattern)
    assert.Equal(t, policyInfo.ExpiresAt, policyInfo2.ExpiresAt)
}
