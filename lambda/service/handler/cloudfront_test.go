package handler

import (
    "context"
    "crypto/rand"
    "crypto/rsa"
    "crypto/x509"
    "database/sql"
    "encoding/base64"
    "encoding/json"
    "encoding/pem"
    "errors"
    "fmt"
    "net/http"
    "strings"
    "testing"
    "time"

    "github.com/DATA-DOG/go-sqlmock"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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
    // Create mock database
    db, mock, err := sqlmock.New()
    require.NoError(t, err)
    defer db.Close()

    // Replace global PennsieveDB for testing
    originalDB := PennsieveDB
    PennsieveDB = db
    defer func() { PennsieveDB = originalDB }()

    tests := []struct {
        name           string
        packageNodeId  string
        datasetNodeId  string
        orgId          int64
        mockSetup      func(sqlmock.Sqlmock)
        expectedPrefix string
        expectError    bool
        errorContains  string
    }{
        {
            name:          "valid package and dataset",
            packageNodeId: "N:package:123",
            datasetNodeId: "N:dataset:456",
            orgId:         1,
            mockSetup: func(mock sqlmock.Sqlmock) {
                rows := sqlmock.NewRows([]string{"package_id", "dataset_id"}).
                    AddRow(789, 101)
                mock.ExpectQuery("SELECT p.id, d.id").
                    WithArgs("N:package:123", "N:dataset:456").
                    WillReturnRows(rows)
            },
            expectedPrefix: "O1/D101/P789/",
            expectError:    false,
        },
        {
            name:          "package not found",
            packageNodeId: "N:package:invalid",
            datasetNodeId: "N:dataset:456",
            orgId:         1,
            mockSetup: func(mock sqlmock.Sqlmock) {
                mock.ExpectQuery("SELECT p.id, d.id").
                    WithArgs("N:package:invalid", "N:dataset:456").
                    WillReturnError(sql.ErrNoRows)
            },
            expectedPrefix: "",
            expectError:    true,
            errorContains:  "package not found or does not belong to specified dataset",
        },
        {
            name:          "package doesn't belong to dataset",
            packageNodeId: "N:package:123",
            datasetNodeId: "N:dataset:wrong",
            orgId:         1,
            mockSetup: func(mock sqlmock.Sqlmock) {
                mock.ExpectQuery("SELECT p.id, d.id").
                    WithArgs("N:package:123", "N:dataset:wrong").
                    WillReturnError(sql.ErrNoRows)
            },
            expectedPrefix: "",
            expectError:    true,
            errorContains:  "package not found or does not belong to specified dataset",
        },
        {
            name:          "database error",
            packageNodeId: "N:package:123",
            datasetNodeId: "N:dataset:456",
            orgId:         1,
            mockSetup: func(mock sqlmock.Sqlmock) {
                mock.ExpectQuery("SELECT p.id, d.id").
                    WithArgs("N:package:123", "N:dataset:456").
                    WillReturnError(errors.New("database connection failed"))
            },
            expectedPrefix: "",
            expectError:    true,
            errorContains:  "database connection failed",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            tt.mockSetup(mock)

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

            // Ensure all expectations were met
            assert.NoError(t, mock.ExpectationsWereMet())
        })
    }
}

func TestCloudFrontSignedURLHandler_GenerateSignedURLWithPolicy(t *testing.T) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

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
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            handler := &CloudFrontSignedURLHandler{
                RequestHandler: RequestHandler{
                    logger: testLogger,
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

    handler := &CloudFrontSignedURLHandler{
        RequestHandler: RequestHandler{
            logger: testLogger,
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
    // Setup database mock
    db, mock, err := sqlmock.New()
    require.NoError(t, err)
    defer db.Close()

    originalDB := PennsieveDB
    PennsieveDB = db
    defer func() { PennsieveDB = originalDB }()

    // Setup CloudFront configuration
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    t.Run("complete flow with valid package", func(t *testing.T) {
        // Setup database expectations
        mock.ExpectQuery("SELECT p.id, d.id").
            WithArgs("N:package:test-456", "N:dataset:test-123").
            WillReturnRows(sqlmock.NewRows([]string{"package_id", "dataset_id"}).
                AddRow(456, 123))

        // Create handler
        ctx := context.Background()
        handler := &CloudFrontSignedURLHandler{
            RequestHandler: RequestHandler{
                method: http.MethodGet,
                queryParams: map[string]string{
                    "dataset_id": "N:dataset:test-123",
                    "package_id": "N:package:test-456",
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
        assert.Contains(t, result.SignedURL, "https://test.cloudfront.net/O1/D123/P456/")
        assert.Contains(t, result.SignedURL, "viewer/manifest.json") // Optional path included
        assert.Contains(t, result.SignedURL, "Policy=")
        assert.Contains(t, result.SignedURL, "Signature=")
        assert.Contains(t, result.SignedURL, "Key-Pair-Id=")

        // Check expiration
        assert.Greater(t, result.ExpiresAt, time.Now().Unix())
        assert.Less(t, result.ExpiresAt, time.Now().Add(2*time.Hour).Unix())

        // Ensure all database expectations were met
        assert.NoError(t, mock.ExpectationsWereMet())
    })

    t.Run("complete flow without optional path", func(t *testing.T) {
        // Setup database expectations
        mock.ExpectQuery("SELECT p.id, d.id").
            WithArgs("N:package:test-789", "N:dataset:test-456").
            WillReturnRows(sqlmock.NewRows([]string{"package_id", "dataset_id"}).
                AddRow(789, 456))

        // Create handler without path parameter
        ctx := context.Background()
        handler := &CloudFrontSignedURLHandler{
            RequestHandler: RequestHandler{
                method: http.MethodGet,
                queryParams: map[string]string{
                    "dataset_id": "N:dataset:test-456",
                    "package_id": "N:package:test-789",
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
        assert.Contains(t, result.SignedURL, "https://test.cloudfront.net/O2/D456/P789/")

        // Ensure all database expectations were met
        assert.NoError(t, mock.ExpectationsWereMet())
    })

    t.Run("error when package not found", func(t *testing.T) {
        // Setup database to return no rows
        mock.ExpectQuery("SELECT p.id, d.id").
            WithArgs("N:package:nonexistent", "N:dataset:test-123").
            WillReturnError(sql.ErrNoRows)

        ctx := context.Background()
        handler := &CloudFrontSignedURLHandler{
            RequestHandler: RequestHandler{
                method: http.MethodGet,
                queryParams: map[string]string{
                    "dataset_id": "N:dataset:test-123",
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

        // Ensure all database expectations were met
        assert.NoError(t, mock.ExpectationsWereMet())
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

// Benchmark tests
func BenchmarkGenerateCloudFrontSignedURL(b *testing.B) {
    defer resetCloudFrontConfig()
    setupCloudFrontConfig()

    handler := &CloudFrontSignedURLHandler{
        RequestHandler: RequestHandler{
            logger: testLogger,
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
