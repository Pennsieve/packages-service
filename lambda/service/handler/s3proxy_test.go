package handler

import (
    "context"
    "net/http"
    "net/url"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestS3ProxyHandleOptions(t *testing.T) {
    req := newTestRequest("OPTIONS", "/proxy/s3", "test-request-id", map[string]string{
        "presigned_url": "https://test-bucket.s3.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256",
    }, "")

    handler := S3ProxyHandler{RequestHandler: *NewHandler(req, nil)} // No claims needed for unauthenticated endpoint

    resp, err := handler.handleOptions(context.Background())

    require.NoError(t, err)
    assert.Equal(t, http.StatusNoContent, resp.StatusCode)
    assert.Equal(t, "*", resp.Headers["Access-Control-Allow-Origin"])
    assert.Equal(t, "GET, HEAD, OPTIONS", resp.Headers["Access-Control-Allow-Methods"])
    assert.Equal(t, "Content-Type, Range, Origin, Accept", resp.Headers["Access-Control-Allow-Headers"])
    assert.Equal(t, "3600", resp.Headers["Access-Control-Max-Age"])
}

func TestS3ProxyHandleGetMissingURL(t *testing.T) {
    req := newTestRequest("GET", "/proxy/s3", "test-request-id", map[string]string{}, "")

    handler := S3ProxyHandler{RequestHandler: *NewHandler(req, nil)}

    resp, err := handler.handleGet(context.Background())

    require.NoError(t, err)
    assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
    assert.Contains(t, resp.Body, "missing required 'presigned_url' query parameter")
}

func TestS3ProxyHandleGetInvalidURL(t *testing.T) {
    tests := []struct {
        name        string
        presignedURL string
        expectedError string
    }{
        {
            name:          "Not a URL",
            presignedURL:  "not-a-url",
            expectedError: "invalid presigned URL",
        },
        {
            name:          "HTTP instead of HTTPS",
            presignedURL:  "http://test-bucket.s3.amazonaws.com/test-key",
            expectedError: "URL must use HTTPS scheme",
        },
        {
            name:          "Not an S3 URL",
            presignedURL:  "https://example.com/test-key",
            expectedError: "URL must be an S3 URL",
        },
        {
            name:          "Missing signature parameters",
            presignedURL:  "https://test-bucket.s3.amazonaws.com/test-key",
            expectedError: "URL does not appear to be a valid presigned URL",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            req := newTestRequest("GET", "/proxy/s3", "test-request-id", map[string]string{
                "presigned_url": tt.presignedURL,
            }, "")

            handler := S3ProxyHandler{RequestHandler: *NewHandler(req, nil)}

            resp, err := handler.handleGet(context.Background())

            require.NoError(t, err)
            assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
            assert.Contains(t, resp.Body, tt.expectedError)
        })
    }
}

func TestS3ProxyHandleGetValidURL(t *testing.T) {
    validURL := "https://test-bucket.s3.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test"
    
    req := newTestRequest("GET", "/proxy/s3", "test-request-id", map[string]string{
        "presigned_url": validURL,
    }, "")

    handler := S3ProxyHandler{RequestHandler: *NewHandler(req, nil)}

    resp, err := handler.handleGet(context.Background())

    require.NoError(t, err)
    assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode)
    assert.Equal(t, validURL, resp.Headers["Location"])
    assert.Equal(t, "*", resp.Headers["Access-Control-Allow-Origin"])
}

func TestS3ProxyHandleHeadMissingURL(t *testing.T) {
    req := newTestRequest("HEAD", "/proxy/s3", "test-request-id", map[string]string{}, "")

    handler := S3ProxyHandler{RequestHandler: *NewHandler(req, nil)}

    resp, err := handler.handleHead(context.Background())

    require.NoError(t, err)
    assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
    assert.Contains(t, resp.Body, "missing required 'presigned_url' query parameter")
}

func TestS3ProxyValidatePresignedURL(t *testing.T) {
    handler := S3ProxyHandler{}

    tests := []struct {
        name        string
        url         string
        shouldError bool
    }{
        {
            name:        "Valid S3 presigned URL with AWS4 signature",
            url:         "https://test-bucket.s3.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldError: false,
        },
        {
            name:        "Valid S3 presigned URL with AWS2 signature",
            url:         "https://test-bucket.s3.amazonaws.com/test-key?AWSAccessKeyId=test&Signature=test",
            shouldError: false,
        },
        {
            name:        "Valid S3 URL with region",
            url:         "https://test-bucket.s3.us-west-2.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldError: false,
        },
        {
            name:        "Invalid - not HTTPS",
            url:         "http://test-bucket.s3.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256",
            shouldError: true,
        },
        {
            name:        "Invalid - not S3",
            url:         "https://example.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256",
            shouldError: true,
        },
        {
            name:        "Invalid - missing signature",
            url:         "https://test-bucket.s3.amazonaws.com/test-key",
            shouldError: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := handler.validatePresignedURL(tt.url)
            if tt.shouldError {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}

func TestS3ProxyBuildCORSHeaders(t *testing.T) {
    handler := S3ProxyHandler{}
    headers := handler.buildCORSHeaders()

    expectedHeaders := map[string]string{
        "Access-Control-Allow-Origin":   "*",
        "Access-Control-Allow-Methods":  "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers":  "Content-Type, Range, Origin, Accept",
        "Access-Control-Expose-Headers": "Content-Length, Content-Type, Content-Range, ETag, Last-Modified, Accept-Ranges, Cache-Control, Content-Encoding, Content-Disposition",
    }

    for key, expectedValue := range expectedHeaders {
        assert.Equal(t, expectedValue, headers[key], "Header %s should match", key)
    }
}

func TestS3ProxyMethodNotAllowed(t *testing.T) {
    req := newTestRequest("PUT", "/proxy/s3", "test-request-id", map[string]string{
        "presigned_url": "https://test-bucket.s3.amazonaws.com/test-key",
    }, "")

    handler := S3ProxyHandler{RequestHandler: *NewHandler(req, nil)}

    resp, err := handler.handle(context.Background())

    require.NoError(t, err)
    assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
    assert.Contains(t, resp.Body, "method PUT not allowed")
}

func TestS3ProxyBucketAllowList(t *testing.T) {
    // Save original and restore after test
    originalAllowedBuckets := ProxyAllowedBuckets
    defer func() {
        ProxyAllowedBuckets = originalAllowedBuckets
    }()

    tests := []struct {
        name           string
        allowedBuckets []string
        presignedURL   string
        shouldAllow    bool
    }{
        {
            name:           "No restrictions - all buckets allowed",
            allowedBuckets: []string{},
            presignedURL:   "https://any-bucket.s3.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldAllow:    true,
        },
        {
            name:           "Bucket in allowed list",
            allowedBuckets: []string{"allowed-bucket", "another-bucket"},
            presignedURL:   "https://allowed-bucket.s3.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldAllow:    true,
        },
        {
            name:           "Bucket not in allowed list",
            allowedBuckets: []string{"allowed-bucket", "another-bucket"},
            presignedURL:   "https://forbidden-bucket.s3.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldAllow:    false,
        },
        {
            name:           "Regional URL with allowed bucket",
            allowedBuckets: []string{"my-bucket"},
            presignedURL:   "https://my-bucket.s3.us-west-2.amazonaws.com/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldAllow:    true,
        },
        {
            name:           "Path-style URL with allowed bucket",
            allowedBuckets: []string{"my-bucket"},
            presignedURL:   "https://s3.amazonaws.com/my-bucket/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldAllow:    true,
        },
        {
            name:           "Path-style URL with disallowed bucket",
            allowedBuckets: []string{"allowed-bucket"},
            presignedURL:   "https://s3.amazonaws.com/forbidden-bucket/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test",
            shouldAllow:    false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            ProxyAllowedBuckets = tt.allowedBuckets

            req := newTestRequest("GET", "/proxy/s3", "test-request-id", map[string]string{
                "presigned_url": tt.presignedURL,
            }, "")

            handler := S3ProxyHandler{RequestHandler: *NewHandler(req, nil)}

            resp, err := handler.handleGet(context.Background())
            require.NoError(t, err)

            if tt.shouldAllow {
                assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode, "URL should be allowed")
                assert.Equal(t, tt.presignedURL, resp.Headers["Location"], "Should redirect to presigned URL")
            } else {
                assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "URL should be blocked")
                assert.Contains(t, resp.Body, "is not in the allowed list", "Should indicate bucket is not allowed")
            }
        })
    }
}

func TestExtractBucketName(t *testing.T) {
    tests := []struct {
        name           string
        url            string
        expectedBucket string
    }{
        {
            name:           "Virtual-hosted style",
            url:            "https://my-bucket.s3.amazonaws.com/test-key",
            expectedBucket: "my-bucket",
        },
        {
            name:           "Virtual-hosted style with region",
            url:            "https://my-bucket.s3.us-west-2.amazonaws.com/test-key",
            expectedBucket: "my-bucket",
        },
        {
            name:           "Path-style",
            url:            "https://s3.amazonaws.com/my-bucket/test-key",
            expectedBucket: "my-bucket",
        },
        {
            name:           "Path-style with region",
            url:            "https://s3.us-west-2.amazonaws.com/my-bucket/test-key",
            expectedBucket: "my-bucket",
        },
        {
            name:           "Legacy format",
            url:            "https://my-bucket.s3-us-west-2.amazonaws.com/test-key",
            expectedBucket: "my-bucket",
        },
        {
            name:           "Real pennsieve dev bucket",
            url:            "https://pennsieve-dev-storage-use1.s3.amazonaws.com/14b49597-25da-4f83-8705-a0cb56313817/test-key",
            expectedBucket: "pennsieve-dev-storage-use1",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            parsedURL, err := url.Parse(tt.url)
            require.NoError(t, err)
            
            bucket := extractBucketName(parsedURL)
            assert.Equal(t, tt.expectedBucket, bucket, "Bucket name should match")
        })
    }
}

func TestExtractS3Key(t *testing.T) {
    tests := []struct {
        name        string
        url         string
        expectedKey string
    }{
        {
            name:        "Virtual-hosted style simple key",
            url:         "https://my-bucket.s3.amazonaws.com/test-key",
            expectedKey: "test-key",
        },
        {
            name:        "Virtual-hosted style with path",
            url:         "https://my-bucket.s3.amazonaws.com/folder/subfolder/file.txt",
            expectedKey: "folder/subfolder/file.txt",
        },
        {
            name:        "Path-style simple key",
            url:         "https://s3.amazonaws.com/my-bucket/test-key",
            expectedKey: "test-key",
        },
        {
            name:        "Path-style with path",
            url:         "https://s3.amazonaws.com/my-bucket/folder/subfolder/file.txt",
            expectedKey: "folder/subfolder/file.txt",
        },
        {
            name:        "Real pennsieve dev key",
            url:         "https://pennsieve-dev-storage-use1.s3.amazonaws.com/14b49597-25da-4f83-8705-a0cb56313817/2d901a56-de34-46ef-8b32-4aa72f4f75d2",
            expectedKey: "14b49597-25da-4f83-8705-a0cb56313817/2d901a56-de34-46ef-8b32-4aa72f4f75d2",
        },
        {
            name:        "Complex path with query params",
            url:         "https://my-bucket.s3.amazonaws.com/path/to/file.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256",
            expectedKey: "path/to/file.parquet",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            parsedURL, err := url.Parse(tt.url)
            require.NoError(t, err)
            
            key := extractS3Key(parsedURL)
            assert.Equal(t, tt.expectedKey, key, "S3 key should match")
        })
    }
}