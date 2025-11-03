package handler

import (
    "context"
    "fmt"
    "net/http"
    "net/url"
    "strings"

    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    log "github.com/sirupsen/logrus"
)

type S3ProxyHandler struct {
    RequestHandler
}

func (h *S3ProxyHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
    switch h.method {
    case http.MethodGet:
        return h.handleGet(ctx)
    case http.MethodHead:
        return h.handleHead(ctx)
    case http.MethodOptions:
        return h.handleOptions(ctx)
    default:
        return h.logAndBuildError(fmt.Sprintf("method %s not allowed", h.method), http.StatusMethodNotAllowed), nil
    }
}

func (h *S3ProxyHandler) handleOptions(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
    h.logger.Info("handling OPTIONS request for S3 proxy (unauthenticated)")

    headers := map[string]string{
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Range, Origin, Accept",
        "Access-Control-Max-Age":       "3600",
    }

    return &events.APIGatewayV2HTTPResponse{
        StatusCode: http.StatusNoContent,
        Headers:    headers,
    }, nil
}

func (h *S3ProxyHandler) handleGet(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
    // Get presigned URL from query parameters
    presignedURL, ok := h.queryParams["presigned_url"]
    if !ok || presignedURL == "" {
        return h.logAndBuildError("missing required 'presigned_url' query parameter", http.StatusBadRequest), nil
    }

    // Validate the presigned URL
    if err := h.validatePresignedURL(presignedURL); err != nil {
        return h.logAndBuildError(fmt.Sprintf("invalid presigned URL: %v", err), http.StatusBadRequest), nil
    }

    h.logger.WithFields(log.Fields{
        "presignedURL": presignedURL,
    }).Info("redirecting to presigned URL")

    // Build response headers with CORS
    headers := h.buildCORSHeaders()
    headers["Location"] = presignedURL

    return &events.APIGatewayV2HTTPResponse{
        StatusCode: http.StatusTemporaryRedirect,
        Headers:    headers,
        Body:       "",
    }, nil
}

func (h *S3ProxyHandler) handleHead(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
    // Get presigned URL from query parameters
    presignedURL, ok := h.queryParams["presigned_url"]
    if !ok || presignedURL == "" {
        return h.logAndBuildError("missing required 'presigned_url' query parameter", http.StatusBadRequest), nil
    }

    // Validate the presigned URL
    if err := h.validatePresignedURL(presignedURL); err != nil {
        return h.logAndBuildError(fmt.Sprintf("invalid presigned URL: %v", err), http.StatusBadRequest), nil
    }

    // Parse the URL to extract bucket and key
    parsedURL, err := url.Parse(presignedURL)
    if err != nil {
        return h.logAndBuildError(fmt.Sprintf("failed to parse presigned URL: %v", err), http.StatusBadRequest), nil
    }

    bucket := extractBucketName(parsedURL)
    key := extractS3Key(parsedURL)

    if bucket == "" || key == "" {
        return h.logAndBuildError("could not extract bucket or key from presigned URL", http.StatusBadRequest), nil
    }

    // Extract response-content-disposition from presigned URL if present (for fallback)
    queryParams := parsedURL.Query()
    responseContentDisposition := queryParams.Get("response-content-disposition")

    h.logger.WithFields(log.Fields{
        "bucket":       bucket,
        "key":          key,
        "presignedURL": presignedURL,
    }).Info("handling HEAD request for S3 metadata using direct S3 access")

    // Use direct S3 HEAD request instead of using the presigned URL
    // This works because presigned URLs are method-specific and won't work for HEAD
    if S3Client == nil {
        return h.logAndBuildError("S3 client not initialized", http.StatusInternalServerError), nil
    }

    headInput := &s3.HeadObjectInput{
        Bucket: &bucket,
        Key:    &key,
    }

    headOutput, err := S3Client.HeadObject(ctx, headInput)
    if err != nil {
        h.logger.WithError(err).WithFields(log.Fields{
            "bucket": bucket,
            "key":    key,
        }).Error("failed to get object metadata from S3")
        
        // Check if it's a 404 or 403 error and return appropriate status
        // AWS SDK v2 error handling
        if strings.Contains(err.Error(), "NotFound") {
            return h.logAndBuildError("object not found", http.StatusNotFound), nil
        }
        if strings.Contains(err.Error(), "AccessDenied") || strings.Contains(err.Error(), "Forbidden") {
            return h.logAndBuildError("access denied", http.StatusForbidden), nil
        }
        
        return h.logAndBuildError(fmt.Sprintf("failed to get object metadata: %v", err), http.StatusBadGateway), nil
    }

    // Debug log the actual content length received from S3
    h.logger.WithFields(log.Fields{
        "contentLength": headOutput.ContentLength,
        "contentType":   headOutput.ContentType,
        "bucket":        bucket,
        "key":           key,
    }).Info("received S3 HEAD response")

    // Build response headers with CORS
    headers := h.buildCORSHeaders()
    
    // Also set lowercase versions for API Gateway compatibility
    headers["access-control-allow-origin"] = headers["Access-Control-Allow-Origin"]
    headers["access-control-expose-headers"] = headers["Access-Control-Expose-Headers"]

    // Set Content-Type to application/octet-stream for binary files
    // This is the standard MIME type for binary data
    headers["Content-Type"] = "application/octet-stream"
    headers["content-type"] = "application/octet-stream" // lowercase version for compatibility
    // ContentLength is int64 (not a pointer) in SDK v2
    headers["Content-Length"] = fmt.Sprintf("%d", headOutput.ContentLength)
    if headOutput.ETag != nil {
        headers["ETag"] = *headOutput.ETag
    }
    if headOutput.LastModified != nil {
        headers["Last-Modified"] = headOutput.LastModified.Format(http.TimeFormat)
    }
    headers["Accept-Ranges"] = "bytes" // S3 always supports range requests
    
    // Additional headers that might be useful
    if headOutput.CacheControl != nil {
        headers["Cache-Control"] = *headOutput.CacheControl
    }
    if headOutput.ContentEncoding != nil {
        headers["Content-Encoding"] = *headOutput.ContentEncoding
    }
    
    // Set Content-Disposition based on the S3 key filename
    // Extract filename from the S3 key (last part after the last slash)
    filename := key
    if lastSlash := strings.LastIndex(key, "/"); lastSlash != -1 {
        filename = key[lastSlash+1:]
    }
    
    // If we have a filename from the key, use it
    if filename != "" && filename != key {
        headers["Content-Disposition"] = fmt.Sprintf("attachment; filename=\"%s\"", filename)
    } else if responseContentDisposition != "" {
        // Fallback to presigned URL content-disposition if no filename in key
        if decoded, err := url.QueryUnescape(responseContentDisposition); err == nil {
            headers["Content-Disposition"] = decoded
        } else {
            headers["Content-Disposition"] = responseContentDisposition
        }
    } else if headOutput.ContentDisposition != nil {
        // Fallback to S3 object metadata
        headers["Content-Disposition"] = *headOutput.ContentDisposition
    }

    h.logger.WithFields(log.Fields{
        "contentLength": headers["Content-Length"],
        "contentType":   headers["Content-Type"],
        "allHeaders":    headers,
    }).Info("returning HEAD response with S3 metadata")

    response := &events.APIGatewayV2HTTPResponse{
        StatusCode: http.StatusOK,
        Headers:    headers,
        Body:       "", // HEAD responses have no body
    }
    
    h.logger.WithFields(log.Fields{
        "responseStatusCode": response.StatusCode,
        "responseHeaders":    response.Headers,
        "responseBody":       response.Body,
    }).Info("final Lambda response being returned")

    // Ensure Content-Length is set explicitly in multiple ways due to API Gateway issues
    contentLengthValue := fmt.Sprintf("%d", headOutput.ContentLength)
    response.Headers["content-length"] = contentLengthValue  // lowercase
    response.Headers["Content-Length"] = contentLengthValue  // proper case
    
    return response, nil
}

// validatePresignedURL validates that the URL is a valid S3 presigned URL
func (h *S3ProxyHandler) validatePresignedURL(presignedURL string) error {
    parsedURL, err := url.Parse(presignedURL)
    if err != nil {
        return fmt.Errorf("failed to parse URL: %w", err)
    }

    // Check that it's an HTTPS URL
    if parsedURL.Scheme != "https" {
        return fmt.Errorf("URL must use HTTPS scheme")
    }

    // Check that it's an S3 URL (amazonaws.com domain)
    if parsedURL.Host == "" {
        return fmt.Errorf("URL must have a valid host")
    }

    // Basic validation that it looks like an S3 URL
    // This could be made more strict if needed
    // Examples of valid S3 hosts:
    // - bucket-name.s3.amazonaws.com
    // - bucket-name.s3.region.amazonaws.com
    // - s3.amazonaws.com/bucket-name
    // - s3.region.amazonaws.com/bucket-name
    if !isS3URL(parsedURL.Host) {
        return fmt.Errorf("URL must be an S3 URL")
    }

    // Extract bucket name from the URL
    bucketName := extractBucketName(parsedURL)
    if bucketName == "" {
        return fmt.Errorf("could not determine bucket name from URL")
    }

    // Validate against allowed buckets if configured
    if h.logger != nil {
        h.logger.WithFields(log.Fields{
            "bucket": bucketName,
            "allowed_buckets_count": len(ProxyAllowedBuckets),
            "allowed_buckets": ProxyAllowedBuckets,
        }).Info("DEBUG: checking bucket against allowed list")
    }
    
    if len(ProxyAllowedBuckets) > 0 {
        allowed := false
        for _, allowedBucket := range ProxyAllowedBuckets {
            if h.logger != nil {
                h.logger.WithFields(log.Fields{
                    "comparing_bucket": bucketName,
                    "against_allowed": allowedBucket,
                    "equal": bucketName == allowedBucket,
                }).Info("DEBUG: bucket comparison")
            }
            if bucketName == allowedBucket {
                allowed = true
                break
            }
        }
        if !allowed {
            if h.logger != nil {
                h.logger.WithFields(log.Fields{
                    "bucket": bucketName,
                    "allowed_buckets": ProxyAllowedBuckets,
                }).Warn("attempted access to non-allowed bucket")
            }
            return fmt.Errorf("bucket %s is not in the allowed list", bucketName)
        }
    } else {
        if h.logger != nil {
            h.logger.Info("DEBUG: no bucket restrictions configured - allowing all buckets")
        }
    }

    // Check for required presigned URL query parameters
    queryParams := parsedURL.Query()
    
    // Check for AWS Signature Version 4 parameters
    if queryParams.Get("X-Amz-Algorithm") == "" &&
       queryParams.Get("X-Amz-Credential") == "" &&
       queryParams.Get("X-Amz-Signature") == "" {
        // Check for AWS Signature Version 2 parameters (legacy)
        if queryParams.Get("AWSAccessKeyId") == "" &&
           queryParams.Get("Signature") == "" {
            return fmt.Errorf("URL does not appear to be a valid presigned URL (missing signature parameters)")
        }
    }

    return nil
}

// extractBucketName extracts the bucket name from an S3 URL
func extractBucketName(parsedURL *url.URL) string {
    host := parsedURL.Host
    path := parsedURL.Path
    
    // Virtual-hosted-style URLs: bucket-name.s3.amazonaws.com
    // or bucket-name.s3.region.amazonaws.com
    if contains(host, ".s3.") || contains(host, ".s3-") {
        // The bucket name is the first part of the host
        parts := strings.Split(host, ".")
        if len(parts) > 0 {
            return parts[0]
        }
    }
    
    // Path-style URLs: s3.amazonaws.com/bucket-name/key
    // or s3.region.amazonaws.com/bucket-name/key
    if strings.HasPrefix(host, "s3.") || strings.HasPrefix(host, "s3-") {
        // The bucket name is the first part of the path
        if path != "" && path != "/" {
            // Remove leading slash
            if strings.HasPrefix(path, "/") {
                path = path[1:]
            }
            // Get the first path segment
            parts := strings.Split(path, "/")
            if len(parts) > 0 && parts[0] != "" {
                return parts[0]
            }
        }
    }
    
    return ""
}

// extractS3Key extracts the S3 key from a presigned URL
func extractS3Key(parsedURL *url.URL) string {
    host := parsedURL.Host
    path := parsedURL.Path
    
    // Remove leading slash
    if strings.HasPrefix(path, "/") {
        path = path[1:]
    }
    
    // Virtual-hosted-style URLs: bucket-name.s3.amazonaws.com/key
    // The entire path is the key
    if contains(host, ".s3.") || contains(host, ".s3-") {
        // Remove query parameters - path is already clean
        return path
    }
    
    // Path-style URLs: s3.amazonaws.com/bucket-name/key
    // Need to remove the bucket name from the path
    if strings.HasPrefix(host, "s3.") || strings.HasPrefix(host, "s3-") {
        parts := strings.SplitN(path, "/", 2)
        if len(parts) == 2 {
            return parts[1] // Return everything after the bucket name
        }
    }
    
    return ""
}

// isS3URL checks if the host is an S3 URL
func isS3URL(host string) bool {
    // Check various S3 URL patterns
    // Patterns include:
    // - bucket.s3.amazonaws.com
    // - bucket.s3.region.amazonaws.com  
    // - bucket.s3-region.amazonaws.com (legacy)
    // - s3.amazonaws.com
    // - s3.region.amazonaws.com
    // - s3-accelerate patterns
    return contains(host, ".s3.amazonaws.com") ||
           contains(host, ".s3-") || // Legacy S3 URLs
           contains(host, "s3.amazonaws.com") ||
           contains(host, ".s3.") && contains(host, ".amazonaws.com") || // Regional S3 URLs like s3.us-west-2.amazonaws.com
           contains(host, "s3-accelerate.amazonaws.com") ||
           contains(host, "s3-accelerate.dualstack.amazonaws.com")
}

// contains is a simple string contains helper
func contains(s, substr string) bool {
    return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || len(substr) < len(s) && containsMiddle(s, substr)))
}

// containsMiddle checks if substr is in the middle of s
func containsMiddle(s, substr string) bool {
    for i := 0; i <= len(s)-len(substr); i++ {
        if s[i:i+len(substr)] == substr {
            return true
        }
    }
    return false
}

// buildCORSHeaders returns standard CORS headers
func (h *S3ProxyHandler) buildCORSHeaders() map[string]string {
    // Explicitly list Content-Length first in exposed headers for DuckDB-WASM
    // Some browsers/frameworks are sensitive to the order and case
    return map[string]string{
        "Access-Control-Allow-Origin":   "*",
        "Access-Control-Allow-Methods":  "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers":  "Content-Type, Content-Length, Range, Origin, Accept",
        "Access-Control-Expose-Headers": "Content-Length, Content-Type, Content-Range, ETag, Last-Modified, Accept-Ranges, Cache-Control, Content-Encoding, Content-Disposition",
        "Access-Control-Max-Age":        "3600",
    }
}

// forwardS3Headers forwards relevant headers from S3 response
func (h *S3ProxyHandler) forwardS3Headers(resp *http.Response, headers map[string]string) {
    // Forward S3 response headers that are essential for clients
    headersToForward := []string{
        "Content-Type",
        "Content-Length", 
        "Content-Range",
        "ETag",
        "Last-Modified",
        "Accept-Ranges", // Critical for range requests
        "Cache-Control",
        "Content-Disposition",
        "Content-Encoding",
    }

    for _, header := range headersToForward {
        if value := resp.Header.Get(header); value != "" {
            headers[header] = value
        }
    }
}