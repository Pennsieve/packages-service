package handler

import (
    "context"
    "fmt"
    "net/http"
    "time"

    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-sdk-go-v2/aws"
    v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
    "github.com/aws/aws-sdk-go-v2/config"
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
    h.logger.Info("handling OPTIONS request for S3 proxy")

    headers := map[string]string{
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, Range, Origin, Accept",
        "Access-Control-Max-Age":       "3600",
    }

    return &events.APIGatewayV2HTTPResponse{
        StatusCode: http.StatusNoContent,
        Headers:    headers,
    }, nil
}

func (h *S3ProxyHandler) handleGet(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
    // Get package ID from query parameters
    packageId, ok := h.queryParams["package_id"]
    if !ok || packageId == "" {
        return h.logAndBuildError("missing required 'package_id' query parameter", http.StatusBadRequest), nil
    }

    datasetIdStr, ok := h.queryParams["dataset_id"]
    if !ok || datasetIdStr == "" {
        return h.logAndBuildError("missing required 'dataset_id' query parameter", http.StatusBadRequest), nil
    }

    // Check for optional path parameter for viewer assets
    assetPath, isAssetRequest := h.queryParams["path"]

    // Check for redirect preference (default to true for backwards compatibility)
    redirectMode := true
    if redirectParam, ok := h.queryParams["redirect"]; ok && redirectParam == "false" {
        redirectMode = false
    }

    h.logger.WithFields(log.Fields{
        "packageId":      packageId,
        "datasetId":      datasetIdStr,
        "assetPath":      assetPath,
        "isAssetRequest": isAssetRequest,
        "redirectMode":   redirectMode,
    }).Info("handling GET request for package")

    var s3Location *S3Location
    var err error

    if isAssetRequest && assetPath != "" {
        // For viewer assets, validate package belongs to dataset and construct asset path
        s3Location, err = h.getS3LocationForViewerAsset(ctx, packageId, datasetIdStr, assetPath)
    } else {
        // For package files, get S3 location from database and validate package belongs to dataset
        s3Location, err = h.getS3LocationForPackage(ctx, packageId, datasetIdStr)
    }
    
    if err != nil {
        return h.logAndBuildError(fmt.Sprintf("failed to get S3 location: %v", err), http.StatusInternalServerError), nil
    }

    // Generate presigned URL
    presignedURL, err := h.generatePresignedURL(ctx, s3Location, http.MethodGet)
    if err != nil {
        return h.logAndBuildError(fmt.Sprintf("failed to generate presigned URL: %v", err), http.StatusInternalServerError), nil
    }

    // Build response headers with CORS
    headers := h.buildCORSHeaders()
    
    if redirectMode {
        // Return redirect response (preferred for large files)
        headers["Location"] = presignedURL

        h.logger.WithFields(log.Fields{
            "presignedURL": presignedURL,
            "packageId":    packageId,
            "datasetId":    datasetIdStr,
        }).Debug("redirecting to presigned URL")

        return &events.APIGatewayV2HTTPResponse{
            StatusCode: http.StatusTemporaryRedirect,
            Headers:    headers,
            Body:       "",
        }, nil
    } else {
        // Return presigned URL in response body (for clients that can't handle redirects)
        // This mode is useful for DuckDB or other clients that might have issues with redirects
        headers["Content-Type"] = "application/json"
        
        responseBody := fmt.Sprintf(`{"presigned_url": "%s"}`, presignedURL)
        
        h.logger.WithFields(log.Fields{
            "presignedURL": presignedURL,
            "packageId":    packageId,
            "datasetId":    datasetIdStr,
        }).Debug("returning presigned URL in response body")

        return &events.APIGatewayV2HTTPResponse{
            StatusCode: http.StatusOK,
            Headers:    headers,
            Body:       responseBody,
        }, nil
    }
}

func (h *S3ProxyHandler) handleHead(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
    // Get package ID from query parameters
    packageId, ok := h.queryParams["package_id"]
    if !ok || packageId == "" {
        return h.logAndBuildError("missing required 'package_id' query parameter", http.StatusBadRequest), nil
    }

    datasetIdStr, ok := h.queryParams["dataset_id"]
    if !ok || datasetIdStr == "" {
        return h.logAndBuildError("missing required 'dataset_id' query parameter", http.StatusBadRequest), nil
    }

    // Check for optional path parameter for viewer assets
    assetPath, isAssetRequest := h.queryParams["path"]

    h.logger.WithFields(log.Fields{
        "packageId":      packageId,
        "datasetId":      datasetIdStr,
        "assetPath":      assetPath,
        "isAssetRequest": isAssetRequest,
    }).Info("handling HEAD request for package metadata")

    var s3Location *S3Location
    var err error

    if isAssetRequest && assetPath != "" {
        // For viewer assets, validate package belongs to dataset and construct asset path
        s3Location, err = h.getS3LocationForViewerAsset(ctx, packageId, datasetIdStr, assetPath)
    } else {
        // For package files, get S3 location from database and validate package belongs to dataset
        s3Location, err = h.getS3LocationForPackage(ctx, packageId, datasetIdStr)
    }
    
    if err != nil {
        return h.logAndBuildError(fmt.Sprintf("failed to get S3 location: %v", err), http.StatusInternalServerError), nil
    }

    // Generate presigned URL for HEAD request
    presignedURL, err := h.generatePresignedURL(ctx, s3Location, http.MethodHead)
    if err != nil {
        return h.logAndBuildError(fmt.Sprintf("failed to generate presigned URL: %v", err), http.StatusInternalServerError), nil
    }

    // Make HEAD request to S3 to get actual metadata
    req, err := http.NewRequestWithContext(ctx, http.MethodHead, presignedURL, nil)
    if err != nil {
        return h.logAndBuildError(fmt.Sprintf("failed to create HEAD request: %v", err), http.StatusInternalServerError), nil
    }

    client := &http.Client{
        Timeout: 10 * time.Second,
    }
    resp, err := client.Do(req)
    if err != nil {
        return h.logAndBuildError(fmt.Sprintf("failed to fetch metadata from S3: %v", err), http.StatusBadGateway), nil
    }
    defer resp.Body.Close()

    // Build response headers with CORS
    headers := h.buildCORSHeaders()

    // Forward relevant S3 response headers that DuckDB needs
    h.forwardS3Headers(resp, headers)

    h.logger.WithFields(log.Fields{
        "contentLength": resp.Header.Get("Content-Length"),
        "contentType":   resp.Header.Get("Content-Type"),
        "packageId":     packageId,
        "datasetId":     datasetIdStr,
    }).Debug("returning HEAD response with S3 metadata")

    return &events.APIGatewayV2HTTPResponse{
        StatusCode: resp.StatusCode,
        Headers:    headers,
        Body:       "", // HEAD responses have no body
    }, nil
}

// S3Location represents the S3 location of a package file
type S3Location struct {
    Bucket string
    Key    string
}

// getS3LocationForPackage queries the database to get the S3 location for a package
// and validates that the package belongs to the specified dataset
func (h *S3ProxyHandler) getS3LocationForPackage(ctx context.Context, packageNodeId, datasetNodeId string) (*S3Location, error) {
    // Query that validates both package existence and dataset ownership
    // This ensures users can only access packages from datasets they have permission for
    query := fmt.Sprintf(`
		SELECT f.s3_bucket, f.s3_key 
		FROM "%d".files f 
		JOIN "%d".packages p ON f.package_id = p.id 
		JOIN "%d".datasets d ON p.dataset_id = d.id 
		WHERE p.node_id = $1 AND d.node_id = $2
		ORDER BY f.created_at DESC
		LIMIT 1
	`, h.claims.OrgClaim.IntId, h.claims.OrgClaim.IntId, h.claims.OrgClaim.IntId)

    var bucket, key string
    err := PennsieveDB.QueryRowContext(ctx, query, packageNodeId, datasetNodeId).Scan(&bucket, &key)
    if err != nil {
        h.logger.WithError(err).WithFields(map[string]interface{}{
            "packageNodeId": packageNodeId,
            "datasetNodeId": datasetNodeId,
        }).Error("failed to get S3 location from database or package does not belong to dataset")
        return nil, fmt.Errorf("package not found, no associated file, or package does not belong to specified dataset: %w", err)
    }

    h.logger.WithFields(log.Fields{
        "packageNodeId": packageNodeId,
        "bucket":        bucket,
        "key":           key,
    }).Debug("retrieved S3 location for package")

    return &S3Location{
        Bucket: bucket,
        Key:    key,
    }, nil
}

// getS3LocationForViewerAsset validates that a package belongs to a dataset and constructs
// the S3 location for viewer assets using the format: {ViewerAssetsBucket}/O{WorkspaceIntId}/D{DatasetIntId}/P{PackageIntId}/{AssetPath}
func (h *S3ProxyHandler) getS3LocationForViewerAsset(ctx context.Context, packageNodeId, datasetNodeId, assetPath string) (*S3Location, error) {
    // Validate that ViewerAssetsBucket is configured
    if ViewerAssetsBucket == "" {
        return nil, fmt.Errorf("viewer assets bucket not configured")
    }

    // Query to get the internal integer IDs and validate that the package belongs to the dataset
    query := fmt.Sprintf(`
        SELECT p.id, d.id
        FROM "%d".packages p 
        JOIN "%d".datasets d ON p.dataset_id = d.id 
        WHERE p.node_id = $1 AND d.node_id = $2
    `, h.claims.OrgClaim.IntId, h.claims.OrgClaim.IntId)

    var packageIntId, datasetIntId int64
    err := PennsieveDB.QueryRowContext(ctx, query, packageNodeId, datasetNodeId).Scan(&packageIntId, &datasetIntId)
    if err != nil {
        h.logger.WithError(err).WithFields(map[string]interface{}{
            "packageNodeId": packageNodeId,
            "datasetNodeId": datasetNodeId,
        }).Error("failed to get integer IDs for package and dataset or package does not belong to dataset")
        return nil, fmt.Errorf("package not found or does not belong to specified dataset: %w", err)
    }

    // Construct the S3 key for the viewer asset using the new format
    // Format: O{WorkspaceIntId}/D{DatasetIntId}/P{PackageIntId}/{AssetPath}
    assetKey := fmt.Sprintf("O%d/D%d/P%d/%s", h.claims.OrgClaim.IntId, datasetIntId, packageIntId, assetPath)

    h.logger.WithFields(log.Fields{
        "packageNodeId":      packageNodeId,
        "datasetNodeId":      datasetNodeId,
        "packageIntId":       packageIntId,
        "datasetIntId":       datasetIntId,
        "workspaceIntId":     h.claims.OrgClaim.IntId,
        "assetPath":          assetPath,
        "viewerAssetsBucket": ViewerAssetsBucket,
        "assetKey":           assetKey,
    }).Debug("constructed S3 location for viewer asset")

    return &S3Location{
        Bucket: ViewerAssetsBucket,
        Key:    assetKey,
    }, nil
}

// generatePresignedURL generates a presigned URL for the given S3 location
func (h *S3ProxyHandler) generatePresignedURL(ctx context.Context, location *S3Location, method string) (string, error) {
    // Load AWS configuration
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        return "", fmt.Errorf("failed to load AWS config: %w", err)
    }

    // Create S3 client
    s3Client := s3.NewFromConfig(cfg)

    // Create presigner
    presigner := s3.NewPresignClient(s3Client)

    // Generate presigned URL based on method
    var presignedReq *v4.PresignedHTTPRequest

    switch method {
    case http.MethodGet:
        presignedReq, err = presigner.PresignGetObject(ctx, &s3.GetObjectInput{
            Bucket: aws.String(location.Bucket),
            Key:    aws.String(location.Key),
        }, func(opts *s3.PresignOptions) {
            opts.Expires = time.Duration(3600 * time.Second) // 1 hour expiry
        })
    case http.MethodHead:
        // For HEAD requests, we need to use GetObject presigner but the actual request will be HEAD
        presignedReq, err = presigner.PresignGetObject(ctx, &s3.GetObjectInput{
            Bucket: aws.String(location.Bucket),
            Key:    aws.String(location.Key),
        }, func(opts *s3.PresignOptions) {
            opts.Expires = time.Duration(3600 * time.Second) // 1 hour expiry
        })
        // Note: The caller will need to use HEAD method with this URL
    default:
        return "", fmt.Errorf("unsupported method for presigned URL: %s", method)
    }

    if err != nil {
        return "", fmt.Errorf("failed to generate presigned URL: %w", err)
    }

    return presignedReq.URL, nil
}

// buildCORSHeaders returns standard CORS headers
func (h *S3ProxyHandler) buildCORSHeaders() map[string]string {
    return map[string]string{
        "Access-Control-Allow-Origin":   "*",
        "Access-Control-Allow-Methods":  "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers":  "Authorization, Content-Type, Range, Origin, Accept",
        "Access-Control-Expose-Headers": "Content-Length, Content-Type, Content-Range, ETag, Last-Modified, Accept-Ranges",
    }
}

// forwardS3Headers forwards relevant headers from S3 response that DuckDB needs
func (h *S3ProxyHandler) forwardS3Headers(resp *http.Response, headers map[string]string) {
    // Forward S3 response headers that are essential for DuckDB and other clients
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

