package handler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/cloudfront/sign"
	log "github.com/sirupsen/logrus"
)

type DiscoverCloudFrontSignedURLHandler struct {
	RequestHandler
}

func (h *DiscoverCloudFrontSignedURLHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	switch h.method {
	case http.MethodGet:
		return h.handleGet(ctx)
	case http.MethodOptions:
		return h.handleOptions(ctx)
	default:
		return h.logAndBuildError(fmt.Sprintf("method %s not allowed", h.method), http.StatusMethodNotAllowed), nil
	}
}

func (h *DiscoverCloudFrontSignedURLHandler) handleOptions(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	headers := map[string]string{
		"Access-Control-Allow-Origin":  "*",
		"Access-Control-Allow-Methods": "GET, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type, Origin, Accept",
		"Access-Control-Max-Age":       "3600",
	}
	return &events.APIGatewayV2HTTPResponse{
		StatusCode: http.StatusNoContent,
		Headers:    headers,
	}, nil
}

func (h *DiscoverCloudFrontSignedURLHandler) handleGet(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	// Load CloudFront signing keys (same as authenticated endpoint)
	if cloudfrontKeyID == "" || cloudfrontPrivateKey == nil {
		secretName, ok := os.LookupEnv("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME")
		if !ok {
			log.Error("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME environment variable not set")
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		authHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
		if err := authHandler.loadKeysFromSecretsManager(ctx, secretName); err != nil {
			log.Errorf("Failed to load keys from Secrets Manager: %v", err)
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}
		log.Info("CloudFront keys loaded and cached for this Lambda container")
	}

	if cloudfrontDistributionDomain == "" || cloudfrontKeyID == "" || cloudfrontPrivateKey == nil {
		return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
	}

	// Get parameters from query string
	packageID := h.queryParams["package_id"]
	path := strings.TrimSpace(h.queryParams["path"])
	includeComponents := h.queryParams["include_components"] == "true"

	if packageID == "" {
		return h.logAndBuildError("missing required 'package_id' query parameter", http.StatusBadRequest), nil
	}

	h.logger.WithFields(log.Fields{
		"packageId": packageID,
		"assetPath": path,
	}).Info("handling GET request for discover CloudFront signed URL")

	// Look up org, dataset, and package info from discover DB + pennsieve DB
	s3Prefix, orgId, err := h.getDiscoverS3Prefix(ctx, packageID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to get S3 prefix: %v", err), http.StatusNotFound), nil
	}

	// Generate CloudFront signed URL with custom policy for prefix access
	signedURL, expiresAt, err := h.generateDiscoverCloudFrontSignedURL(ctx, s3Prefix, path, orgId)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to generate signed URL: %v", err), http.StatusInternalServerError), nil
	}

	// Build response
	response := CloudFrontSignedURLResponse{
		SignedURL:  signedURL,
		ExpiresAt:  expiresAt.Unix(),
	}

	if includeComponents {
		components, err := h.extractDiscoverURLComponents(ctx, signedURL, expiresAt, s3Prefix, orgId)
		if err != nil {
			h.logger.WithError(err).Warn("Failed to extract URL components, continuing without them")
		} else {
			response.Components = components
		}
	}

	// Use custom encoder to avoid escaping HTML characters like &
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(response)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to marshal response: %v", err), http.StatusInternalServerError), nil
	}
	responseBody := buf.Bytes()
	if len(responseBody) > 0 && responseBody[len(responseBody)-1] == '\n' {
		responseBody = responseBody[:len(responseBody)-1]
	}

	headers := map[string]string{
		"Content-Type":                  "application/json",
		"Access-Control-Allow-Origin":   "*",
		"Access-Control-Allow-Methods":  "GET, OPTIONS",
		"Access-Control-Allow-Headers":  "Content-Type, Origin, Accept",
		"Access-Control-Expose-Headers": "Content-Type",
	}

	return &events.APIGatewayV2HTTPResponse{
		StatusCode: http.StatusOK,
		Headers:    headers,
		Body:       string(responseBody),
	}, nil
}

// getDiscoverS3Prefix looks up the package in the discover database and constructs the S3 prefix.
// Returns the S3 prefix and the source organization ID.
func (h *DiscoverCloudFrontSignedURLHandler) getDiscoverS3Prefix(ctx context.Context, packageNodeId string) (string, int64, error) {
	if DiscoverDB == nil {
		return "", 0, fmt.Errorf("discover database not configured")
	}

	// Query discover DB: get source_organization_id and source_dataset_id from public_file_versions + public_datasets
	var sourceOrgId, sourceDatasetId int64
	discoverQuery := `
		SELECT DISTINCT pd.source_organization_id, pd.source_dataset_id
		FROM discover.public_file_versions fv
		JOIN discover.public_datasets pd ON fv.dataset_id = pd.id
		WHERE fv.source_package_id = $1
		LIMIT 1
	`
	err := DiscoverDB.QueryRowContext(ctx, discoverQuery, packageNodeId).Scan(&sourceOrgId, &sourceDatasetId)
	if err != nil {
		h.logger.WithError(err).WithField("packageNodeId", packageNodeId).Error("failed to look up package in discover database")
		return "", 0, fmt.Errorf("published package not found: %w", err)
	}

	// Query pennsieve DB: get the package integer ID from the org-scoped schema
	var packageIntId int64
	pennsieveQuery := fmt.Sprintf(`SELECT id FROM "%d".packages WHERE node_id = $1`, sourceOrgId)
	err = PennsieveDB.QueryRowContext(ctx, pennsieveQuery, packageNodeId).Scan(&packageIntId)
	if err != nil {
		h.logger.WithError(err).WithFields(log.Fields{
			"packageNodeId": packageNodeId,
			"sourceOrgId":   sourceOrgId,
		}).Error("failed to get package integer ID from pennsieve database")
		return "", 0, fmt.Errorf("package not found in platform: %w", err)
	}

	s3Prefix := fmt.Sprintf("O%d/D%d/P%d/", sourceOrgId, sourceDatasetId, packageIntId)

	h.logger.WithFields(log.Fields{
		"packageNodeId":   packageNodeId,
		"sourceOrgId":     sourceOrgId,
		"sourceDatasetId": sourceDatasetId,
		"packageIntId":    packageIntId,
		"s3Prefix":        s3Prefix,
	}).Debug("constructed S3 prefix for discover package")

	return s3Prefix, sourceOrgId, nil
}

// generateDiscoverCloudFrontSignedURL generates a signed URL for a discover package.
// Similar to the authenticated version but uses the provided orgId instead of claims.
func (h *DiscoverCloudFrontSignedURLHandler) generateDiscoverCloudFrontSignedURL(ctx context.Context, s3Prefix string, optionalPath string, orgId int64) (string, time.Time, error) {
	cloudfrontPathPrefix, err := getOrganizationCloudFrontPathByOrgId(ctx, orgId)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to get organization CloudFront path: %w", err)
	}

	var resourcePattern string
	if cloudfrontPathPrefix != "" {
		resourcePattern = fmt.Sprintf("https://%s%s/%s*", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix)
	} else {
		resourcePattern = fmt.Sprintf("https://%s/%s*", cloudfrontDistributionDomain, s3Prefix)
	}

	expiresAt := time.Now().Add(1 * time.Hour)

	policy := &sign.Policy{
		Statements: []sign.Statement{
			{
				Resource: resourcePattern,
				Condition: sign.Condition{
					DateLessThan: sign.NewAWSEpochTime(expiresAt),
				},
			},
		},
	}

	signer := sign.NewURLSigner(cloudfrontKeyID, cloudfrontPrivateKey)

	var baseURL string
	if cloudfrontPathPrefix != "" {
		if optionalPath != "" {
			baseURL = fmt.Sprintf("https://%s%s/%s%s", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix, optionalPath)
		} else {
			baseURL = fmt.Sprintf("https://%s%s/%s", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix)
		}
	} else {
		if optionalPath != "" {
			baseURL = fmt.Sprintf("https://%s/%s%s", cloudfrontDistributionDomain, s3Prefix, optionalPath)
		} else {
			baseURL = fmt.Sprintf("https://%s/%s", cloudfrontDistributionDomain, s3Prefix)
		}
	}

	signedURL, err := signer.SignWithPolicy(baseURL, policy)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to sign URL with policy: %w", err)
	}

	return signedURL, expiresAt, nil
}

// extractDiscoverURLComponents extracts URL components for the discover endpoint.
func (h *DiscoverCloudFrontSignedURLHandler) extractDiscoverURLComponents(ctx context.Context, signedURL string, expiresAt time.Time, s3Prefix string, orgId int64) (*CloudFrontURLComponents, error) {
	parts := strings.Split(signedURL, "?")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid signed URL format")
	}

	baseURL := parts[0]
	queryString := parts[1]

	queryParts := strings.Split(queryString, "&")
	var policyStr, signature, keyPairID string

	for _, part := range queryParts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "Policy":
			policyStr = kv[1]
		case "Signature":
			signature = kv[1]
		case "Key-Pair-Id":
			keyPairID = kv[1]
		}
	}

	var extractedBaseURL string
	cloudfrontPathPrefix, err := getOrganizationCloudFrontPathByOrgId(ctx, orgId)
	if err == nil {
		if cloudfrontPathPrefix != "" {
			extractedBaseURL = fmt.Sprintf("https://%s%s/%s", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix)
		} else {
			extractedBaseURL = fmt.Sprintf("https://%s/%s", cloudfrontDistributionDomain, s3Prefix)
		}
	} else {
		if idx := strings.LastIndex(baseURL, "/"); idx != -1 {
			extractedBaseURL = baseURL[:idx+1]
		} else {
			extractedBaseURL = baseURL
		}
	}

	components := &CloudFrontURLComponents{
		BaseURL:   extractedBaseURL,
		Policy:    policyStr,
		Signature: signature,
		KeyPairID: keyPairID,
	}

	if policyStr != "" {
		authHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
		policyInfo, err := authHandler.extractPolicyInfo(policyStr, expiresAt)
		if err == nil {
			components.PolicyInfo = policyInfo
		}
	}

	return components, nil
}

// getOrganizationCloudFrontPathByOrgId is a standalone version that queries by org ID
// without requiring auth claims.
func getOrganizationCloudFrontPathByOrgId(ctx context.Context, orgId int64) (string, error) {
	query := `SELECT storage_bucket FROM pennsieve.organizations WHERE id = $1`
	var bucketName sql.NullString
	err := PennsieveDB.QueryRowContext(ctx, query, orgId).Scan(&bucketName)
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("failed to query organization storage bucket: %w", err)
	}

	if bucketName.Valid && bucketName.String != "" {
		pathPrefix := generateDeterministicPath(bucketName.String)
		return "/" + pathPrefix, nil
	}
	return "", nil
}
