package handler

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/cloudfront/sign"
)

type DiscoverCloudFrontSignedURLHandler struct {
	RequestHandler
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

// handleListAssets lists viewer assets for a published package (unauthenticated).
func (h *DiscoverCloudFrontSignedURLHandler) handleListAssets(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	packageNodeID := h.queryParams["package_id"]
	if packageNodeID == "" {
		return h.logAndBuildError("missing required 'package_id' query parameter", http.StatusBadRequest), nil
	}

	h.logger.WithField("packageId", packageNodeID).Info("handling GET request for discover viewer assets")

	// Validate that the package is published and resolve IDs
	orgID, datasetIntID, packageIntID, err := h.resolveDiscoverPackage(ctx, packageNodeID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve published package: %v", err), http.StatusNotFound), nil
	}

	// Resolve dataset node ID for the response
	var datasetNodeID string
	dsQuery := fmt.Sprintf(`SELECT node_id FROM "%d".datasets WHERE id = $1`, orgID)
	if err := PennsieveDB.QueryRowContext(ctx, dsQuery, datasetIntID).Scan(&datasetNodeID); err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve dataset node ID: %v", err), http.StatusInternalServerError), nil
	}

	// Query viewer assets linked to this package
	query := fmt.Sprintf(`
		SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at
		FROM "%d".viewer_assets va
		JOIN "%d".viewer_asset_packages vap ON va.id = vap.viewer_asset_id
		WHERE vap.package_id = $1
		ORDER BY va.created_at DESC
	`, orgID, orgID)

	rows, err := PennsieveDB.QueryContext(ctx, query, packageIntID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to query assets: %v", err), http.StatusInternalServerError), nil
	}
	defer rows.Close()

	// Build asset URL base: https://{domain}{pathPrefix}/O{orgID}/D{datasetID}/
	var assetURLBase string
	if cloudfrontDistributionDomain != "" {
		pathPrefix, _ := getOrganizationCloudFrontPathByOrgId(ctx, orgID)
		if pathPrefix != "" {
			assetURLBase = fmt.Sprintf("https://%s%s/O%d/D%d/", cloudfrontDistributionDomain, pathPrefix, orgID, datasetIntID)
		} else {
			assetURLBase = fmt.Sprintf("https://%s/O%d/D%d/", cloudfrontDistributionDomain, orgID, datasetIntID)
		}
	}

	var assets []viewerAssetResponse
	for rows.Next() {
		var a viewerAssetRow
		if err := rows.Scan(&a.ID, &a.DatasetID, &a.Name, &a.AssetType, &a.Properties, &a.S3Bucket, &a.Status, &a.CreatedAt); err != nil {
			return h.logAndBuildError(fmt.Sprintf("failed to scan asset row: %v", err), http.StatusInternalServerError), nil
		}

		// Get linked package node IDs
		pkgQuery := fmt.Sprintf(`
			SELECT p.node_id FROM "%d".packages p
			JOIN "%d".viewer_asset_packages vap ON p.id = vap.package_id
			WHERE vap.viewer_asset_id = $1
		`, orgID, orgID)
		pkgRows, err := PennsieveDB.QueryContext(ctx, pkgQuery, a.ID)
		var pkgIDs []string
		if err == nil {
			for pkgRows.Next() {
				var nodeID string
				if pkgRows.Scan(&nodeID) == nil {
					pkgIDs = append(pkgIDs, nodeID)
				}
			}
			pkgRows.Close()
		}
		if pkgIDs == nil {
			pkgIDs = []string{}
		}

		var assetURL string
		if assetURLBase != "" {
			assetURL = assetURLBase + a.ID + "/"
		}

		assets = append(assets, viewerAssetResponse{
			ID:         a.ID,
			DatasetID:  datasetNodeID,
			Name:       a.Name,
			AssetType:  a.AssetType,
			AssetURL:   assetURL,
			Properties: &a.Properties,
			Status:     a.Status,
			PackageIDs: pkgIDs,
			CreatedAt:  a.CreatedAt.Format(time.RFC3339),
		})
	}
	if assets == nil {
		assets = []viewerAssetResponse{}
	}

	resp := listViewerAssetsResponse{Assets: assets}

	// Load CloudFront signing keys if not yet cached
	if cloudfrontDistributionDomain != "" && cloudfrontPrivateKey == nil {
		if secretName, ok := os.LookupEnv("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME"); ok {
			authHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
			if err := authHandler.loadKeysFromSecretsManager(ctx, secretName); err != nil {
				h.logger.WithError(err).Warn("failed to load CloudFront signing keys")
			}
		}
	}

	// Generate CloudFront signed policy
	if cloudfrontDistributionDomain != "" && cloudfrontPrivateKey != nil {
		s3Prefix := fmt.Sprintf("O%d/D%d/", orgID, datasetIntID)
		signedURL, expiresAt, err := h.generateDiscoverCloudFrontSignedURL(ctx, s3Prefix, "", orgID)
		if err == nil {
			components, err := h.extractDiscoverURLComponents(ctx, signedURL, expiresAt, s3Prefix, orgID)
			if err == nil {
				resp.CloudFront = components
			}
		}
	}

	return h.buildResponse(resp, http.StatusOK)
}

// resolveDiscoverPackage validates that a package is published via the Discover DB
// and returns the source org ID, dataset integer ID, and package integer ID.
func (h *DiscoverCloudFrontSignedURLHandler) resolveDiscoverPackage(ctx context.Context, packageNodeID string) (orgID, datasetIntID, packageIntID int64, err error) {
	if DiscoverDB == nil {
		return 0, 0, 0, fmt.Errorf("discover database not configured")
	}

	discoverQuery := `
		SELECT DISTINCT pd.source_organization_id, pd.source_dataset_id
		FROM discover.public_file_versions fv
		JOIN discover.public_datasets pd ON fv.dataset_id = pd.id
		WHERE fv.source_package_id = $1
		LIMIT 1
	`
	err = DiscoverDB.QueryRowContext(ctx, discoverQuery, packageNodeID).Scan(&orgID, &datasetIntID)
	if err != nil {
		h.logger.WithError(err).WithField("packageNodeId", packageNodeID).Error("failed to look up package in discover database")
		return 0, 0, 0, fmt.Errorf("published package not found")
	}

	pennsieveQuery := fmt.Sprintf(`SELECT id FROM "%d".packages WHERE node_id = $1`, orgID)
	err = PennsieveDB.QueryRowContext(ctx, pennsieveQuery, packageNodeID).Scan(&packageIntID)
	if err != nil {
		h.logger.WithError(err).WithField("packageNodeId", packageNodeID).Error("failed to get package integer ID")
		return 0, 0, 0, fmt.Errorf("package not found in platform")
	}

	return orgID, datasetIntID, packageIntID, nil
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
