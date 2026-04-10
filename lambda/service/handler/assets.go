package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/permissions"
	log "github.com/sirupsen/logrus"
)

// Table and column constants
const (
	viewerAssetsTable         = "viewer_assets"
	viewerAssetPackagesTable  = "viewer_asset_packages"
	viewerAssetsS3Prefix      = "viewer-assets"
)

// ViewerAssetS3Prefix returns the S3 key prefix for a viewer asset.
func ViewerAssetS3Prefix(orgID, datasetID int64, assetID string) string {
	return fmt.Sprintf("%s/O%d/D%d/%s/", viewerAssetsS3Prefix, orgID, datasetID, assetID)
}

type ViewerAssetsHandler struct {
	RequestHandler
}

// Request/response types

type viewerAssetRow struct {
	ID         string          `json:"id"`
	DatasetID  int64           `json:"-"`
	Name       string          `json:"name"`
	AssetType  string          `json:"asset_type"`
	Properties json.RawMessage `json:"properties"`
	S3Bucket   string          `json:"-"`
	Status     string          `json:"status"`
	CreatedBy  *int64          `json:"-"`
	CreatedAt  time.Time       `json:"created_at"`
}

type createViewerAssetRequest struct {
	Name       string           `json:"name"`
	AssetType  string           `json:"asset_type"`
	Properties *json.RawMessage `json:"properties"`
	PackageIDs []string         `json:"package_ids"`
}

type updateViewerAssetRequest struct {
	Status     *string          `json:"status"`
	Properties *json.RawMessage `json:"properties"`
	PackageIDs *[]string        `json:"package_ids"`
}

type uploadCredentialsResponse struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
	Expiration      string `json:"expiration"`
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
	KeyPrefix       string `json:"key_prefix"`
}

type viewerAssetResponse struct {
	ID         string           `json:"id"`
	DatasetID  string           `json:"dataset_id"`
	Name       string           `json:"name"`
	AssetType  string           `json:"asset_type"`
	AssetURL   string           `json:"asset_url,omitempty"`
	Properties *json.RawMessage `json:"properties"`
	Status     string           `json:"status"`
	PackageIDs []string         `json:"package_ids"`
	CreatedAt  string           `json:"created_at"`
}

type createViewerAssetResponseBody struct {
	Asset             viewerAssetResponse       `json:"asset"`
	UploadCredentials *uploadCredentialsResponse `json:"upload_credentials"`
}

type listViewerAssetsResponse struct {
	Assets     []viewerAssetResponse    `json:"assets"`
	CloudFront *CloudFrontURLComponents `json:"cloudfront,omitempty"`
}

// Handlers

func (h *ViewerAssetsHandler) handleCreate(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	if !authorizer.HasRole(*h.claims, permissions.EditFiles) {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}

	var req createViewerAssetRequest
	if err := json.Unmarshal([]byte(h.body), &req); err != nil {
		return h.logAndBuildError("invalid request body", http.StatusBadRequest), nil
	}
	if req.Name == "" || req.AssetType == "" {
		return h.logAndBuildError("name and asset_type are required", http.StatusBadRequest), nil
	}

	orgID := h.claims.OrgClaim.IntId
	datasetNodeID := h.queryParams["dataset_id"]

	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve dataset: %v", err), http.StatusBadRequest), nil
	}

	// Batch-resolve package IDs upfront (validates they belong to the dataset)
	var resolvedPkgIDs []int64
	var attachedNodeIDs []string
	if len(req.PackageIDs) > 0 {
		resolvedPkgIDs, attachedNodeIDs, err = h.resolvePackageIDs(ctx, orgID, datasetIntID, req.PackageIDs)
		if err != nil {
			return h.logAndBuildError(fmt.Sprintf("failed to resolve packages: %v", err), http.StatusBadRequest), nil
		}
	}

	storageBucket, err := h.resolveStorageBucket(ctx, orgID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve storage bucket: %v", err), http.StatusInternalServerError), nil
	}

	props := json.RawMessage("{}")
	if req.Properties != nil {
		props = *req.Properties
	}

	var asset viewerAssetRow
	insertQuery := fmt.Sprintf(`
		INSERT INTO "%d".%s (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, dataset_id, name, asset_type, properties, s3_bucket, status, created_by, created_at
	`, orgID, viewerAssetsTable)

	err = PennsieveDB.QueryRowContext(ctx, insertQuery,
		datasetIntID, req.Name, req.AssetType, props, storageBucket, h.claims.UserClaim.Id,
	).Scan(&asset.ID, &asset.DatasetID, &asset.Name, &asset.AssetType,
		&asset.Properties, &asset.S3Bucket, &asset.Status, &asset.CreatedBy, &asset.CreatedAt)
	if err != nil {
		h.logger.WithError(err).Error("failed to insert viewer asset")
		return h.logAndBuildError("failed to create viewer asset", http.StatusInternalServerError), nil
	}

	// Batch insert package links
	if len(resolvedPkgIDs) > 0 {
		if err := h.batchInsertPackageLinks(ctx, orgID, asset.ID, resolvedPkgIDs); err != nil {
			h.logger.WithError(err).Error("failed to attach packages")
		}
	}

	keyPrefix := ViewerAssetS3Prefix(orgID, datasetIntID, asset.ID)
	creds, err := h.generateUploadCredentials(ctx, storageBucket, keyPrefix)
	if err != nil {
		h.logger.WithError(err).Error("failed to generate upload credentials")
		return h.logAndBuildError("failed to generate upload credentials", http.StatusInternalServerError), nil
	}

	resp := createViewerAssetResponseBody{
		Asset: viewerAssetResponse{
			ID:         asset.ID,
			DatasetID:  datasetNodeID,
			Name:       asset.Name,
			AssetType:  asset.AssetType,
			Properties: &asset.Properties,
			Status:     asset.Status,
			PackageIDs: attachedNodeIDs,
			CreatedAt:  asset.CreatedAt.Format(time.RFC3339),
		},
		UploadCredentials: creds,
	}

	return h.buildResponse(resp, http.StatusCreated)
}

func (h *ViewerAssetsHandler) handleList(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	if !authorizer.HasRole(*h.claims, permissions.ViewFiles) {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}

	orgID := h.claims.OrgClaim.IntId
	datasetNodeID := h.queryParams["dataset_id"]
	packageNodeID := h.queryParams["package_id"]

	if packageNodeID == "" {
		return h.logAndBuildError("package_id is required", http.StatusBadRequest), nil
	}

	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve dataset: %v", err), http.StatusBadRequest), nil
	}

	packageIntID, err := h.resolvePackageID(ctx, orgID, packageNodeID, datasetIntID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve package: %v", err), http.StatusBadRequest), nil
	}

	query := fmt.Sprintf(`
		SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at
		FROM "%d".%s va
		JOIN "%d".%s vap ON va.id = vap.viewer_asset_id
		WHERE vap.package_id = $1
		ORDER BY va.created_at DESC
	`, orgID, viewerAssetsTable, orgID, viewerAssetPackagesTable)

	rows, err := PennsieveDB.QueryContext(ctx, query, packageIntID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to query assets: %v", err), http.StatusInternalServerError), nil
	}
	defer rows.Close()

	// Build asset URL base: https://{domain}{pathPrefix}/O{orgID}/D{datasetID}/
	var assetURLBase string
	if cloudfrontDistributionDomain != "" {
		cfHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
		pathPrefix, _ := cfHandler.getOrganizationCloudFrontPath(ctx, orgID)
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

		pkgIDs, _ := h.getLinkedPackageNodeIDs(ctx, orgID, a.ID)

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

	// Load CloudFront signing keys from Secrets Manager if not yet cached
	if cloudfrontDistributionDomain != "" && cloudfrontPrivateKey == nil {
		if secretName, ok := os.LookupEnv("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME"); ok {
			cfHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
			if err := cfHandler.loadKeysFromSecretsManager(ctx, secretName); err != nil {
				h.logger.WithError(err).Warn("failed to load CloudFront signing keys")
			}
		}
	}

	// Generate CloudFront signed policy if signing keys are available
	var cookies []string
	if cloudfrontDistributionDomain != "" && cloudfrontPrivateKey != nil {
		s3Prefix := fmt.Sprintf("O%d/D%d/", orgID, datasetIntID)
		cfHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
		signedURL, expiresAt, err := cfHandler.generateCloudFrontSignedURLWithPolicy(s3Prefix, "")
		if err == nil {
			components, err := cfHandler.extractURLComponents(signedURL, expiresAt, s3Prefix)
			if err == nil {
				resp.CloudFront = components
				cookieDomain := "." + os.Getenv("PENNSIEVE_DOMAIN")
				cookies = components.CloudFrontCookies(cookieDomain)
			}
		}
	}

	apiResp, err := h.buildResponse(resp, http.StatusOK)
	if err != nil {
		return nil, err
	}
	if len(cookies) > 0 {
		apiResp.Cookies = cookies
	}
	return apiResp, nil
}

func (h *ViewerAssetsHandler) handleUpdate(ctx context.Context, assetID string) (*events.APIGatewayV2HTTPResponse, error) {
	if !authorizer.HasRole(*h.claims, permissions.EditFiles) {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}

	var req updateViewerAssetRequest
	if err := json.Unmarshal([]byte(h.body), &req); err != nil {
		return h.logAndBuildError("invalid request body", http.StatusBadRequest), nil
	}

	orgID := h.claims.OrgClaim.IntId
	datasetNodeID := h.queryParams["dataset_id"]

	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve dataset: %v", err), http.StatusBadRequest), nil
	}

	// Build dynamic UPDATE
	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	if req.Status != nil {
		setClauses = append(setClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, *req.Status)
		argIdx++
	}
	if req.Properties != nil {
		setClauses = append(setClauses, fmt.Sprintf("properties = $%d", argIdx))
		args = append(args, *req.Properties)
		argIdx++
	}

	if len(setClauses) > 0 {
		updateQuery := fmt.Sprintf(`
			UPDATE "%d".%s SET %s
			WHERE id = $%d AND dataset_id = $%d
			RETURNING id, dataset_id, name, asset_type, properties, s3_bucket, status, created_at
		`, orgID, viewerAssetsTable, strings.Join(setClauses, ", "), argIdx, argIdx+1)

		args = append(args, assetID, datasetIntID)

		var asset viewerAssetRow
		err = PennsieveDB.QueryRowContext(ctx, updateQuery, args...).Scan(
			&asset.ID, &asset.DatasetID, &asset.Name, &asset.AssetType,
			&asset.Properties, &asset.S3Bucket, &asset.Status, &asset.CreatedAt)
		if err == sql.ErrNoRows {
			return h.logAndBuildError("asset not found", http.StatusNotFound), nil
		}
		if err != nil {
			return h.logAndBuildError(fmt.Sprintf("failed to update asset: %v", err), http.StatusInternalServerError), nil
		}
	}

	// Update package links if provided
	if req.PackageIDs != nil {
		deleteQuery := fmt.Sprintf(`DELETE FROM "%d".%s WHERE viewer_asset_id = $1`, orgID, viewerAssetPackagesTable)
		if _, err := PennsieveDB.ExecContext(ctx, deleteQuery, assetID); err != nil {
			h.logger.WithError(err).Error("failed to delete existing package links")
		}

		if len(*req.PackageIDs) > 0 {
			resolvedPkgIDs, _, err := h.resolvePackageIDs(ctx, orgID, datasetIntID, *req.PackageIDs)
			if err == nil && len(resolvedPkgIDs) > 0 {
				if err := h.batchInsertPackageLinks(ctx, orgID, assetID, resolvedPkgIDs); err != nil {
					h.logger.WithError(err).Error("failed to attach packages")
				}
			}
		}
	}

	// Re-fetch for response
	var asset viewerAssetRow
	fetchQuery := fmt.Sprintf(`
		SELECT id, dataset_id, name, asset_type, properties, s3_bucket, status, created_at
		FROM "%d".%s WHERE id = $1 AND dataset_id = $2
	`, orgID, viewerAssetsTable)
	err = PennsieveDB.QueryRowContext(ctx, fetchQuery, assetID, datasetIntID).Scan(
		&asset.ID, &asset.DatasetID, &asset.Name, &asset.AssetType,
		&asset.Properties, &asset.S3Bucket, &asset.Status, &asset.CreatedAt)
	if err == sql.ErrNoRows {
		return h.logAndBuildError("asset not found", http.StatusNotFound), nil
	}
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to fetch asset: %v", err), http.StatusInternalServerError), nil
	}

	pkgIDs, _ := h.getLinkedPackageNodeIDs(ctx, orgID, asset.ID)

	resp := viewerAssetResponse{
		ID:         asset.ID,
		DatasetID:  datasetNodeID,
		Name:       asset.Name,
		AssetType:  asset.AssetType,
		Properties: &asset.Properties,
		Status:     asset.Status,
		PackageIDs: pkgIDs,
		CreatedAt:  asset.CreatedAt.Format(time.RFC3339),
	}

	return h.buildResponse(resp, http.StatusOK)
}

func (h *ViewerAssetsHandler) handleDelete(ctx context.Context, assetID string) (*events.APIGatewayV2HTTPResponse, error) {
	if !authorizer.HasRole(*h.claims, permissions.EditFiles) {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}

	orgID := h.claims.OrgClaim.IntId
	datasetNodeID := h.queryParams["dataset_id"]

	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve dataset: %v", err), http.StatusBadRequest), nil
	}

	var s3Bucket string
	fetchQuery := fmt.Sprintf(`SELECT s3_bucket FROM "%d".%s WHERE id = $1 AND dataset_id = $2`, orgID, viewerAssetsTable)
	err = PennsieveDB.QueryRowContext(ctx, fetchQuery, assetID, datasetIntID).Scan(&s3Bucket)
	if err == sql.ErrNoRows {
		return h.logAndBuildError("asset not found", http.StatusNotFound), nil
	}
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to fetch asset: %v", err), http.StatusInternalServerError), nil
	}

	keyPrefix := ViewerAssetS3Prefix(orgID, datasetIntID, assetID)
	if err := deleteS3Prefix(ctx, s3Bucket, keyPrefix); err != nil {
		h.logger.WithError(err).Warn("failed to delete S3 objects, continuing with database delete")
	}

	deleteQuery := fmt.Sprintf(`DELETE FROM "%d".%s WHERE id = $1 AND dataset_id = $2`, orgID, viewerAssetsTable)
	result, err := PennsieveDB.ExecContext(ctx, deleteQuery, assetID, datasetIntID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to delete asset: %v", err), http.StatusInternalServerError), nil
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return h.logAndBuildError("asset not found", http.StatusNotFound), nil
	}

	return &events.APIGatewayV2HTTPResponse{StatusCode: http.StatusNoContent}, nil
}

// Database helpers

func (h *ViewerAssetsHandler) resolveDatasetID(ctx context.Context, orgID int64, datasetNodeID string) (int64, error) {
	query := fmt.Sprintf(`SELECT id FROM "%d".datasets WHERE node_id = $1`, orgID)
	var id int64
	if err := PennsieveDB.QueryRowContext(ctx, query, datasetNodeID).Scan(&id); err != nil {
		return 0, fmt.Errorf("dataset not found: %s", datasetNodeID)
	}
	return id, nil
}

func (h *ViewerAssetsHandler) resolvePackageID(ctx context.Context, orgID int64, packageNodeID string, datasetIntID int64) (int64, error) {
	query := fmt.Sprintf(`SELECT id FROM "%d".packages WHERE node_id = $1 AND dataset_id = $2`, orgID)
	var id int64
	if err := PennsieveDB.QueryRowContext(ctx, query, packageNodeID, datasetIntID).Scan(&id); err != nil {
		return 0, fmt.Errorf("package not found or does not belong to dataset: %s", packageNodeID)
	}
	return id, nil
}

// resolvePackageIDs batch-resolves package node IDs to integer IDs, returning
// the resolved int IDs and the corresponding node IDs (intersection of valid ones).
func (h *ViewerAssetsHandler) resolvePackageIDs(ctx context.Context, orgID, datasetIntID int64, packageNodeIDs []string) ([]int64, []string, error) {
	if len(packageNodeIDs) == 0 {
		return nil, nil, nil
	}

	placeholders := make([]string, len(packageNodeIDs))
	args := make([]interface{}, len(packageNodeIDs)+1)
	args[0] = datasetIntID
	for i, nodeID := range packageNodeIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = nodeID
	}

	query := fmt.Sprintf(`SELECT id, node_id FROM "%d".packages WHERE dataset_id = $1 AND node_id IN (%s)`,
		orgID, strings.Join(placeholders, ", "))

	rows, err := PennsieveDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve package IDs: %w", err)
	}
	defer rows.Close()

	var intIDs []int64
	var nodeIDs []string
	for rows.Next() {
		var intID int64
		var nodeID string
		if err := rows.Scan(&intID, &nodeID); err != nil {
			return nil, nil, err
		}
		intIDs = append(intIDs, intID)
		nodeIDs = append(nodeIDs, nodeID)
	}
	return intIDs, nodeIDs, nil
}

func (h *ViewerAssetsHandler) resolveStorageBucket(ctx context.Context, orgID int64) (string, error) {
	query := `SELECT storage_bucket FROM pennsieve.organizations WHERE id = $1`
	var bucketName sql.NullString
	if err := PennsieveDB.QueryRowContext(ctx, query, orgID).Scan(&bucketName); err != nil {
		return "", fmt.Errorf("failed to query organization: %w", err)
	}
	if bucketName.Valid && bucketName.String != "" {
		return bucketName.String, nil
	}
	if ViewerAssetsBucket != "" {
		return ViewerAssetsBucket, nil
	}
	return "", fmt.Errorf("no storage bucket configured")
}

// batchInsertPackageLinks inserts all package links in a single INSERT statement.
func (h *ViewerAssetsHandler) batchInsertPackageLinks(ctx context.Context, orgID int64, assetID string, packageIntIDs []int64) error {
	if len(packageIntIDs) == 0 {
		return nil
	}

	valuePlaceholders := make([]string, len(packageIntIDs))
	args := make([]interface{}, 0, 1+len(packageIntIDs))
	args = append(args, assetID)
	for i, pkgID := range packageIntIDs {
		valuePlaceholders[i] = fmt.Sprintf("($1, $%d)", i+2)
		args = append(args, pkgID)
	}

	query := fmt.Sprintf(`INSERT INTO "%d".%s (viewer_asset_id, package_id) VALUES %s ON CONFLICT DO NOTHING`,
		orgID, viewerAssetPackagesTable, strings.Join(valuePlaceholders, ", "))

	_, err := PennsieveDB.ExecContext(ctx, query, args...)
	return err
}

func (h *ViewerAssetsHandler) getLinkedPackageNodeIDs(ctx context.Context, orgID int64, assetID string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT p.node_id FROM "%d".packages p
		JOIN "%d".%s vap ON p.id = vap.package_id
		WHERE vap.viewer_asset_id = $1
	`, orgID, orgID, viewerAssetPackagesTable)

	rows, err := PennsieveDB.QueryContext(ctx, query, assetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodeIDs []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		nodeIDs = append(nodeIDs, nodeID)
	}
	if nodeIDs == nil {
		nodeIDs = []string{}
	}
	return nodeIDs, nil
}

// AWS helpers

func (h *ViewerAssetsHandler) generateUploadCredentials(ctx context.Context, bucket, keyPrefix string) (*uploadCredentialsResponse, error) {
	roleARN := os.Getenv("UPLOAD_CREDENTIALS_ROLE_ARN")
	if roleARN == "" {
		return nil, fmt.Errorf("UPLOAD_CREDENTIALS_ROLE_ARN not configured")
	}

	region := os.Getenv("REGION")
	if region == "" {
		region = "us-east-1"
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	sessionPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": [
				"s3:PutObject",
				"s3:AbortMultipartUpload",
				"s3:ListMultipartUploadParts",
				"s3:ListBucketMultipartUploads"
			],
			"Resource": [
				"arn:aws:s3:::%s",
				"arn:aws:s3:::%s/%s*"
			]
		}]
	}`, bucket, bucket, keyPrefix)

	sessionName := fmt.Sprintf("viewer-asset-%d-%d", h.claims.OrgClaim.IntId, h.claims.UserClaim.Id)

	stsClient := sts.NewFromConfig(cfg)
	durationSeconds := int32(3600)
	result, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName),
		Policy:          aws.String(sessionPolicy),
		DurationSeconds: &durationSeconds,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to assume upload role: %w", err)
	}

	return &uploadCredentialsResponse{
		AccessKeyID:     *result.Credentials.AccessKeyId,
		SecretAccessKey: *result.Credentials.SecretAccessKey,
		SessionToken:    *result.Credentials.SessionToken,
		Expiration:      result.Credentials.Expiration.Format(time.RFC3339),
		Bucket:          bucket,
		Region:          region,
		KeyPrefix:       keyPrefix,
	}, nil
}

// deleteS3Prefix deletes all S3 objects under the given prefix.
// Exported as a package function so the cleanup Lambda can reuse it.
func deleteS3Prefix(ctx context.Context, bucket, prefix string) error {
	region := os.Getenv("REGION")
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}
		if len(page.Contents) == 0 {
			continue
		}

		objects := make([]types.ObjectIdentifier, len(page.Contents))
		for i, obj := range page.Contents {
			objects[i] = types.ObjectIdentifier{Key: obj.Key}
		}

		_, err = s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: objects},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		log.Infof("Deleted %d objects from s3://%s/%s", len(objects), bucket, prefix)
	}

	return nil
}