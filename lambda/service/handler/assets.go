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

type ViewerAssetsHandler struct {
	RequestHandler
}

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
	Properties *json.RawMessage `json:"properties"`
	Status     string           `json:"status"`
	PackageIDs []string         `json:"package_ids"`
	CreatedAt  string           `json:"created_at"`
}

type createViewerAssetResponse struct {
	Asset             viewerAssetResponse        `json:"asset"`
	UploadCredentials *uploadCredentialsResponse  `json:"upload_credentials"`
}

type listViewerAssetsResponse struct {
	Assets     []viewerAssetResponse    `json:"assets"`
	CloudFront *CloudFrontURLComponents `json:"cloudfront,omitempty"`
}

func (h *ViewerAssetsHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	// /assets or /assets/{asset_id}
	if h.path == "/assets" {
		switch h.method {
		case http.MethodPost:
			return h.handleCreate(ctx)
		case http.MethodGet:
			return h.handleList(ctx)
		default:
			return h.logAndBuildError(fmt.Sprintf("method %s not allowed", h.method), http.StatusMethodNotAllowed), nil
		}
	}

	// /assets/{asset_id}
	assetID := strings.TrimPrefix(h.path, "/assets/")
	if assetID == "" {
		return h.logAndBuildError("missing asset_id", http.StatusBadRequest), nil
	}

	switch h.method {
	case http.MethodPatch:
		return h.handleUpdate(ctx, assetID)
	case http.MethodDelete:
		return h.handleDelete(ctx, assetID)
	default:
		return h.logAndBuildError(fmt.Sprintf("method %s not allowed", h.method), http.StatusMethodNotAllowed), nil
	}
}

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

	// Resolve dataset node ID to integer ID
	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve dataset: %v", err), http.StatusBadRequest), nil
	}

	// Resolve storage bucket for this org
	storageBucket, err := h.resolveStorageBucket(ctx, orgID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve storage bucket: %v", err), http.StatusInternalServerError), nil
	}

	// Insert viewer_assets row
	props := json.RawMessage("{}")
	if req.Properties != nil {
		props = *req.Properties
	}

	var asset viewerAssetRow
	insertQuery := fmt.Sprintf(`
		INSERT INTO "%d".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, dataset_id, name, asset_type, properties, s3_bucket, status, created_by, created_at
	`, orgID)

	err = PennsieveDB.QueryRowContext(ctx, insertQuery,
		datasetIntID, req.Name, req.AssetType, props, storageBucket, h.claims.UserClaim.Id,
	).Scan(&asset.ID, &asset.DatasetID, &asset.Name, &asset.AssetType,
		&asset.Properties, &asset.S3Bucket, &asset.Status, &asset.CreatedBy, &asset.CreatedAt)
	if err != nil {
		h.logger.WithError(err).Error("failed to insert viewer asset")
		return h.logAndBuildError("failed to create viewer asset", http.StatusInternalServerError), nil
	}

	// Attach to packages if provided
	var packageNodeIDs []string
	if len(req.PackageIDs) > 0 {
		packageNodeIDs, err = h.attachPackages(ctx, orgID, datasetIntID, asset.ID, req.PackageIDs)
		if err != nil {
			h.logger.WithError(err).Error("failed to attach packages")
		}
	}

	// Generate STS upload credentials
	keyPrefix := fmt.Sprintf("viewer-assets/O%d/D%d/%s/", orgID, datasetIntID, asset.ID)
	creds, err := h.generateUploadCredentials(ctx, storageBucket, keyPrefix)
	if err != nil {
		h.logger.WithError(err).Error("failed to generate upload credentials")
		return h.logAndBuildError("failed to generate upload credentials", http.StatusInternalServerError), nil
	}

	resp := createViewerAssetResponse{
		Asset: viewerAssetResponse{
			ID:         asset.ID,
			DatasetID:  datasetNodeID,
			Name:       asset.Name,
			AssetType:  asset.AssetType,
			Properties: &asset.Properties,
			Status:     asset.Status,
			PackageIDs: packageNodeIDs,
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

	// Resolve package to int ID and validate it belongs to the dataset
	packageIntID, err := h.resolvePackageID(ctx, orgID, packageNodeID, datasetIntID)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to resolve package: %v", err), http.StatusBadRequest), nil
	}

	// Query assets linked to this package
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

	var assets []viewerAssetResponse
	for rows.Next() {
		var a viewerAssetRow
		if err := rows.Scan(&a.ID, &a.DatasetID, &a.Name, &a.AssetType, &a.Properties, &a.S3Bucket, &a.Status, &a.CreatedAt); err != nil {
			return h.logAndBuildError(fmt.Sprintf("failed to scan asset row: %v", err), http.StatusInternalServerError), nil
		}

		// Get linked package node IDs for this asset
		pkgIDs, _ := h.getLinkedPackageNodeIDs(ctx, orgID, a.ID)

		assets = append(assets, viewerAssetResponse{
			ID:         a.ID,
			DatasetID:  datasetNodeID,
			Name:       a.Name,
			AssetType:  a.AssetType,
			Properties: &a.Properties,
			Status:     a.Status,
			PackageIDs: pkgIDs,
			CreatedAt:  a.CreatedAt.Format(time.RFC3339),
		})
	}

	if assets == nil {
		assets = []viewerAssetResponse{}
	}

	// Generate CloudFront signed policy for the dataset prefix
	resp := listViewerAssetsResponse{Assets: assets}

	s3Prefix := fmt.Sprintf("O%d/D%d/", orgID, datasetIntID)
	cfHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
	signedURL, expiresAt, err := cfHandler.generateCloudFrontSignedURLWithPolicy(s3Prefix, "")
	if err == nil {
		components, err := cfHandler.extractURLComponents(signedURL, expiresAt, s3Prefix)
		if err == nil {
			resp.CloudFront = components
		}
	}

	return h.buildResponse(resp, http.StatusOK)
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
			UPDATE "%d".viewer_assets SET %s
			WHERE id = $%d AND dataset_id = $%d
			RETURNING id, dataset_id, name, asset_type, properties, s3_bucket, status, created_at
		`, orgID, strings.Join(setClauses, ", "), argIdx, argIdx+1)

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
		// Delete existing links and insert new ones
		deleteQuery := fmt.Sprintf(`DELETE FROM "%d".viewer_asset_packages WHERE viewer_asset_id = $1`, orgID)
		if _, err := PennsieveDB.ExecContext(ctx, deleteQuery, assetID); err != nil {
			h.logger.WithError(err).Error("failed to delete existing package links")
		}

		if len(*req.PackageIDs) > 0 {
			if _, err := h.attachPackages(ctx, orgID, datasetIntID, assetID, *req.PackageIDs); err != nil {
				h.logger.WithError(err).Error("failed to attach packages")
			}
		}
	}

	// Re-fetch the asset for response
	var asset viewerAssetRow
	fetchQuery := fmt.Sprintf(`
		SELECT id, dataset_id, name, asset_type, properties, s3_bucket, status, created_at
		FROM "%d".viewer_assets WHERE id = $1 AND dataset_id = $2
	`, orgID)
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

	// Fetch asset to get s3_bucket before deleting
	var s3Bucket string
	fetchQuery := fmt.Sprintf(`SELECT s3_bucket FROM "%d".viewer_assets WHERE id = $1 AND dataset_id = $2`, orgID)
	err = PennsieveDB.QueryRowContext(ctx, fetchQuery, assetID, datasetIntID).Scan(&s3Bucket)
	if err == sql.ErrNoRows {
		return h.logAndBuildError("asset not found", http.StatusNotFound), nil
	}
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to fetch asset: %v", err), http.StatusInternalServerError), nil
	}

	// Delete S3 objects under the asset prefix
	keyPrefix := fmt.Sprintf("viewer-assets/O%d/D%d/%s/", orgID, datasetIntID, assetID)
	if err := h.deleteS3Prefix(ctx, s3Bucket, keyPrefix); err != nil {
		h.logger.WithError(err).Warn("failed to delete S3 objects, continuing with database delete")
	}

	// Delete from Postgres (cascades to junction table, trigger writes to cleanup queue)
	deleteQuery := fmt.Sprintf(`DELETE FROM "%d".viewer_assets WHERE id = $1 AND dataset_id = $2`, orgID)
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

// Helper functions

func (h *ViewerAssetsHandler) resolveDatasetID(ctx context.Context, orgID int64, datasetNodeID string) (int64, error) {
	query := fmt.Sprintf(`SELECT id FROM "%d".datasets WHERE node_id = $1`, orgID)
	var datasetIntID int64
	err := PennsieveDB.QueryRowContext(ctx, query, datasetNodeID).Scan(&datasetIntID)
	if err != nil {
		return 0, fmt.Errorf("dataset not found: %s", datasetNodeID)
	}
	return datasetIntID, nil
}

func (h *ViewerAssetsHandler) resolvePackageID(ctx context.Context, orgID int64, packageNodeID string, datasetIntID int64) (int64, error) {
	query := fmt.Sprintf(`SELECT id FROM "%d".packages WHERE node_id = $1 AND dataset_id = $2`, orgID)
	var packageIntID int64
	err := PennsieveDB.QueryRowContext(ctx, query, packageNodeID, datasetIntID).Scan(&packageIntID)
	if err != nil {
		return 0, fmt.Errorf("package not found or does not belong to dataset: %s", packageNodeID)
	}
	return packageIntID, nil
}

func (h *ViewerAssetsHandler) resolveStorageBucket(ctx context.Context, orgID int64) (string, error) {
	query := `SELECT storage_bucket FROM pennsieve.organizations WHERE id = $1`
	var bucketName sql.NullString
	err := PennsieveDB.QueryRowContext(ctx, query, orgID).Scan(&bucketName)
	if err != nil {
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

func (h *ViewerAssetsHandler) attachPackages(ctx context.Context, orgID, datasetIntID int64, assetID string, packageNodeIDs []string) ([]string, error) {
	attached := []string{}
	for _, nodeID := range packageNodeIDs {
		pkgIntID, err := h.resolvePackageID(ctx, orgID, nodeID, datasetIntID)
		if err != nil {
			h.logger.WithError(err).Warnf("skipping package %s", nodeID)
			continue
		}
		insertQuery := fmt.Sprintf(`
			INSERT INTO "%d".viewer_asset_packages (viewer_asset_id, package_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING
		`, orgID)
		if _, err := PennsieveDB.ExecContext(ctx, insertQuery, assetID, pkgIntID); err != nil {
			h.logger.WithError(err).Warnf("failed to attach package %s", nodeID)
			continue
		}
		attached = append(attached, nodeID)
	}
	return attached, nil
}

func (h *ViewerAssetsHandler) getLinkedPackageNodeIDs(ctx context.Context, orgID int64, assetID string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT p.node_id FROM "%d".packages p
		JOIN "%d".viewer_asset_packages vap ON p.id = vap.package_id
		WHERE vap.viewer_asset_id = $1
	`, orgID, orgID)

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

func (h *ViewerAssetsHandler) deleteS3Prefix(ctx context.Context, bucket, prefix string) error {
	region := os.Getenv("REGION")
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	// List and delete in batches
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