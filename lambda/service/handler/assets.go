package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/lib/pq"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/permissions"
)

// Table and column constants
const (
	viewerAssetsTable        = "viewer_assets"
	viewerAssetPackagesTable = "viewer_asset_packages"
	viewerAssetsS3Prefix     = "viewer-assets"
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
	// ChatSessionID is set only while the asset is chat-scoped (an inline chat
	// figure). NULL once promoted (cleared via clear_chat_session) or for any
	// plain dataset/package asset.
	ChatSessionID sql.NullString `json:"-"`
}

type createViewerAssetRequest struct {
	Name          string           `json:"name"`
	AssetType     string           `json:"asset_type"`
	Properties    *json.RawMessage `json:"properties"`
	PackageIDs    []string         `json:"package_ids"`
	ChatSessionID *string          `json:"chat_session_id"`
}

type updateViewerAssetRequest struct {
	Status     *string          `json:"status"`
	Properties *json.RawMessage `json:"properties"`
	PackageIDs *[]string        `json:"package_ids"`
	// ClearChatSession, when true, detaches the asset from its chat session
	// (chat_session_id = NULL) — i.e. "promote" the figure out of the chat so
	// it becomes a normal dataset asset (and/or a package-linked one via
	// PackageIDs). dataset_id is unchanged, so the bytes never move.
	ClearChatSession *bool `json:"clear_chat_session"`
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
	// PackageIDs is the node IDs in Packages. Kept for older callers.
	PackageIDs []string               `json:"package_ids"`
	Packages   []assetPackageResponse `json:"packages"`
	// ChatSessionID is present only while the asset is still a chat-scoped
	// figure. The frontend uses its presence to distinguish a not-yet-promoted
	// chat figure (offer "promote") from an already-promoted one (don't).
	ChatSessionID string `json:"chat_session_id,omitempty"`
	CreatedAt     string `json:"created_at"`
}

type createViewerAssetResponseBody struct {
	Asset             viewerAssetResponse        `json:"asset"`
	UploadCredentials *uploadCredentialsResponse `json:"upload_credentials"`
}

// assetPackageResponse is one package linked to a viewer asset.
type assetPackageResponse struct {
	PackageID string `json:"package_id"`
	Name      string `json:"name"`
	// FilePath is the package's file path inside the published dataset.
	// Discover only.
	FilePath string `json:"file_path,omitempty"`
}

type listViewerAssetsResponse struct {
	Assets []viewerAssetResponse `json:"assets"`
	// TotalCount, Limit and Offset are only set on a paged list, so an unpaged
	// list looks exactly like it did before paging existed.
	TotalCount *int `json:"total_count,omitempty"`
	Limit      *int `json:"limit,omitempty"`
	Offset     *int `json:"offset,omitempty"`
	// Version is the published version discover listed from. Discover only.
	Version    int                      `json:"version,omitempty"`
	CloudFront *CloudFrontURLComponents `json:"cloudfront,omitempty"`
}

// setPage fills in total_count, limit and offset when q is a paged list.
func (r *listViewerAssetsResponse) setPage(q listQuery, total int) {
	if !q.paged() {
		return
	}
	r.TotalCount = &total
	r.Offset = &q.offset
	if q.limit > 0 {
		r.Limit = &q.limit
	}
}

type getViewerAssetResponse struct {
	Asset      viewerAssetResponse      `json:"asset"`
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
		return h.logAndBuildErrorCause("failed to resolve dataset", http.StatusBadRequest, err), nil
	}

	// Batch-resolve package IDs upfront (validates they belong to the dataset),
	// so a removed package stops the request before the asset is created.
	var resolvedPkgIDs []int64
	if len(req.PackageIDs) > 0 {
		resolvedPkgIDs, err = h.resolvePackageIDs(ctx, orgID, datasetIntID, req.PackageIDs)
		if errors.Is(err, errPackageRemoved) {
			return h.logAndBuildError(err.Error(), http.StatusNotFound), nil
		}
		if err != nil {
			return h.logAndBuildErrorCause("failed to resolve packages", http.StatusBadRequest, err), nil
		}
	}

	storageBucket, err := resolveStorageBucket(ctx, orgID)
	if err != nil {
		return h.logAndBuildErrorCause("failed to resolve storage bucket", http.StatusInternalServerError, err), nil
	}

	props := json.RawMessage("{}")
	if req.Properties != nil {
		props = *req.Properties
	}

	// chat_session_id is optional; when set the figure is chat-scoped (excluded
	// from the dataset listing) while its bytes still live under D{dataset}/.
	cols := []string{"dataset_id", "name", "asset_type", "properties", "s3_bucket", "created_by"}
	vals := []interface{}{datasetIntID, req.Name, req.AssetType, props, storageBucket, h.claims.UserClaim.Id}
	if req.ChatSessionID != nil && *req.ChatSessionID != "" {
		cols = append(cols, "chat_session_id")
		vals = append(vals, *req.ChatSessionID)
	}
	placeholders := make([]string, len(vals))
	for i := range vals {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	var asset viewerAssetRow
	insertQuery := fmt.Sprintf(`
		INSERT INTO "%d".%s (%s)
		VALUES (%s)
		RETURNING id, dataset_id, name, asset_type, properties, s3_bucket, status, created_by, created_at
	`, orgID, viewerAssetsTable, strings.Join(cols, ", "), strings.Join(placeholders, ", "))

	err = PennsieveDB.QueryRowContext(ctx, insertQuery, vals...).Scan(
		&asset.ID, &asset.DatasetID, &asset.Name, &asset.AssetType,
		&asset.Properties, &asset.S3Bucket, &asset.Status, &asset.CreatedBy, &asset.CreatedAt)
	if err != nil {
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:          err,
			logging.KeyOrganizationID: orgID,
			logging.KeyDatasetID:      datasetIntID,
		}, "failed to insert viewer asset")
		return h.logAndBuildError("failed to create viewer asset", http.StatusInternalServerError), nil
	}

	// Batch insert package links
	if len(resolvedPkgIDs) > 0 {
		if err := h.batchInsertPackageLinks(ctx, orgID, asset.ID, resolvedPkgIDs); err != nil {
			h.logger.LogErrorWithFields(logging.Fields{
				logging.KeyError:         err,
				logging.KeyViewerAssetID: asset.ID,
			}, "failed to attach packages")
		}
	}

	keyPrefix := ViewerAssetS3Prefix(orgID, datasetIntID, asset.ID)
	creds, err := h.generateUploadCredentials(ctx, storageBucket, keyPrefix)
	if err != nil {
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:         err,
			logging.KeyViewerAssetID: asset.ID,
			logging.KeyS3Bucket:      storageBucket,
		}, "failed to generate upload credentials")
		return h.logAndBuildError("failed to generate upload credentials", http.StatusInternalServerError), nil
	}

	// Read the links back, like GET and PATCH do, so package_ids and packages
	// show what was actually linked, without removed packages.
	linked := h.linkedPackagesOrEmpty(ctx, orgID, []string{asset.ID})

	resp := createViewerAssetResponseBody{
		Asset: viewerAssetResponse{
			ID:         asset.ID,
			DatasetID:  datasetNodeID,
			Name:       asset.Name,
			AssetType:  asset.AssetType,
			Properties: &asset.Properties,
			Status:     asset.Status,
			PackageIDs: packageNodeIDs(linked[asset.ID]),
			Packages:   packageResponses(linked[asset.ID]),
			CreatedAt:  asset.CreatedAt.Format(time.RFC3339),
		},
		UploadCredentials: creds,
	}

	return h.buildResponse(resp, http.StatusCreated)
}

// handleList returns viewer assets for a dataset, in one of three modes:
//   - package_id: assets linked to that package (package-scoped)
//   - scope=all:  every non-chat asset in the dataset, linked or not
//   - neither:    assets with no package links (dataset-scoped)
//
// Every mode takes the asset_type, status, limit and offset filters. Removed
// packages (in the trash or replaced) are hidden.
func (h *ViewerAssetsHandler) handleList(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	if !authorizer.HasRole(*h.claims, permissions.ViewFiles) {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}

	orgID := h.claims.OrgClaim.IntId
	datasetNodeID := h.queryParams["dataset_id"]
	packageNodeID := h.queryParams["package_id"]
	scope := h.queryParams["scope"]
	status := h.queryParams["status"]

	if err := checkScope(scope, packageNodeID); err != nil {
		return h.logAndBuildError(err.Error(), http.StatusBadRequest), nil
	}
	if status != "" && !isValidAssetStatus(status) {
		return h.logAndBuildError(invalidStatusMessage, http.StatusBadRequest), nil
	}
	limit, offset, err := parsePage(h.queryParams, scope)
	if err != nil {
		return h.logAndBuildError(err.Error(), http.StatusBadRequest), nil
	}

	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildErrorCause("failed to resolve dataset", http.StatusBadRequest, err), nil
	}

	// Default: the dataset's own assets (no package links), minus chat figures.
	q := listQuery{orgID: orgID, datasetID: datasetIntID, mode: listDatasetUnlinked, excludeChat: true}
	switch {
	case packageNodeID != "":
		packageIntID, err := h.resolvePackageID(ctx, orgID, packageNodeID, datasetIntID)
		if errors.Is(err, errPackageRemoved) {
			return h.logAndBuildError(errPackageRemoved.Error(), http.StatusNotFound), nil
		}
		if err != nil {
			return h.logAndBuildErrorCause("failed to resolve package", http.StatusBadRequest, err), nil
		}
		q = listQuery{orgID: orgID, datasetID: datasetIntID, packageID: packageIntID, mode: listByPackage}
	case scope == listScopeAll:
		q.mode = listDatasetAll
		q.liveLinksOnly = true
	}

	q.assetType = h.queryParams["asset_type"]
	q.status = status
	q.limit, q.offset = limit, offset
	return h.runListQuery(ctx, datasetNodeID, q)
}

// invalidStatusMessage is the 400 message for a status that isn't created or ready.
const invalidStatusMessage = "'status' must be 'created' or 'ready'"

// handleGet resolves a single viewer asset by ID and returns it with a per-asset
// CloudFront URL plus dataset-scoped signing cookies. Used by the frontend to
// render inline chat figures (which are excluded from the dataset listing): the
// caller has the asset ID (from the chat content block) and the dataset it lives
// in. Authorization is dataset RBAC — the dataset cookie grants read to every
// asset under O{org}/D{dataset}/, so no chat-session-specific check is needed.
func (h *ViewerAssetsHandler) handleGet(ctx context.Context, assetID string) (*events.APIGatewayV2HTTPResponse, error) {
	if !authorizer.HasRole(*h.claims, permissions.ViewFiles) {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}

	orgID := h.claims.OrgClaim.IntId
	datasetNodeID := h.queryParams["dataset_id"]

	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildErrorCause("failed to resolve dataset", http.StatusBadRequest, err), nil
	}

	// Scope the lookup to the claimed dataset: that pairing is the authorization
	// boundary (the caller proved dataset access via the dataset_id claim).
	var a viewerAssetRow
	query := fmt.Sprintf(`
		SELECT id, dataset_id, name, asset_type, properties, s3_bucket, status, chat_session_id, created_at
		FROM "%d".%s WHERE id = $1 AND dataset_id = $2
	`, orgID, viewerAssetsTable)
	err = PennsieveDB.QueryRowContext(ctx, query, assetID, datasetIntID).Scan(
		&a.ID, &a.DatasetID, &a.Name, &a.AssetType, &a.Properties, &a.S3Bucket, &a.Status, &a.ChatSessionID, &a.CreatedAt)
	if err == sql.ErrNoRows {
		return h.logAndBuildError("asset not found", http.StatusNotFound), nil
	}
	if err != nil {
		return h.logAndBuildErrorCause("failed to fetch asset", http.StatusInternalServerError, err), nil
	}

	linked := h.linkedPackagesOrEmpty(ctx, orgID, []string{a.ID})

	resp := getViewerAssetResponse{
		Asset: viewerAssetResponse{
			ID:            a.ID,
			DatasetID:     datasetNodeID,
			Name:          a.Name,
			AssetType:     a.AssetType,
			AssetURL:      viewerAssetURL(orgID, datasetIntID, a.ID, a.S3Bucket),
			Properties:    &a.Properties,
			Status:        a.Status,
			PackageIDs:    packageNodeIDs(linked[a.ID]),
			Packages:      packageResponses(linked[a.ID]),
			ChatSessionID: a.ChatSessionID.String,
			CreatedAt:     a.CreatedAt.Format(time.RFC3339),
		},
	}

	components, cookies := h.signViewerAssetsCloudFront(ctx, orgID, datasetIntID,
		[]string{cloudFrontPathPrefixForBucket(a.S3Bucket)})
	if components != nil {
		resp.CloudFront = components
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

// runListQuery runs a viewer-assets list query, builds responses with per-asset
// CloudFront URLs and signed-policy cookies, and serializes the result.
func (h *ViewerAssetsHandler) runListQuery(ctx context.Context, datasetNodeID string, q listQuery) (*events.APIGatewayV2HTTPResponse, error) {
	orgID, datasetIntID := q.orgID, q.datasetID
	rows, total, err := queryViewerAssets(ctx, q)
	if err != nil {
		return h.logAndBuildErrorCause("failed to query assets", http.StatusInternalServerError, err), nil
	}

	linked := h.linkedPackagesOrEmpty(ctx, orgID, viewerAssetIDs(rows))

	assets := make([]viewerAssetResponse, 0, len(rows))
	var pathPrefixes []string
	for _, a := range rows {
		pathPrefixes = append(pathPrefixes, cloudFrontPathPrefixForBucket(a.S3Bucket))
		assets = append(assets, viewerAssetResponse{
			ID:         a.ID,
			DatasetID:  datasetNodeID,
			Name:       a.Name,
			AssetType:  a.AssetType,
			AssetURL:   viewerAssetURL(orgID, datasetIntID, a.ID, a.S3Bucket),
			Properties: &a.Properties,
			Status:     a.Status,
			PackageIDs: packageNodeIDs(linked[a.ID]),
			Packages:   packageResponses(linked[a.ID]),
			CreatedAt:  a.CreatedAt.Format(time.RFC3339),
		})
	}

	resp := listViewerAssetsResponse{Assets: assets}
	resp.setPage(q, total)
	components, cookies := h.signViewerAssetsCloudFront(ctx, orgID, datasetIntID, pathPrefixes)
	if components != nil {
		resp.CloudFront = components
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
	// Check before any write, so a bad status changes nothing.
	if req.Status != nil && !isValidAssetStatus(*req.Status) {
		return h.logAndBuildError(invalidStatusMessage, http.StatusBadRequest), nil
	}

	orgID := h.claims.OrgClaim.IntId
	datasetNodeID := h.queryParams["dataset_id"]

	datasetIntID, err := h.resolveDatasetID(ctx, orgID, datasetNodeID)
	if err != nil {
		return h.logAndBuildErrorCause("failed to resolve dataset", http.StatusBadRequest, err), nil
	}

	// Resolve the new links before any write, so a removed package (404) or a
	// failed lookup leaves the asset and its links exactly as they were.
	var resolvedPkgIDs []int64
	if req.PackageIDs != nil && len(*req.PackageIDs) > 0 {
		resolvedPkgIDs, err = h.resolvePackageIDs(ctx, orgID, datasetIntID, *req.PackageIDs)
		if errors.Is(err, errPackageRemoved) {
			return h.logAndBuildError(err.Error(), http.StatusNotFound), nil
		}
		if err != nil {
			return h.logAndBuildErrorCause("failed to resolve packages", http.StatusInternalServerError, err), nil
		}
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
	// Promote out of chat: detach the chat session. dataset_id stays set, so
	// viewer_assets_scope_check holds and the bytes never move.
	if req.ClearChatSession != nil && *req.ClearChatSession {
		setClauses = append(setClauses, "chat_session_id = NULL")
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
			return h.logAndBuildErrorCause("failed to update asset", http.StatusInternalServerError, err), nil
		}
	}

	// Update package links if provided
	if req.PackageIDs != nil {
		// Only replace links to live packages. The client never sees links to
		// removed packages, so it can't send them back; deleting them here would
		// break a later restore from the trash.
		deleteQuery := fmt.Sprintf(`
			DELETE FROM "%[1]d".%[2]s vap
			USING "%[1]d".packages p
			WHERE vap.viewer_asset_id = $1 AND p.id = vap.package_id AND %[3]s
		`, orgID, viewerAssetPackagesTable, livePackageSQL("p"))
		if _, err := PennsieveDB.ExecContext(ctx, deleteQuery, assetID); err != nil {
			h.logger.LogErrorWithFields(logging.Fields{
				logging.KeyError:         err,
				logging.KeyViewerAssetID: assetID,
			}, "failed to delete existing package links")
		}

		if len(resolvedPkgIDs) > 0 {
			if err := h.batchInsertPackageLinks(ctx, orgID, assetID, resolvedPkgIDs); err != nil {
				h.logger.LogErrorWithFields(logging.Fields{
					logging.KeyError:         err,
					logging.KeyViewerAssetID: assetID,
				}, "failed to attach packages")
			}
		}
	}

	// Re-fetch for response
	var asset viewerAssetRow
	fetchQuery := fmt.Sprintf(`
		SELECT id, dataset_id, name, asset_type, properties, s3_bucket, status, chat_session_id, created_at
		FROM "%d".%s WHERE id = $1 AND dataset_id = $2
	`, orgID, viewerAssetsTable)
	err = PennsieveDB.QueryRowContext(ctx, fetchQuery, assetID, datasetIntID).Scan(
		&asset.ID, &asset.DatasetID, &asset.Name, &asset.AssetType,
		&asset.Properties, &asset.S3Bucket, &asset.Status, &asset.ChatSessionID, &asset.CreatedAt)
	if err == sql.ErrNoRows {
		return h.logAndBuildError("asset not found", http.StatusNotFound), nil
	}
	if err != nil {
		return h.logAndBuildErrorCause("failed to fetch asset", http.StatusInternalServerError, err), nil
	}

	linked := h.linkedPackagesOrEmpty(ctx, orgID, []string{asset.ID})

	resp := viewerAssetResponse{
		ID:            asset.ID,
		DatasetID:     datasetNodeID,
		Name:          asset.Name,
		AssetType:     asset.AssetType,
		Properties:    &asset.Properties,
		Status:        asset.Status,
		PackageIDs:    packageNodeIDs(linked[asset.ID]),
		Packages:      packageResponses(linked[asset.ID]),
		ChatSessionID: asset.ChatSessionID.String,
		CreatedAt:     asset.CreatedAt.Format(time.RFC3339),
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
		return h.logAndBuildErrorCause("failed to resolve dataset", http.StatusBadRequest, err), nil
	}

	var s3Bucket string
	fetchQuery := fmt.Sprintf(`SELECT s3_bucket FROM "%d".%s WHERE id = $1 AND dataset_id = $2`, orgID, viewerAssetsTable)
	err = PennsieveDB.QueryRowContext(ctx, fetchQuery, assetID, datasetIntID).Scan(&s3Bucket)
	if err == sql.ErrNoRows {
		return h.logAndBuildError("asset not found", http.StatusNotFound), nil
	}
	if err != nil {
		return h.logAndBuildErrorCause("failed to fetch asset", http.StatusInternalServerError, err), nil
	}

	keyPrefix := ViewerAssetS3Prefix(orgID, datasetIntID, assetID)
	if err := deleteS3Prefix(ctx, s3Bucket, keyPrefix); err != nil {
		h.logger.LogWarnWithFields(logging.Fields{
			logging.KeyError:         err,
			logging.KeyViewerAssetID: assetID,
			logging.KeyS3Bucket:      s3Bucket,
			logging.KeyS3Prefix:      keyPrefix,
		}, "failed to delete S3 objects, continuing with database delete")
	}

	deleteQuery := fmt.Sprintf(`DELETE FROM "%d".%s WHERE id = $1 AND dataset_id = $2`, orgID, viewerAssetsTable)
	result, err := PennsieveDB.ExecContext(ctx, deleteQuery, assetID, datasetIntID)
	if err != nil {
		return h.logAndBuildErrorCause("failed to delete asset", http.StatusInternalServerError, err), nil
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

// errPackageRemoved means the package exists in the dataset but is in the trash
// or was replaced by a newer upload. Callers answer 404, not 400.
var errPackageRemoved = errors.New("package is in the trash or was replaced")

// resolvePackageID returns the integer ID of a package in the dataset. A removed
// package returns errPackageRemoved; an unknown package, or one from another
// dataset, returns a plain error.
func (h *ViewerAssetsHandler) resolvePackageID(ctx context.Context, orgID int64, packageNodeID string, datasetIntID int64) (int64, error) {
	query := fmt.Sprintf(`SELECT p.id, %[2]s FROM "%[1]d".packages p WHERE p.node_id = $1 AND p.dataset_id = $2`,
		orgID, livePackageSQL("p"))
	var id int64
	var live bool
	if err := PennsieveDB.QueryRowContext(ctx, query, packageNodeID, datasetIntID).Scan(&id, &live); err != nil {
		return 0, fmt.Errorf("package not found or does not belong to dataset: %s", packageNodeID)
	}
	if !live {
		return 0, errPackageRemoved
	}
	return id, nil
}

// resolvePackageIDs batch-resolves package node IDs in the dataset to integer
// IDs. Unknown packages, and packages from another dataset, are skipped. If any
// of the packages is removed (in the trash or replaced), it links nothing and
// returns errPackageRemoved naming them, so the caller can answer 404 before
// writing anything.
func (h *ViewerAssetsHandler) resolvePackageIDs(ctx context.Context, orgID, datasetIntID int64, packageNodeIDs []string) ([]int64, error) {
	if len(packageNodeIDs) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(packageNodeIDs))
	args := make([]interface{}, len(packageNodeIDs)+1)
	args[0] = datasetIntID
	for i, nodeID := range packageNodeIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = nodeID
	}

	query := fmt.Sprintf(`SELECT p.id, p.node_id, %[3]s FROM "%[1]d".packages p WHERE p.dataset_id = $1 AND p.node_id IN (%[2]s)`,
		orgID, strings.Join(placeholders, ", "), livePackageSQL("p"))

	rows, err := PennsieveDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve package IDs: %w", err)
	}
	defer rows.Close()

	var intIDs []int64
	var removed []string
	for rows.Next() {
		var intID int64
		var nodeID string
		var live bool
		if err := rows.Scan(&intID, &nodeID, &live); err != nil {
			return nil, err
		}
		if !live {
			removed = append(removed, nodeID)
			continue
		}
		intIDs = append(intIDs, intID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read package IDs: %w", err)
	}

	if len(removed) > 0 {
		sort.Strings(removed) // a stable message, whatever order the rows came in
		return nil, fmt.Errorf("%w: %s", errPackageRemoved, strings.Join(removed, ", "))
	}
	return intIDs, nil
}

// resolveStorageBucket returns the bucket new viewer assets of the organization go
// to: its storage bucket if set, otherwise ViewerAssetsBucket.
func resolveStorageBucket(ctx context.Context, orgID int64) (string, error) {
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

// packageRef is a package linked to a viewer asset. FilePath is only set by
// discover, for packages in the latest published version.
type packageRef struct{ NodeID, Name, FilePath string }

// packageRefsByAsset returns the packages linked to each of the given viewer
// assets, keyed by asset ID and ordered by package node ID. It is one query for
// all the assets, not one per asset. Assets with no links are not in the map.
//
// liveOnly leaves out removed packages (in the trash or replaced). The
// authenticated handlers pass true. Discover passes false: there the gate is
// whether the package is published, not its state in the workspace.
func packageRefsByAsset(ctx context.Context, orgID int64, assetIDs []string, liveOnly bool) (map[string][]packageRef, error) {
	refs := map[string][]packageRef{}
	if len(assetIDs) == 0 {
		return refs, nil
	}

	liveFilter := ""
	if liveOnly {
		liveFilter = "AND " + livePackageSQL("p")
	}
	query := fmt.Sprintf(`
		SELECT vap.viewer_asset_id, p.node_id, p.name
		FROM "%[1]d".%[2]s vap
		JOIN "%[1]d".packages p ON p.id = vap.package_id
		WHERE vap.viewer_asset_id = ANY($1::uuid[])
		  %[3]s
		ORDER BY p.node_id
	`, orgID, viewerAssetPackagesTable, liveFilter)

	rows, err := PennsieveDB.QueryContext(ctx, query, pq.Array(assetIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query linked packages: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var assetID string
		var ref packageRef
		if err := rows.Scan(&assetID, &ref.NodeID, &ref.Name); err != nil {
			return nil, fmt.Errorf("failed to scan linked package: %w", err)
		}
		refs[assetID] = append(refs[assetID], ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read linked packages: %w", err)
	}
	return refs, nil
}

// packageNodeIDs returns the node IDs of refs. It never returns nil, so an
// asset with no links shows "package_ids": [] in JSON, not null.
func packageNodeIDs(refs []packageRef) []string {
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.NodeID)
	}
	return ids
}

// packageResponses turns refs into the `packages` response field. It never
// returns nil, so an asset with no links shows "packages": [] in JSON.
func packageResponses(refs []packageRef) []assetPackageResponse {
	packages := make([]assetPackageResponse, 0, len(refs))
	for _, ref := range refs {
		packages = append(packages, assetPackageResponse{PackageID: ref.NodeID, Name: ref.Name, FilePath: ref.FilePath})
	}
	return packages
}

// linkedPackagesOrEmpty is packageRefsByAsset for the authenticated handlers, so
// removed packages are left out. A failed lookup is logged and treated as "no
// links": the assets still come back, with empty package_ids and packages,
// instead of the whole request failing.
func (h *ViewerAssetsHandler) linkedPackagesOrEmpty(ctx context.Context, orgID int64, assetIDs []string) map[string][]packageRef {
	refs, err := packageRefsByAsset(ctx, orgID, assetIDs, true)
	if err == nil {
		return refs
	}
	fields := logging.Fields{
		logging.KeyError:          err,
		logging.KeyOrganizationID: orgID,
		logging.KeyCount:          len(assetIDs),
	}
	// GET, POST and PATCH look up a single asset: name it so the log says which one.
	if len(assetIDs) == 1 {
		fields[logging.KeyViewerAssetID] = assetIDs[0]
	}
	// Keep this message as is: it is searched for in the prod logs.
	h.logger.LogErrorWithFields(fields, "failed to look up linked packages for viewer asset; returning empty package list")
	return map[string][]packageRef{}
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

		slog.Info("deleted S3 objects for viewer asset prefix",
			slog.Int(logging.KeyDeletedCount, len(objects)),
			slog.String(logging.KeyS3Bucket, bucket),
			slog.String(logging.KeyS3Prefix, prefix))
	}

	return nil
}
