package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dataset"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/organization"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/role"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const assetsTestOrgID int64 = 2
const assetsTestDatasetNodeID = "N:dataset:test-1" // from seed DB

func assetsEditorClaims() *authorizer.Claims {
	return &authorizer.Claims{
		OrgClaim:  &organization.Claim{IntId: assetsTestOrgID},
		UserClaim: &user.Claim{Id: 101, NodeId: "N:user:test-101"},
		DatasetClaim: &dataset.Claim{
			Role:   role.Editor,
			NodeId: assetsTestDatasetNodeID,
			IntId:  1,
		},
	}
}

func assetsViewerClaims() *authorizer.Claims {
	return &authorizer.Claims{
		OrgClaim:  &organization.Claim{IntId: assetsTestOrgID},
		UserClaim: &user.Claim{Id: 101, NodeId: "N:user:test-101"},
		DatasetClaim: &dataset.Claim{
			Role:   role.Viewer,
			NodeId: assetsTestDatasetNodeID,
			IntId:  1,
		},
	}
}

func setupAssetsTestDB(t *testing.T) *store.TestDB {
	t.Helper()
	db := store.OpenDB(t)

	// Seed DB has org schemas but no datasets — create one on demand
	_, err := db.DB.Exec(`INSERT INTO "2".dataset_status (id, name, display_name, color, created_at, updated_at)
		VALUES (1, 'NO_STATUS', 'No Status', '#71747C', NOW(), NOW()) ON CONFLICT (id) DO NOTHING`)
	require.NoError(t, err)
	_, err = db.DB.Exec(`INSERT INTO "2".datasets (id, name, state, node_id, created_at, updated_at, status_id,
		size, etag, description, license, tags, contributors, banner_id, readme_id)
		VALUES (1, 'Assets Test Dataset', 'READY', $1, NOW(), NOW(), 1, 0, NOW(),
		'', '', ARRAY[]::varchar[], ARRAY[]::varchar[], NULL, NULL)
		ON CONFLICT (id) DO NOTHING`, assetsTestDatasetNodeID)
	require.NoError(t, err)

	originalDB := PennsieveDB
	PennsieveDB = db.DB
	ViewerAssetsBucket = "test-storage-bucket"

	t.Cleanup(func() {
		PennsieveDB = originalDB
		db.DB.Exec(`DELETE FROM "2".viewer_asset_packages`)
		db.DB.Exec(`DELETE FROM "2".viewer_assets`)
		db.DB.Exec(`DELETE FROM "2".chat_sessions`)
		db.Truncate(2, "packages")
		db.Truncate(2, "datasets")
		db.Close()
	})

	return db
}

// createTestChatSession inserts a chat_sessions row so chat-scoped viewer
// assets have a valid chat_session_id FK target. Returns the session UUID.
func createTestChatSession(t *testing.T, db *store.TestDB, orgID, datasetID int) string {
	t.Helper()
	var id string
	err := db.DB.QueryRow(fmt.Sprintf(`
		INSERT INTO "%d".chat_sessions (user_id, dataset_id, compute_node_id)
		VALUES (101, $1, 'test-compute-node') RETURNING id`, orgID), datasetID).Scan(&id)
	require.NoError(t, err)
	return id
}

func boolPtr(b bool) *bool { return &b }

func createTestPackages(t *testing.T, db *store.TestDB, orgID int, datasetID int) []string {
	t.Helper()
	nodeIDs := []string{"N:package:test-pkg-a", "N:package:test-pkg-b", "N:package:test-pkg-c"}
	for i, nodeID := range nodeIDs {
		_, err := db.DB.Exec(
			fmt.Sprintf(`INSERT INTO "%d".packages (id, name, type, state, dataset_id, node_id, owner_id, created_at, updated_at)
			 VALUES ($1, $2, 'Image', 'READY', $3, $4, 1, NOW(), NOW()) ON CONFLICT (id) DO NOTHING`, orgID),
			5001+i, fmt.Sprintf("Test Package %d", i), datasetID, nodeID)
		require.NoError(t, err)
	}
	return nodeIDs
}

func TestCreateViewerAsset_Unauthorized(t *testing.T) {
	setupAssetsTestDB(t)

	body, _ := json.Marshal(createViewerAssetRequest{
		Name:      "test-asset",
		AssetType: "ome-zarr",
	})

	req := newTestRequest("POST", "/assets", "create-asset-unauth",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(body))

	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestCreateViewerAsset_MissingFields(t *testing.T) {
	setupAssetsTestDB(t)

	body, _ := json.Marshal(createViewerAssetRequest{Name: "test-asset"})

	req := newTestRequest("POST", "/assets", "create-asset-missing",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(body))

	resp, err := NewHandler(req, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListViewerAssets_RequiresDatasetID(t *testing.T) {
	setupAssetsTestDB(t)

	req := newTestRequest("GET", "/assets", "list-no-ds", nil, "")

	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDeleteViewerAsset_Unauthorized(t *testing.T) {
	setupAssetsTestDB(t)

	req := newTestRequest("DELETE", "/assets/some-uuid", "delete-unauth",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")

	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestPatchViewerAsset_Unauthorized(t *testing.T) {
	setupAssetsTestDB(t)

	body, _ := json.Marshal(updateViewerAssetRequest{Status: strPtr("ready")})
	req := newTestRequest("PATCH", "/assets/some-uuid", "patch-unauth",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(body))

	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestViewerAssetsFullLifecycle(t *testing.T) {
	db := setupAssetsTestDB(t)
	pkgNodeIDs := createTestPackages(t, db, 2, 1)

	// 1. Insert asset directly in DB (bypassing STS which needs real AWS)
	var assetID string
	err := db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES (1, 'lifecycle-test', 'ome-zarr', '{"key":"value"}', 'test-storage-bucket', 101)
		RETURNING id
	`).Scan(&assetID)
	require.NoError(t, err)

	_, err = db.DB.Exec(`
		INSERT INTO "2".viewer_asset_packages (viewer_asset_id, package_id) VALUES ($1, 5001), ($1, 5002)
	`, assetID)
	require.NoError(t, err)

	// 2. List assets for package A
	listReq := newTestRequest("GET", "/assets", "list-lifecycle",
		map[string]string{
			"dataset_id": assetsTestDatasetNodeID,
			"package_id": pkgNodeIDs[0],
		}, "")

	listResp, err := NewHandler(listReq, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)

	if listResp.StatusCode == http.StatusOK {
		var result listViewerAssetsResponse
		err = json.Unmarshal([]byte(listResp.Body), &result)
		require.NoError(t, err)
		require.Len(t, result.Assets, 1)
		assert.Equal(t, "lifecycle-test", result.Assets[0].Name)
		assert.Equal(t, "ome-zarr", result.Assets[0].AssetType)
		assert.Len(t, result.Assets[0].PackageIDs, 2)
	}

	// 3. Patch status to ready
	patchBody, _ := json.Marshal(updateViewerAssetRequest{Status: strPtr("ready")})
	patchReq := newTestRequest("PATCH", "/assets/"+assetID, "patch-lifecycle",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(patchBody))

	patchResp, err := NewHandler(patchReq, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, patchResp.StatusCode)

	var updatedAsset viewerAssetResponse
	err = json.Unmarshal([]byte(patchResp.Body), &updatedAsset)
	require.NoError(t, err)
	assert.Equal(t, "ready", updatedAsset.Status)

	// 4. Update package links (replace with pkg-c only)
	patchBody2, _ := json.Marshal(updateViewerAssetRequest{
		PackageIDs: &[]string{pkgNodeIDs[2]},
	})
	patchReq2 := newTestRequest("PATCH", "/assets/"+assetID, "patch-pkgs",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(patchBody2))

	patchResp2, err := NewHandler(patchReq2, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, patchResp2.StatusCode)

	var updatedAsset2 viewerAssetResponse
	err = json.Unmarshal([]byte(patchResp2.Body), &updatedAsset2)
	require.NoError(t, err)
	assert.Equal(t, []string{pkgNodeIDs[2]}, updatedAsset2.PackageIDs)

	// 5. Delete (S3 deletion will fail in test env, but DB should succeed)
	deleteReq := newTestRequest("DELETE", "/assets/"+assetID, "delete-lifecycle",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")

	deleteResp, err := NewHandler(deleteReq, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, deleteResp.StatusCode)

	// Verify gone
	var count int
	err = db.DB.QueryRow(`SELECT count(*) FROM "2".viewer_assets WHERE id = $1`, assetID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	err = db.DB.QueryRow(`SELECT count(*) FROM "2".viewer_asset_packages WHERE viewer_asset_id = $1`, assetID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// 6. Verify 404 on re-delete
	deleteResp2, err := NewHandler(deleteReq, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, deleteResp2.StatusCode)
}

func strPtr(s string) *string {
	return &s
}

// TestListViewerAssets_NoPackageID_ReturnsDatasetScoped verifies that GET /assets
// with only dataset_id (no package_id) returns assets with no rows in
// viewer_asset_packages.
func TestListViewerAssets_NoPackageID_ReturnsDatasetScoped(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	// Asset A: linked to a package
	var pkgScopedID string
	err := db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES (1, 'pkg-scoped', 'ome-zarr', '{}', 'test-storage-bucket', 101) RETURNING id`).Scan(&pkgScopedID)
	require.NoError(t, err)
	_, err = db.DB.Exec(`INSERT INTO "2".viewer_asset_packages (viewer_asset_id, package_id) VALUES ($1, 5001)`, pkgScopedID)
	require.NoError(t, err)

	// Asset B: no package links — dataset-scoped
	var datasetScopedID string
	err = db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES (1, 'dataset-scoped', 'parquet-umap-viewer', '{}', 'test-storage-bucket', 101) RETURNING id`).Scan(&datasetScopedID)
	require.NoError(t, err)

	req := newTestRequest("GET", "/assets", "list-dataset-scoped",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")

	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	require.Len(t, result.Assets, 1)
	assert.Equal(t, datasetScopedID, result.Assets[0].ID)
	assert.Equal(t, "dataset-scoped", result.Assets[0].Name)
	assert.Empty(t, result.Assets[0].PackageIDs)
}

// TestPatchPackageLinks_RemoveAll_BecomesDatasetScoped verifies that an asset
// can transition from package-scoped to dataset-scoped via PATCH with an empty
// package_ids array, and that subsequent list calls reflect the change.
func TestPatchPackageLinks_RemoveAll_BecomesDatasetScoped(t *testing.T) {
	db := setupAssetsTestDB(t)
	pkgNodeIDs := createTestPackages(t, db, 2, 1)

	var assetID string
	err := db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES (1, 'transition-test', 'ome-zarr', '{}', 'test-storage-bucket', 101) RETURNING id`).Scan(&assetID)
	require.NoError(t, err)
	_, err = db.DB.Exec(`INSERT INTO "2".viewer_asset_packages (viewer_asset_id, package_id) VALUES ($1, 5001)`, assetID)
	require.NoError(t, err)

	// PATCH to empty package_ids
	patchBody, _ := json.Marshal(updateViewerAssetRequest{PackageIDs: &[]string{}})
	patchReq := newTestRequest("PATCH", "/assets/"+assetID, "patch-remove-all",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(patchBody))

	patchResp, err := NewHandler(patchReq, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, patchResp.StatusCode)

	var updated viewerAssetResponse
	require.NoError(t, json.Unmarshal([]byte(patchResp.Body), &updated))
	assert.Empty(t, updated.PackageIDs)

	// Verify the asset now appears in dataset-scoped list
	dsReq := newTestRequest("GET", "/assets", "list-ds-after",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	dsResp, err := NewHandler(dsReq, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, dsResp.StatusCode)

	var dsResult listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(dsResp.Body), &dsResult))
	require.Len(t, dsResult.Assets, 1)
	assert.Equal(t, assetID, dsResult.Assets[0].ID)

	// And no longer appears under the original package
	pkgReq := newTestRequest("GET", "/assets", "list-by-pkg-after",
		map[string]string{"dataset_id": assetsTestDatasetNodeID, "package_id": pkgNodeIDs[0]}, "")
	pkgResp, err := NewHandler(pkgReq, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, pkgResp.StatusCode)

	var pkgResult listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(pkgResp.Body), &pkgResult))
	assert.Empty(t, pkgResult.Assets)
}

// TestPatchPackageLinks_AddToEmpty_BecomesPackageScoped verifies the reverse
// transition: a dataset-scoped asset becomes package-scoped when PATCHed with
// non-empty package_ids.
func TestPatchPackageLinks_AddToEmpty_BecomesPackageScoped(t *testing.T) {
	db := setupAssetsTestDB(t)
	pkgNodeIDs := createTestPackages(t, db, 2, 1)

	var assetID string
	err := db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES (1, 'add-links', 'ome-zarr', '{}', 'test-storage-bucket', 101) RETURNING id`).Scan(&assetID)
	require.NoError(t, err)

	patchBody, _ := json.Marshal(updateViewerAssetRequest{PackageIDs: &[]string{pkgNodeIDs[0]}})
	patchReq := newTestRequest("PATCH", "/assets/"+assetID, "patch-add-links",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(patchBody))

	patchResp, err := NewHandler(patchReq, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, patchResp.StatusCode)

	var updated viewerAssetResponse
	require.NoError(t, json.Unmarshal([]byte(patchResp.Body), &updated))
	assert.Equal(t, []string{pkgNodeIDs[0]}, updated.PackageIDs)

	// Dataset-scoped list should no longer include it
	dsReq := newTestRequest("GET", "/assets", "list-ds-empty",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	dsResp, err := NewHandler(dsReq, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, dsResp.StatusCode)

	var dsResult listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(dsResp.Body), &dsResult))
	assert.Empty(t, dsResult.Assets)
}

// TestListViewerAssets_ExcludesChatScoped verifies a chat-scoped asset
// (chat_session_id set) is excluded from the dataset-scoped listing, while a
// plain dataset-scoped asset still appears.
func TestListViewerAssets_ExcludesChatScoped(t *testing.T) {
	db := setupAssetsTestDB(t)
	sessionID := createTestChatSession(t, db, 2, 1)

	// Chat-scoped asset
	var chatAssetID string
	err := db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by, chat_session_id)
		VALUES (1, 'chat-figure', 'png', '{}', 'test-storage-bucket', 101, $1) RETURNING id`, sessionID).Scan(&chatAssetID)
	require.NoError(t, err)

	// Plain dataset-scoped asset
	var dsAssetID string
	err = db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by)
		VALUES (1, 'dataset-figure', 'ome-zarr', '{}', 'test-storage-bucket', 101) RETURNING id`).Scan(&dsAssetID)
	require.NoError(t, err)

	req := newTestRequest("GET", "/assets", "list-excl-chat",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	require.Len(t, result.Assets, 1)
	assert.Equal(t, dsAssetID, result.Assets[0].ID)
}

// TestGetViewerAsset_ByID verifies a single asset is resolvable by ID via
// GET /assets/{id}, including a chat-scoped one that is hidden from the list.
func TestGetViewerAsset_ByID(t *testing.T) {
	db := setupAssetsTestDB(t)
	sessionID := createTestChatSession(t, db, 2, 1)

	var assetID string
	err := db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by, chat_session_id)
		VALUES (1, 'chat-figure', 'png', '{}', 'test-storage-bucket', 101, $1) RETURNING id`, sessionID).Scan(&assetID)
	require.NoError(t, err)

	req := newTestRequest("GET", "/assets/"+assetID, "get-by-id",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result getViewerAssetResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	assert.Equal(t, assetID, result.Asset.ID)
	assert.Equal(t, "chat-figure", result.Asset.Name)
}

// TestGetViewerAsset_NotFound verifies an unknown asset ID returns 404.
func TestGetViewerAsset_NotFound(t *testing.T) {
	setupAssetsTestDB(t)

	req := newTestRequest("GET", "/assets/00000000-0000-0000-0000-000000000000", "get-missing",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestGetViewerAsset_Unauthorized verifies viewer-role is required.
func TestGetViewerAsset_Unauthorized(t *testing.T) {
	setupAssetsTestDB(t)

	claims := &authorizer.Claims{
		OrgClaim:     &organization.Claim{IntId: assetsTestOrgID},
		UserClaim:    &user.Claim{Id: 101, NodeId: "N:user:test-101"},
		DatasetClaim: &dataset.Claim{Role: role.None, NodeId: assetsTestDatasetNodeID, IntId: 1},
	}
	req := newTestRequest("GET", "/assets/some-uuid", "get-unauth",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	resp, err := NewHandler(req, claims).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestPatchClearChatSession_PromotesToDataset verifies that PATCH with
// clear_chat_session=true detaches a chat figure so it becomes a normal
// dataset-scoped asset (now visible in the dataset listing).
func TestPatchClearChatSession_PromotesToDataset(t *testing.T) {
	db := setupAssetsTestDB(t)
	sessionID := createTestChatSession(t, db, 2, 1)

	var assetID string
	err := db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by, chat_session_id)
		VALUES (1, 'promote-me', 'png', '{}', 'test-storage-bucket', 101, $1) RETURNING id`, sessionID).Scan(&assetID)
	require.NoError(t, err)

	// Initially excluded from the dataset listing.
	listReq := newTestRequest("GET", "/assets", "list-before-promote",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	listResp, err := NewHandler(listReq, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	var before listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(listResp.Body), &before))
	assert.Empty(t, before.Assets)

	// Promote: clear the chat session.
	patchBody, _ := json.Marshal(updateViewerAssetRequest{ClearChatSession: boolPtr(true)})
	patchReq := newTestRequest("PATCH", "/assets/"+assetID, "patch-promote",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(patchBody))
	patchResp, err := NewHandler(patchReq, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, patchResp.StatusCode)

	// chat_session_id is now NULL in the DB.
	var chatSessionID sql.NullString
	require.NoError(t, db.DB.QueryRow(`SELECT chat_session_id FROM "2".viewer_assets WHERE id = $1`, assetID).Scan(&chatSessionID))
	assert.False(t, chatSessionID.Valid)

	// Now visible in the dataset listing.
	listReq2 := newTestRequest("GET", "/assets", "list-after-promote",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	listResp2, err := NewHandler(listReq2, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	var after listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(listResp2.Body), &after))
	require.Len(t, after.Assets, 1)
	assert.Equal(t, assetID, after.Assets[0].ID)
}

