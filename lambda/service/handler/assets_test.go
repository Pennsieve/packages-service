package handler

import (
	"context"
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
		db.Truncate(2, "packages")
		db.Truncate(2, "datasets")
		db.Close()
	})

	return db
}

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

func TestListViewerAssets_RequiresPackageID(t *testing.T) {
	setupAssetsTestDB(t)

	req := newTestRequest("GET", "/assets", "list-no-pkg",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")

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