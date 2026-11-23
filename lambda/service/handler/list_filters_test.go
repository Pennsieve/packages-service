package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for the GET /assets filters (scope, asset_type, status, limit, offset),
// the packages[] field, and the status check in PATCH /assets/{id}.
//
// insertDiscoverTestAsset makes a ready 'thumb' asset in org 2 / dataset 1; the
// tests change the type or status by hand when they need to.

// listFiltersGet calls the authenticated GET /assets as a viewer.
func listFiltersGet(t *testing.T, params map[string]string) *events.APIGatewayV2HTTPResponse {
	t.Helper()
	req := newTestRequest("GET", "/assets", "list-filters", params, "")
	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	return resp
}

// listFiltersGetOK calls GET /assets, requires a 200 and decodes the body.
func listFiltersGetOK(t *testing.T, params map[string]string) listViewerAssetsResponse {
	t.Helper()
	resp := listFiltersGet(t, params)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	var result listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	return result
}

// listFiltersSetType changes an asset's asset_type.
func listFiltersSetType(t *testing.T, db *store.TestDB, assetID, assetType string) {
	t.Helper()
	_, err := db.DB.Exec(`UPDATE "2".viewer_assets SET asset_type = $2 WHERE id = $1`, assetID, assetType)
	require.NoError(t, err)
}

// patchStatusSend sends PATCH /assets/{id} with {"status": status} as an editor.
func patchStatusSend(t *testing.T, assetID, status string) *events.APIGatewayV2HTTPResponse {
	t.Helper()
	body, _ := json.Marshal(updateViewerAssetRequest{Status: strPtr(status)})
	req := newTestRequest("PATCH", "/assets/"+assetID, "patch-status",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(body))
	resp, err := NewHandler(req, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	return resp
}

// patchStatusStored reads an asset's status straight from the DB.
func patchStatusStored(t *testing.T, db *store.TestDB, assetID string) string {
	t.Helper()
	var status string
	require.NoError(t, db.DB.QueryRow(`SELECT status FROM "2".viewer_assets WHERE id = $1`, assetID).Scan(&status))
	return status
}

// --- scope=all ---

// scope=all returns every asset in the dataset, linked or not, newest first.
// Chat figures are still left out.
func TestListFilters_ScopeAll_LinkedAndUnlinkedNewestFirst(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)
	sessionID := createTestChatSession(t, db, 2, 1)

	unlinked := insertDiscoverTestAsset(t, db, "unlinked", 1, "")
	linkedA := insertDiscoverTestAsset(t, db, "linked-a", 2, "", 5001)
	linkedB := insertDiscoverTestAsset(t, db, "linked-b", 3, "", 5002)
	insertDiscoverTestAsset(t, db, "chat", 4, sessionID)

	result := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "scope": "all"})

	assert.Equal(t, []string{unlinked, linkedA, linkedB}, assetIDs(result))
}

// Assets with the same created_at come back in id order, so pages are stable.
func TestListFilters_ScopeAll_SameCreatedAtOrderedByID(t *testing.T) {
	db := setupAssetsTestDB(t)

	ids := []string{
		insertDiscoverTestAsset(t, db, "one", 1, ""),
		insertDiscoverTestAsset(t, db, "two", 1, ""),
		insertDiscoverTestAsset(t, db, "three", 1, ""),
	}
	_, err := db.DB.Exec(`UPDATE "2".viewer_assets SET created_at = '2026-01-01T00:00:00Z'`)
	require.NoError(t, err)
	sort.Strings(ids)

	result := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "scope": "all"})

	assert.Equal(t, ids, assetIDs(result))
}

// --- 400s ---

// Bad scope, status, limit and offset values are rejected with a 400 whose
// message names the bad param.
func TestListFilters_BadParamsAre400(t *testing.T) {
	setupAssetsTestDB(t)

	cases := []struct {
		name     string
		params   map[string]string
		mentions string
	}{
		{"scope with package_id", map[string]string{"scope": "all", "package_id": "N:package:test-pkg-a"}, "scope"},
		{"unknown scope", map[string]string{"scope": "everything"}, "scope"},
		{"unknown status", map[string]string{"status": "bogus"}, "status"},
		{"status is case sensitive", map[string]string{"status": "Ready"}, "status"},
		{"limit 0", map[string]string{"limit": "0"}, "limit"},
		{"limit over 500", map[string]string{"limit": "501"}, "limit"},
		{"limit not a number", map[string]string{"limit": "abc"}, "limit"},
		{"negative offset", map[string]string{"offset": "-1"}, "offset"},
		{"offset not a number", map[string]string{"offset": "abc"}, "offset"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["dataset_id"] = assetsTestDatasetNodeID
			resp := listFiltersGet(t, tc.params)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, resp.Body, tc.mentions)
		})
	}
}

// --- asset_type ---

// In the default mode, asset_type keeps only unlinked assets of that type.
func TestListFilters_AssetType_DefaultMode(t *testing.T) {
	db := setupAssetsTestDB(t)

	umap := insertDiscoverTestAsset(t, db, "umap", 1, "")
	insertDiscoverTestAsset(t, db, "thumb", 2, "")
	listFiltersSetType(t, db, umap, "umap")

	result := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "asset_type": "umap"})

	assert.Equal(t, []string{umap}, assetIDs(result))
}

// With scope=all, asset_type keeps linked and unlinked assets of that type.
func TestListFilters_AssetType_ScopeAll(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	umapUnlinked := insertDiscoverTestAsset(t, db, "umap-unlinked", 1, "")
	umapLinked := insertDiscoverTestAsset(t, db, "umap-linked", 2, "", 5001)
	insertDiscoverTestAsset(t, db, "thumb-linked", 3, "", 5001)
	listFiltersSetType(t, db, umapUnlinked, "umap")
	listFiltersSetType(t, db, umapLinked, "umap")

	result := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"scope":      "all",
		"asset_type": "umap",
	})

	assert.Equal(t, []string{umapUnlinked, umapLinked}, assetIDs(result))
}

// In package mode, asset_type keeps only the package's assets of that type.
func TestListFilters_AssetType_PackageMode(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	umap := insertDiscoverTestAsset(t, db, "umap-a", 1, "", 5001)
	insertDiscoverTestAsset(t, db, "thumb-a", 2, "", 5001)
	listFiltersSetType(t, db, umap, "umap")

	result := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"package_id": "N:package:test-pkg-a",
		"asset_type": "umap",
	})

	assert.Equal(t, []string{umap}, assetIDs(result))
}

// asset_type is an exact match: "UMAP" does not find "umap".
func TestListFilters_AssetType_ExactMatch(t *testing.T) {
	db := setupAssetsTestDB(t)

	umap := insertDiscoverTestAsset(t, db, "umap", 1, "")
	listFiltersSetType(t, db, umap, "umap")

	result := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"scope":      "all",
		"asset_type": "UMAP",
	})

	assert.Empty(t, result.Assets)
}

// --- status ---

// In the default mode, status=created and status=ready each keep only their own
// unlinked assets.
func TestListFilters_Status_DefaultMode(t *testing.T) {
	db := setupAssetsTestDB(t)

	ready := insertDiscoverTestAsset(t, db, "ready", 1, "")
	created := insertDiscoverTestAsset(t, db, "created", 2, "")
	markDiscoverTestAssetCreated(t, db, created)

	gotCreated := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "status": "created"})
	gotReady := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "status": "ready"})

	assert.Equal(t, []string{created}, assetIDs(gotCreated))
	assert.Equal(t, []string{ready}, assetIDs(gotReady))
}

// With scope=all, status keeps linked and unlinked assets with that status.
func TestListFilters_Status_ScopeAll(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	readyUnlinked := insertDiscoverTestAsset(t, db, "ready-unlinked", 1, "")
	createdUnlinked := insertDiscoverTestAsset(t, db, "created-unlinked", 2, "")
	readyLinked := insertDiscoverTestAsset(t, db, "ready-linked", 3, "", 5001)
	createdLinked := insertDiscoverTestAsset(t, db, "created-linked", 4, "", 5001)
	markDiscoverTestAssetCreated(t, db, createdUnlinked)
	markDiscoverTestAssetCreated(t, db, createdLinked)

	gotCreated := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID, "scope": "all", "status": "created",
	})
	gotReady := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID, "scope": "all", "status": "ready",
	})

	assert.Equal(t, []string{createdUnlinked, createdLinked}, assetIDs(gotCreated))
	assert.Equal(t, []string{readyUnlinked, readyLinked}, assetIDs(gotReady))
}

// In package mode, status keeps only the package's assets with that status.
func TestListFilters_Status_PackageMode(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	insertDiscoverTestAsset(t, db, "ready-a", 1, "", 5001)
	created := insertDiscoverTestAsset(t, db, "created-a", 2, "", 5001)
	markDiscoverTestAssetCreated(t, db, created)

	result := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"package_id": "N:package:test-pkg-a",
		"status":     "created",
	})

	assert.Equal(t, []string{created}, assetIDs(result))
}

// --- limit / offset ---

// limit=2&offset=2 over 5 assets returns the 3rd and 4th newest, and reports
// total_count 5, limit 2 and offset 2.
func TestListFilters_Paging_MiddlePage(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	insertDiscoverTestAsset(t, db, "first", 1, "")
	insertDiscoverTestAsset(t, db, "second", 2, "", 5001)
	third := insertDiscoverTestAsset(t, db, "third", 3, "")
	fourth := insertDiscoverTestAsset(t, db, "fourth", 4, "", 5002)
	insertDiscoverTestAsset(t, db, "fifth", 5, "")

	result := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"scope":      "all",
		"limit":      "2",
		"offset":     "2",
	})

	assert.Equal(t, []string{third, fourth}, assetIDs(result))
	require.NotNil(t, result.TotalCount)
	require.NotNil(t, result.Limit)
	require.NotNil(t, result.Offset)
	assert.Equal(t, 5, *result.TotalCount)
	assert.Equal(t, 2, *result.Limit)
	assert.Equal(t, 2, *result.Offset)
}

// total_count counts only the assets that pass the filters, not the whole
// dataset.
func TestListFilters_Paging_TotalCountRespectsFilters(t *testing.T) {
	db := setupAssetsTestDB(t)

	umap1 := insertDiscoverTestAsset(t, db, "umap-1", 1, "")
	insertDiscoverTestAsset(t, db, "thumb-1", 2, "")
	umap2 := insertDiscoverTestAsset(t, db, "umap-2", 3, "")
	insertDiscoverTestAsset(t, db, "thumb-2", 4, "")
	umap3 := insertDiscoverTestAsset(t, db, "umap-3", 5, "")
	listFiltersSetType(t, db, umap1, "umap")
	listFiltersSetType(t, db, umap2, "umap")
	listFiltersSetType(t, db, umap3, "umap")

	result := listFiltersGetOK(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"scope":      "all",
		"asset_type": "umap",
		"limit":      "2",
	})

	assert.Equal(t, []string{umap1, umap2}, assetIDs(result))
	require.NotNil(t, result.TotalCount)
	assert.Equal(t, 3, *result.TotalCount)
}

// A page past the end has no assets but still reports the real total_count.
func TestListFilters_Paging_OffsetPastEnd(t *testing.T) {
	db := setupAssetsTestDB(t)

	insertDiscoverTestAsset(t, db, "one", 1, "")
	insertDiscoverTestAsset(t, db, "two", 2, "")
	insertDiscoverTestAsset(t, db, "three", 3, "")

	resp := listFiltersGet(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"scope":      "all",
		"offset":     "10",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	var result listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))

	assert.Contains(t, resp.Body, `"assets":[]`, "an empty page should be [], not null")
	require.NotNil(t, result.TotalCount)
	require.NotNil(t, result.Offset)
	assert.Equal(t, 3, *result.TotalCount)
	assert.Equal(t, 10, *result.Offset)
}

// scope=all with no limit pages at 100: with 101 assets it returns 100 and
// reports limit 100, offset 0, total_count 101.
func TestListFilters_Paging_ScopeAllDefaultLimit(t *testing.T) {
	db := setupAssetsTestDB(t)

	_, err := db.DB.Exec(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, status, created_by, created_at)
		SELECT 1, 'bulk-' || n, 'thumb', '{}', 'test-storage-bucket', 'ready', 101, NOW() - make_interval(mins => n)
		FROM generate_series(1, 101) AS n`)
	require.NoError(t, err)

	result := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "scope": "all"})

	assert.Len(t, result.Assets, 100)
	require.NotNil(t, result.TotalCount)
	require.NotNil(t, result.Limit)
	require.NotNil(t, result.Offset)
	assert.Equal(t, 101, *result.TotalCount)
	assert.Equal(t, 100, *result.Limit)
	assert.Equal(t, 0, *result.Offset)
}

// The default (unlinked) mode pages too when limit is passed.
func TestListFilters_Paging_DefaultModeWithLimit(t *testing.T) {
	db := setupAssetsTestDB(t)

	newest := insertDiscoverTestAsset(t, db, "newest", 1, "")
	insertDiscoverTestAsset(t, db, "older", 2, "")

	result := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "limit": "1"})

	assert.Equal(t, []string{newest}, assetIDs(result))
	require.NotNil(t, result.TotalCount)
	assert.Equal(t, 2, *result.TotalCount)
}

// --- packages[] ---

// Each asset's packages[] has the package node ID and name, in the same order
// as package_ids. file_path is discover-only, so it is not in the body.
func TestListFilters_PackagesHaveNames(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	insertDiscoverTestAsset(t, db, "linked-a-and-c", 1, "", 5001, 5003)

	resp := listFiltersGet(t, map[string]string{"dataset_id": assetsTestDatasetNodeID, "scope": "all"})
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	var result listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))

	require.Len(t, result.Assets, 1)
	asset := result.Assets[0]
	assert.Equal(t, []assetPackageResponse{
		{PackageID: "N:package:test-pkg-a", Name: "Test Package 0"},
		{PackageID: "N:package:test-pkg-c", Name: "Test Package 2"},
	}, asset.Packages)
	assert.Equal(t, []string{"N:package:test-pkg-a", "N:package:test-pkg-c"}, asset.PackageIDs)
	assert.NotContains(t, resp.Body, "file_path")
}

// An unlinked asset shows "packages": [] (not null), like package_ids.
func TestListFilters_PackagesEmptyForUnlinked(t *testing.T) {
	db := setupAssetsTestDB(t)

	insertDiscoverTestAsset(t, db, "unlinked", 1, "")

	resp := listFiltersGet(t, map[string]string{"dataset_id": assetsTestDatasetNodeID})
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	assert.Contains(t, resp.Body, `"packages":[]`)
	assert.Contains(t, resp.Body, `"package_ids":[]`)
}

// --- old callers see no change ---

// With no new params, the default mode still lists only unlinked assets.
func TestListFilters_DefaultMode_StillUnlinkedOnly(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	unlinked := insertDiscoverTestAsset(t, db, "unlinked", 1, "")
	insertDiscoverTestAsset(t, db, "linked", 2, "", 5001)

	result := listFiltersGetOK(t, map[string]string{"dataset_id": assetsTestDatasetNodeID})

	assert.Equal(t, []string{unlinked}, assetIDs(result))
}

// A list that wasn't asked to page has no total_count, limit or offset keys in
// the JSON, so it looks exactly like it did before paging existed. Filters on
// their own don't turn paging on.
func TestListFilters_Unpaged_NoPagingKeys(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	insertDiscoverTestAsset(t, db, "unlinked", 1, "")
	insertDiscoverTestAsset(t, db, "linked", 2, "", 5001)

	cases := []struct {
		name   string
		params map[string]string
	}{
		{"default mode", map[string]string{}},
		{"package mode", map[string]string{"package_id": "N:package:test-pkg-a"}},
		{"default mode with filters", map[string]string{"asset_type": "thumb", "status": "ready"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["dataset_id"] = assetsTestDatasetNodeID
			resp := listFiltersGet(t, tc.params)
			require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

			var body map[string]any
			require.NoError(t, json.Unmarshal([]byte(resp.Body), &body))
			assert.Len(t, body["assets"], 1)
			assert.NotContains(t, body, "total_count")
			assert.NotContains(t, body, "limit")
			assert.NotContains(t, body, "offset")
		})
	}
}

// --- PATCH status ---

// PATCH accepts "ready" and "created" and stores them.
func TestPatchStatus_AcceptsCreatedAndReady(t *testing.T) {
	db := setupAssetsTestDB(t)
	assetID := insertDiscoverTestAsset(t, db, "asset", 1, "")
	markDiscoverTestAssetCreated(t, db, assetID)

	resp := patchStatusSend(t, assetID, "ready")
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	assert.Equal(t, "ready", patchStatusStored(t, db, assetID))

	resp = patchStatusSend(t, assetID, "created")
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	assert.Equal(t, "created", patchStatusStored(t, db, assetID))
}

// PATCH rejects any other status with a 400 and leaves the stored status alone.
// The asset starts as "created", so a "Ready" that slipped through as "ready"
// would show up as a change.
func TestPatchStatus_RejectsOtherValues(t *testing.T) {
	db := setupAssetsTestDB(t)
	assetID := insertDiscoverTestAsset(t, db, "asset", 1, "")
	markDiscoverTestAssetCreated(t, db, assetID)

	for _, status := range []string{"Ready", "complete", ""} {
		t.Run("status "+status, func(t *testing.T) {
			resp := patchStatusSend(t, assetID, status)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, resp.Body, invalidStatusMessage)
			assert.Equal(t, "created", patchStatusStored(t, db, assetID))
		})
	}
}
