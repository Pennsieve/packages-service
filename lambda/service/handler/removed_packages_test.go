package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pennsieve/packages-service/api/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In these tests (packages from createTestPackages):
//   - a (5001) is live
//   - b (5002) is in the trash (state DELETED)
//   - c (5003) was replaced by a newer upload (replaced_by_package_id = 5001)
const (
	removedTestPkgA = "N:package:test-pkg-a"
	removedTestPkgB = "N:package:test-pkg-b"
	removedTestPkgC = "N:package:test-pkg-c"
)

// trashPackage puts a package in the trash.
func trashPackage(t *testing.T, db *store.TestDB, packageID int) {
	t.Helper()
	_, err := db.DB.Exec(`UPDATE "2".packages SET state = 'DELETED' WHERE id = $1`, packageID)
	require.NoError(t, err)
}

// replacePackage marks a package as replaced by a newer upload.
func replacePackage(t *testing.T, db *store.TestDB, packageID, replacedByID int) {
	t.Helper()
	_, err := db.DB.Exec(`UPDATE "2".packages SET replaced_by_package_id = $2 WHERE id = $1`, packageID, replacedByID)
	require.NoError(t, err)
}

// setupRemovedPackages creates packages a, b and c, then trashes b and marks c
// as replaced by a.
func setupRemovedPackages(t *testing.T) *store.TestDB {
	t.Helper()
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)
	trashPackage(t, db, 5002)
	replacePackage(t, db, 5003, 5001)
	return db
}

// listWorkspaceAssets calls the authenticated GET /assets as a viewer and
// requires a 200.
func listWorkspaceAssets(t *testing.T, params map[string]string) listViewerAssetsResponse {
	t.Helper()
	req := newTestRequest("GET", "/assets", "removed-list", params, "")
	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	var result listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	return result
}

// TestRemovedPackages_LeftOutOfList checks that listing by a live package only
// reports the live package in package_ids, not the trashed or replaced ones.
func TestRemovedPackages_LeftOutOfList(t *testing.T) {
	db := setupRemovedPackages(t)
	assetID := insertDiscoverTestAsset(t, db, "linked-to-all", 0, "", 5001, 5002, 5003)

	result := listWorkspaceAssets(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"package_id": removedTestPkgA,
	})

	require.Equal(t, []string{assetID}, assetIDs(result))
	assert.Equal(t, []string{removedTestPkgA}, result.Assets[0].PackageIDs)
}

// TestRemovedPackages_LeftOutOfGet checks that GET /assets/{id} only reports
// the live package, in both package_ids and packages.
func TestRemovedPackages_LeftOutOfGet(t *testing.T) {
	db := setupRemovedPackages(t)
	assetID := insertDiscoverTestAsset(t, db, "linked-to-all", 0, "", 5001, 5002, 5003)

	req := newTestRequest("GET", "/assets/"+assetID, "removed-get",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, "")
	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	var result getViewerAssetResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	assert.Equal(t, []string{removedTestPkgA}, result.Asset.PackageIDs)
	assert.Equal(t, []assetPackageResponse{{PackageID: removedTestPkgA, Name: "Test Package 0"}}, result.Asset.Packages)
}

// TestRemovedPackages_LeftOutOfPatchResponse checks that a PATCH which does not
// touch the links still only reports the live package (package_ids and packages).
func TestRemovedPackages_LeftOutOfPatchResponse(t *testing.T) {
	db := setupRemovedPackages(t)
	assetID := insertDiscoverTestAsset(t, db, "linked-to-all", 0, "", 5001, 5002, 5003)

	props := json.RawMessage(`{"colour":"red"}`)
	body, _ := json.Marshal(updateViewerAssetRequest{Properties: &props})
	req := newTestRequest("PATCH", "/assets/"+assetID, "removed-patch",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(body))
	resp, err := NewHandler(req, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	var result viewerAssetResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	assert.Equal(t, []string{removedTestPkgA}, result.PackageIDs)
	assert.Equal(t, []assetPackageResponse{{PackageID: removedTestPkgA, Name: "Test Package 0"}}, result.Packages)
}

// TestRemovedPackages_ListByRemovedPackageIs404 checks that asking for the
// assets of a trashed or replaced package is a 404 (it exists, but is gone),
// not a 400.
func TestRemovedPackages_ListByRemovedPackageIs404(t *testing.T) {
	db := setupRemovedPackages(t)
	insertDiscoverTestAsset(t, db, "linked-to-b-and-c", 0, "", 5002, 5003)

	for name, packageNodeID := range map[string]string{
		"trashed":  removedTestPkgB,
		"replaced": removedTestPkgC,
	} {
		t.Run(name, func(t *testing.T) {
			req := newTestRequest("GET", "/assets", "removed-list-404",
				map[string]string{"dataset_id": assetsTestDatasetNodeID, "package_id": packageNodeID}, "")
			resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, resp.StatusCode, resp.Body)
		})
	}
}

// TestRemovedPackages_ListByUnknownPackageIs400 checks that a package node ID
// that does not exist is still a 400, so it is not confused with a removed one.
func TestRemovedPackages_ListByUnknownPackageIs400(t *testing.T) {
	setupRemovedPackages(t)

	req := newTestRequest("GET", "/assets", "removed-list-400",
		map[string]string{"dataset_id": assetsTestDatasetNodeID, "package_id": "N:package:does-not-exist"}, "")
	resp, err := NewHandler(req, assetsViewerClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
}

// TestRemovedPackages_RestoreBringsLinksBack checks that the links of a trashed
// package are kept, so taking it out of the trash shows them again.
func TestRemovedPackages_RestoreBringsLinksBack(t *testing.T) {
	db := setupRemovedPackages(t)
	assetID := insertDiscoverTestAsset(t, db, "linked-to-a-and-b", 0, "", 5001, 5002)

	// While b is in the trash, only a is reported.
	before := listWorkspaceAssets(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"package_id": removedTestPkgA,
	})
	require.Equal(t, []string{assetID}, assetIDs(before))
	require.Equal(t, []string{removedTestPkgA}, before.Assets[0].PackageIDs)

	// Restore b.
	_, err := db.DB.Exec(`UPDATE "2".packages SET state = 'READY' WHERE id = 5002`)
	require.NoError(t, err)

	// Listing by b works again, and both packages are reported.
	after := listWorkspaceAssets(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"package_id": removedTestPkgB,
	})
	require.Equal(t, []string{assetID}, assetIDs(after))
	assert.Equal(t, []string{removedTestPkgA, removedTestPkgB}, after.Assets[0].PackageIDs)
}

// TestRemovedPackages_DiscoverIgnoresRemoval checks that discover does not look
// at the package's state in the workspace: once published, a package that is
// later trashed or replaced still lists its asset and is still reported.
func TestRemovedPackages_DiscoverIgnoresRemoval(t *testing.T) {
	cases := map[string]func(t *testing.T, db *store.TestDB){
		"trashed":  func(t *testing.T, db *store.TestDB) { trashPackage(t, db, 5001) },
		"replaced": func(t *testing.T, db *store.TestDB) { replacePackage(t, db, 5001, 5002) },
	}
	for name, removeA := range cases {
		t.Run(name, func(t *testing.T) {
			db := setupAssetsTestDB(t)
			setupDiscoverStub(t, db)
			createTestPackages(t, db, 2, 1)
			assetID := insertDiscoverTestAsset(t, db, "thumb-a", 0, "", 5001)
			publishDiscoverVersion(t, db, 1, publishSucceededStatus, removedTestPkgA)

			removeA(t, db)

			result := listDiscoverAssets(t, map[string]string{"package_id": removedTestPkgA})
			require.Equal(t, []string{assetID}, assetIDs(result))
			assert.Equal(t, []string{removedTestPkgA}, result.Assets[0].PackageIDs)
		})
	}
}

// TestRemovedPackages_ScopeAll checks the authenticated scope=all list: an
// asset linked only to removed packages is left out, an asset linked to a live
// and a removed package is kept (reporting only the live one), and an unlinked
// asset is kept.
func TestRemovedPackages_ScopeAll(t *testing.T) {
	db := setupRemovedPackages(t)

	// Newest first.
	unlinkedID := insertDiscoverTestAsset(t, db, "unlinked", 1, "")
	mixedID := insertDiscoverTestAsset(t, db, "linked-to-a-and-b", 2, "", 5001, 5002)
	insertDiscoverTestAsset(t, db, "linked-to-b-and-c", 3, "", 5002, 5003)

	result := listWorkspaceAssets(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"scope":      "all",
	})

	require.Equal(t, []string{unlinkedID, mixedID}, assetIDs(result))
	assert.Equal(t, []string{}, result.Assets[0].PackageIDs)
	assert.Equal(t, []string{removedTestPkgA}, result.Assets[1].PackageIDs)
}

// TestRemovedPackages_PatchLinksKeepsRemovedLinks checks that replacing an
// asset's package_ids keeps its links to removed packages. The client never
// sees those links, so it can't send them back; dropping them would stop a
// restore from the trash from bringing the asset back.
func TestRemovedPackages_PatchLinksKeepsRemovedLinks(t *testing.T) {
	db := setupRemovedPackages(t)
	assetID := insertDiscoverTestAsset(t, db, "linked-to-all", 0, "", 5001, 5002, 5003)

	// The client saw package_ids [a] and sends it back unchanged.
	body, _ := json.Marshal(updateViewerAssetRequest{PackageIDs: &[]string{removedTestPkgA}})
	req := newTestRequest("PATCH", "/assets/"+assetID, "removed-patch-links",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(body))
	resp, err := NewHandler(req, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	// Restore b from the trash and un-replace c: both links are still there.
	_, err = db.DB.Exec(`UPDATE "2".packages SET state = 'READY' WHERE id = 5002`)
	require.NoError(t, err)
	_, err = db.DB.Exec(`UPDATE "2".packages SET replaced_by_package_id = NULL WHERE id = 5003`)
	require.NoError(t, err)

	result := listWorkspaceAssets(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"package_id": removedTestPkgA,
	})
	require.Equal(t, []string{assetID}, assetIDs(result))
	assert.Equal(t, []string{removedTestPkgA, removedTestPkgB, removedTestPkgC}, result.Assets[0].PackageIDs)
}

// TestRemovedPackages_ScopeAllTotalCount checks that total_count leaves out an
// asset whose links are all to removed packages, the same as the list does.
func TestRemovedPackages_ScopeAllTotalCount(t *testing.T) {
	db := setupRemovedPackages(t)
	unlinkedID := insertDiscoverTestAsset(t, db, "unlinked", 1, "")
	insertDiscoverTestAsset(t, db, "removed-only", 2, "", 5002, 5003)
	insertDiscoverTestAsset(t, db, "live-and-removed", 3, "", 5001, 5002)

	result := listWorkspaceAssets(t, map[string]string{
		"dataset_id": assetsTestDatasetNodeID,
		"scope":      "all",
		"limit":      "1",
	})

	require.Equal(t, []string{unlinkedID}, assetIDs(result))
	require.NotNil(t, result.TotalCount)
	assert.Equal(t, 2, *result.TotalCount, "the removed-only asset must not be counted")
}

// storedLinks returns the package IDs an asset is linked to in the DB, sorted.
func storedLinks(t *testing.T, db *store.TestDB, assetID string) []int {
	t.Helper()
	rows, err := db.DB.Query(`SELECT package_id FROM "2".viewer_asset_packages WHERE viewer_asset_id = $1 ORDER BY package_id`, assetID)
	require.NoError(t, err)
	defer rows.Close()
	links := []int{}
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		links = append(links, id)
	}
	require.NoError(t, rows.Err())
	return links
}

// sendAssetRequest sends a POST or PATCH as an editor and returns the status and body.
func sendAssetRequest(t *testing.T, method, path string, body any) (int, string) {
	t.Helper()
	bodyJSON, err := json.Marshal(body)
	require.NoError(t, err)
	req := newTestRequest(method, path, "removed-write",
		map[string]string{"dataset_id": assetsTestDatasetNodeID}, string(bodyJSON))
	resp, err := NewHandler(req, assetsEditorClaims()).WithDefaultService().handle(context.Background())
	require.NoError(t, err)
	return resp.StatusCode, resp.Body
}

// TestRemovedPackages_CreateWithRemovedPackageIs404 checks that POST /assets
// linking a trashed or replaced package is a 404 naming it, and creates no asset.
func TestRemovedPackages_CreateWithRemovedPackageIs404(t *testing.T) {
	cases := []struct{ name, nodeID string }{
		{name: "trashed", nodeID: removedTestPkgB},
		{name: "replaced", nodeID: removedTestPkgC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupRemovedPackages(t)

			status, body := sendAssetRequest(t, "POST", "/assets", createViewerAssetRequest{
				Name: "new-asset", AssetType: "thumb", PackageIDs: []string{removedTestPkgA, tc.nodeID},
			})

			assert.Equal(t, http.StatusNotFound, status, body)
			assert.Contains(t, body, tc.nodeID)
			assert.NotContains(t, body, removedTestPkgA, "only the removed package is named")

			var count int
			require.NoError(t, db.DB.QueryRow(`SELECT COUNT(*) FROM "2".viewer_assets`).Scan(&count))
			assert.Zero(t, count, "no asset should be created")
		})
	}
}

// TestRemovedPackages_ResolvePackageIDs checks the lookup POST and PATCH share:
// live packages resolve, unknown ones are skipped as before, and any removed
// package fails the whole call, naming every removed package. (POST can't run
// end to end here: it needs STS for the upload credentials.)
func TestRemovedPackages_ResolvePackageIDs(t *testing.T) {
	setupRemovedPackages(t)
	h := &ViewerAssetsHandler{}
	ctx := context.Background()

	ids, err := h.resolvePackageIDs(ctx, assetsTestOrgID, 1, []string{removedTestPkgA, "N:package:does-not-exist"})
	require.NoError(t, err)
	assert.Equal(t, []int64{5001}, ids)

	_, err = h.resolvePackageIDs(ctx, assetsTestOrgID, 1, []string{removedTestPkgC, removedTestPkgA, removedTestPkgB})
	assert.ErrorIs(t, err, errPackageRemoved)
	assert.ErrorContains(t, err, removedTestPkgB+", "+removedTestPkgC)
}

// TestRemovedPackages_PatchWithRemovedPackageIs404 checks that a PATCH linking a
// trashed or replaced package is a 404 and changes nothing: not the links, and
// not the other fields sent in the same request.
func TestRemovedPackages_PatchWithRemovedPackageIs404(t *testing.T) {
	cases := []struct{ name, nodeID string }{
		{name: "trashed", nodeID: removedTestPkgB},
		{name: "replaced", nodeID: removedTestPkgC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupRemovedPackages(t)
			assetID := insertDiscoverTestAsset(t, db, "linked-to-a", 0, "", 5001)

			status, body := sendAssetRequest(t, "PATCH", "/assets/"+assetID, updateViewerAssetRequest{
				Status:     strPtr("created"),
				PackageIDs: &[]string{tc.nodeID},
			})

			assert.Equal(t, http.StatusNotFound, status, body)
			assert.Contains(t, body, tc.nodeID)
			assert.Equal(t, []int{5001}, storedLinks(t, db, assetID), "links must not change")
			var stored string
			require.NoError(t, db.DB.QueryRow(`SELECT status FROM "2".viewer_assets WHERE id = $1`, assetID).Scan(&stored))
			assert.Equal(t, "ready", stored, "status must not change")
		})
	}
}

// TestRemovedPackages_PatchWithLivePackageWorks checks that linking a live
// package with PATCH still works.
func TestRemovedPackages_PatchWithLivePackageWorks(t *testing.T) {
	db := setupRemovedPackages(t)
	assetID := insertDiscoverTestAsset(t, db, "unlinked", 0, "")

	status, body := sendAssetRequest(t, "PATCH", "/assets/"+assetID, updateViewerAssetRequest{
		PackageIDs: &[]string{removedTestPkgA},
	})

	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, []int{5001}, storedLinks(t, db, assetID))
}
