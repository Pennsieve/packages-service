package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/pennsieve/packages-service/api/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// discoverTestPublishedDatasetID is the discover.public_datasets.id that the stub
// maps to source dataset 1 in org 2.
const discoverTestPublishedDatasetID = 77

// setupDiscoverStub creates a minimal discover schema (only the columns the
// handler reads) in the test database and points DiscoverDB at it. Published
// dataset 77 maps to org 2 / dataset 1.
func setupDiscoverStub(t *testing.T, db *store.TestDB) {
	t.Helper()
	// Drop first: a run that was interrupted before its cleanup leaves the
	// schema behind, and that must not break the next run.
	_, err := db.DB.Exec(`
		DROP SCHEMA IF EXISTS discover CASCADE;
		CREATE SCHEMA discover;
		CREATE TABLE discover.public_datasets (
			id INT PRIMARY KEY,
			source_organization_id INT NOT NULL,
			source_dataset_id INT NOT NULL
		);
		CREATE TABLE discover.public_dataset_versions (
			dataset_id INT NOT NULL REFERENCES discover.public_datasets(id),
			version INT NOT NULL,
			status VARCHAR(255) NOT NULL,
			migrated BOOLEAN NOT NULL DEFAULT false,
			UNIQUE (dataset_id, version)
		);
		CREATE TABLE discover.public_file_versions (
			id SERIAL PRIMARY KEY,
			source_package_id TEXT,
			s3_key TEXT NOT NULL,
			dataset_id INT NOT NULL
		);
		CREATE TABLE discover.public_dataset_version_files (
			dataset_id INT NOT NULL,
			dataset_version INT NOT NULL,
			file_id INT NOT NULL REFERENCES discover.public_file_versions(id)
		);`)
	require.NoError(t, err)
	_, err = db.DB.Exec(`INSERT INTO discover.public_datasets (id, source_organization_id, source_dataset_id) VALUES ($1, 2, 1)`,
		discoverTestPublishedDatasetID)
	require.NoError(t, err)

	originalDiscoverDB := DiscoverDB
	DiscoverDB = db.DB
	// Registered after setupAssetsTestDB's cleanup, so it runs before the
	// connection is closed.
	t.Cleanup(func() {
		DiscoverDB = originalDiscoverDB
		db.DB.Exec(`DROP SCHEMA IF EXISTS discover CASCADE`)
	})
}

// publishDiscoverVersion records a version of the stub published dataset with
// the given status, containing one file for each package node ID.
func publishDiscoverVersion(t *testing.T, db *store.TestDB, version int, status string, packageNodeIDs ...string) {
	t.Helper()
	_, err := db.DB.Exec(`INSERT INTO discover.public_dataset_versions (dataset_id, version, status) VALUES ($1, $2, $3)`,
		discoverTestPublishedDatasetID, version, status)
	require.NoError(t, err)
	for _, nodeID := range packageNodeIDs {
		var fileID int
		require.NoError(t, db.DB.QueryRow(`
			INSERT INTO discover.public_file_versions (source_package_id, s3_key, dataset_id)
			VALUES ($1, $2, $3) RETURNING id`,
			nodeID, fmt.Sprintf("%d/%d/files/%s.png", discoverTestPublishedDatasetID, version, nodeID),
			discoverTestPublishedDatasetID).Scan(&fileID))
		_, err = db.DB.Exec(`INSERT INTO discover.public_dataset_version_files (dataset_id, dataset_version, file_id) VALUES ($1, $2, $3)`,
			discoverTestPublishedDatasetID, version, fileID)
		require.NoError(t, err)
	}
}

// insertDiscoverTestAsset inserts a ready viewer asset in org 2 / dataset 1,
// created minutesAgo minutes ago, linked to the given package integer IDs.
// chatSessionID may be empty. Returns the asset ID. Ready, because discover only
// lists ready assets.
func insertDiscoverTestAsset(t *testing.T, db *store.TestDB, name string, minutesAgo int, chatSessionID string, packageIntIDs ...int) string {
	t.Helper()
	var chat interface{}
	if chatSessionID != "" {
		chat = chatSessionID
	}
	var id string
	require.NoError(t, db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, status, created_by, chat_session_id, created_at)
		VALUES (1, $1, 'thumb', '{}', 'test-storage-bucket', 'ready', 101, $2, NOW() - make_interval(mins => $3)) RETURNING id`,
		name, chat, minutesAgo).Scan(&id))
	for _, pkgID := range packageIntIDs {
		_, err := db.DB.Exec(`INSERT INTO "2".viewer_asset_packages (viewer_asset_id, package_id) VALUES ($1, $2)`, id, pkgID)
		require.NoError(t, err)
	}
	return id
}

func listDiscoverAssets(t *testing.T, params map[string]string) listViewerAssetsResponse {
	t.Helper()
	req := newTestRequest("GET", "/discover/assets", "discover-list", params, "")
	resp, err := NewDiscoverHandler(req).handleDiscover(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	var result listViewerAssetsResponse
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
	return result
}

// discoverListStatus calls GET /discover/assets and returns the status code and
// body, for requests that are expected to fail (listDiscoverAssets requires 200).
func discoverListStatus(t *testing.T, params map[string]string) (int, string) {
	t.Helper()
	req := newTestRequest("GET", "/discover/assets", "discover-list-status", params, "")
	resp, err := NewDiscoverHandler(req).handleDiscover(context.Background())
	require.NoError(t, err)
	return resp.StatusCode, resp.Body
}

// addDiscoverFile adds one file with the given s3_key for a package to an
// existing version of the stub published dataset.
func addDiscoverFile(t *testing.T, db *store.TestDB, version int, packageNodeID, s3Key string) {
	t.Helper()
	var fileID int
	require.NoError(t, db.DB.QueryRow(`
		INSERT INTO discover.public_file_versions (source_package_id, s3_key, dataset_id)
		VALUES ($1, $2, $3) RETURNING id`,
		packageNodeID, s3Key, discoverTestPublishedDatasetID).Scan(&fileID))
	_, err := db.DB.Exec(`INSERT INTO discover.public_dataset_version_files (dataset_id, dataset_version, file_id) VALUES ($1, $2, $3)`,
		discoverTestPublishedDatasetID, version, fileID)
	require.NoError(t, err)
}

// markDiscoverVersionMigrated sets migrated = true on a version of the stub
// published dataset. A migrated version's s3_keys have no version folder.
func markDiscoverVersionMigrated(t *testing.T, db *store.TestDB, version int) {
	t.Helper()
	_, err := db.DB.Exec(`UPDATE discover.public_dataset_versions SET migrated = true WHERE dataset_id = $1 AND version = $2`,
		discoverTestPublishedDatasetID, version)
	require.NoError(t, err)
}

// setDiscoverTestAssetType changes an asset's type (insertDiscoverTestAsset
// always makes 'thumb' assets).
func setDiscoverTestAssetType(t *testing.T, db *store.TestDB, assetID, assetType string) {
	t.Helper()
	_, err := db.DB.Exec(`UPDATE "2".viewer_assets SET asset_type = $1 WHERE id = $2`, assetType, assetID)
	require.NoError(t, err)
}

// assertDiscoverPage checks a paged list's total_count, limit and offset.
func assertDiscoverPage(t *testing.T, result listViewerAssetsResponse, totalCount, limit, offset int) {
	t.Helper()
	require.NotNil(t, result.TotalCount, "total_count")
	require.NotNil(t, result.Limit, "limit")
	require.NotNil(t, result.Offset, "offset")
	assert.Equal(t, totalCount, *result.TotalCount, "total_count")
	assert.Equal(t, limit, *result.Limit, "limit")
	assert.Equal(t, offset, *result.Offset, "offset")
}

// discoverAssetFixtures creates packages a (5001), b (5002) and c (5003) and these
// assets, newest first:
//   - unlinked:  no package links
//   - thumb-a:   linked to a
//   - thumb-b:   linked to b
//   - multi-ac:  linked to a and c
//   - chat:      chat-scoped, no package links
func discoverAssetFixtures(t *testing.T, db *store.TestDB) map[string]string {
	t.Helper()
	createTestPackages(t, db, 2, 1)
	sessionID := createTestChatSession(t, db, 2, 1)
	return map[string]string{
		"unlinked": insertDiscoverTestAsset(t, db, "unlinked", 1, ""),
		"thumb-a":  insertDiscoverTestAsset(t, db, "thumb-a", 2, "", 5001),
		"thumb-b":  insertDiscoverTestAsset(t, db, "thumb-b", 3, "", 5002),
		"multi-ac": insertDiscoverTestAsset(t, db, "multi-ac", 4, "", 5001, 5003),
		"chat":     insertDiscoverTestAsset(t, db, "chat", 5, sessionID),
	}
}

func assetIDs(result listViewerAssetsResponse) []string {
	ids := make([]string, len(result.Assets))
	for i, a := range result.Assets {
		ids[i] = a.ID
	}
	return ids
}

// TestDiscoverListAssets_ScopeAll verifies scope=all returns unlinked assets plus
// assets linked to a published package, newest first; drops assets whose linked
// packages are unpublished or only in a failed version; reports only published
// package links; and excludes chat-scoped assets.
func TestDiscoverListAssets_ScopeAll(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	ids := discoverAssetFixtures(t, db)

	// a is published; c is only in a failed version; b was never published.
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")
	publishDiscoverVersion(t, db, 2, "PUBLISH_FAILED", "N:package:test-pkg-a", "N:package:test-pkg-c")

	result := listDiscoverAssets(t, map[string]string{
		"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
		"scope":      "all",
	})

	assert.Equal(t, []string{ids["unlinked"], ids["thumb-a"], ids["multi-ac"]}, assetIDs(result))
	byID := map[string]viewerAssetResponse{}
	for _, a := range result.Assets {
		byID[a.ID] = a
	}
	assert.Equal(t, []string{}, byID[ids["unlinked"]].PackageIDs)
	assert.Equal(t, []string{"N:package:test-pkg-a"}, byID[ids["thumb-a"]].PackageIDs)
	assert.Equal(t, []string{"N:package:test-pkg-a"}, byID[ids["multi-ac"]].PackageIDs,
		"unpublished package c must not be reported")
	assert.Equal(t, assetsTestDatasetNodeID, byID[ids["thumb-a"]].DatasetID)
}

// TestDiscoverListAssets_ScopeAll_NoPublishedVersion verifies that a dataset
// with no version at all is a 404 for scope=all: nothing of it is public yet,
// not even its unlinked assets.
func TestDiscoverListAssets_ScopeAll_NoPublishedVersion(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	discoverAssetFixtures(t, db)

	status, body := discoverListStatus(t, map[string]string{
		"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
		"scope":      "all",
	})
	assert.Equal(t, http.StatusNotFound, status, body)
}

// TestDiscoverListAssets_DatasetMode_NeedsSucceededVersion verifies dataset mode
// (default and scope=all) is a 404 unless the dataset has a PUBLISH_SUCCEEDED
// version. A public_datasets row alone can be a failed or embargoed publish.
func TestDiscoverListAssets_DatasetMode_NeedsSucceededVersion(t *testing.T) {
	cases := []struct {
		name       string
		status     string
		wantStatus int
	}{
		{name: "only a failed version", status: "PUBLISH_FAILED", wantStatus: http.StatusNotFound},
		{name: "only an embargoed version", status: "EMBARGO_SUCCEEDED", wantStatus: http.StatusNotFound},
		{name: "a succeeded version", status: publishSucceededStatus, wantStatus: http.StatusOK},
	}
	for _, tc := range cases {
		for _, scope := range []string{"", "all"} {
			t.Run(tc.name+" scope="+scope, func(t *testing.T) {
				db := setupAssetsTestDB(t)
				setupDiscoverStub(t, db)
				insertDiscoverTestAsset(t, db, "unlinked", 1, "")
				publishDiscoverVersion(t, db, 1, tc.status)

				params := map[string]string{"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID)}
				if scope != "" {
					params["scope"] = scope
				}
				status, body := discoverListStatus(t, params)
				assert.Equal(t, tc.wantStatus, status, body)
			})
		}
	}
}

// TestDiscoverListAssets_DatasetDefault_UnlinkedOnly verifies dataset mode without
// scope returns only assets with no package links, and excludes chat-scoped
// assets.
func TestDiscoverListAssets_DatasetDefault_UnlinkedOnly(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	ids := discoverAssetFixtures(t, db)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")

	result := listDiscoverAssets(t, map[string]string{
		"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
	})
	assert.Equal(t, []string{ids["unlinked"]}, assetIDs(result))
}

// TestDiscoverListAssets_DatasetDefault_EmptyLinksAndVersion verifies the default
// dataset mode, which skips the package-link lookup, still returns empty
// package_ids and packages (not null) and the latest published version.
func TestDiscoverListAssets_DatasetDefault_EmptyLinksAndVersion(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	ids := discoverAssetFixtures(t, db)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")
	publishDiscoverVersion(t, db, 2, publishSucceededStatus, "N:package:test-pkg-a")

	result := listDiscoverAssets(t, map[string]string{
		"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
	})

	require.Equal(t, []string{ids["unlinked"]}, assetIDs(result))
	assert.Equal(t, []string{}, result.Assets[0].PackageIDs)
	assert.Equal(t, []assetPackageResponse{}, result.Assets[0].Packages)
	assert.Equal(t, 2, result.Version)
}

// TestDiscoverListAssets_PackageMode_LeavesOutUnpublishedPackageIDs verifies
// package mode returns the package's assets, but their package_ids leave out a
// linked package (c) that was never published.
func TestDiscoverListAssets_PackageMode_LeavesOutUnpublishedPackageIDs(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	ids := discoverAssetFixtures(t, db)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")

	result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})

	require.Equal(t, []string{ids["thumb-a"], ids["multi-ac"]}, assetIDs(result))
	assert.Equal(t, []string{"N:package:test-pkg-a"}, result.Assets[0].PackageIDs)
	assert.Equal(t, []string{"N:package:test-pkg-a"}, result.Assets[1].PackageIDs,
		"c is linked to multi-ac but never published")
}

// TestDiscoverListAssets_PackageMode_ReturnsEveryPublishedPackage verifies an
// asset linked to several published packages reports all of them, in both
// package_ids and packages, while an unpublished link (b) is left out.
func TestDiscoverListAssets_PackageMode_ReturnsEveryPublishedPackage(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)
	assetID := insertDiscoverTestAsset(t, db, "multi-abc", 1, "", 5001, 5002, 5003)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a", "N:package:test-pkg-c")

	result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})

	require.Equal(t, []string{assetID}, assetIDs(result))
	assert.Equal(t, []string{"N:package:test-pkg-a", "N:package:test-pkg-c"}, result.Assets[0].PackageIDs)
	assert.Equal(t, []assetPackageResponse{
		{PackageID: "N:package:test-pkg-a", Name: "Test Package 0", FilePath: "files/N:package:test-pkg-a.png"},
		{PackageID: "N:package:test-pkg-c", Name: "Test Package 2", FilePath: "files/N:package:test-pkg-c.png"},
	}, result.Assets[0].Packages)
}

// TestDiscoverListAssets_PackageMode_OnlyInFailedVersion verifies a package
// whose only file is in a failed version is not public: package mode is a 404.
func TestDiscoverListAssets_PackageMode_OnlyInFailedVersion(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	discoverAssetFixtures(t, db)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")
	publishDiscoverVersion(t, db, 2, "PUBLISH_FAILED", "N:package:test-pkg-a", "N:package:test-pkg-c")

	status, _ := discoverListStatus(t, map[string]string{"package_id": "N:package:test-pkg-c"})
	assert.Equal(t, http.StatusNotFound, status)
}

// TestDiscoverListAssets_PackageOnlyInOlderVersion verifies a package that was
// in an older succeeded version but is gone from the latest one counts as
// unpublished in every mode. c is in version 1 only; a is in both.
func TestDiscoverListAssets_PackageOnlyInOlderVersion(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)
	multiID := insertDiscoverTestAsset(t, db, "multi-ac", 1, "", 5001, 5003)
	insertDiscoverTestAsset(t, db, "only-c", 2, "", 5003)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a", "N:package:test-pkg-c")
	publishDiscoverVersion(t, db, 2, publishSucceededStatus, "N:package:test-pkg-a")

	t.Run("package mode is a 404", func(t *testing.T) {
		status, _ := discoverListStatus(t, map[string]string{"package_id": "N:package:test-pkg-c"})
		assert.Equal(t, http.StatusNotFound, status)
	})

	t.Run("scope=all drops the asset linked only to it and trims it from the other", func(t *testing.T) {
		result := listDiscoverAssets(t, map[string]string{
			"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
			"scope":      "all",
		})
		require.Equal(t, []string{multiID}, assetIDs(result))
		assert.Equal(t, []string{"N:package:test-pkg-a"}, result.Assets[0].PackageIDs)
	})

	t.Run("package mode trims it from package_ids", func(t *testing.T) {
		result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})
		require.Equal(t, []string{multiID}, assetIDs(result))
		assert.Equal(t, []string{"N:package:test-pkg-a"}, result.Assets[0].PackageIDs)
	})
}

// TestDiscoverListAssets_FilePath_UnmigratedVersion verifies that for an
// unmigrated version, file_path drops the "{dataset id}/{version}/" prefix of
// the s3_key, and the response reports the latest succeeded version.
func TestDiscoverListAssets_FilePath_UnmigratedVersion(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)
	insertDiscoverTestAsset(t, db, "thumb-a", 1, "", 5001)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")
	publishDiscoverVersion(t, db, 2, publishSucceededStatus, "N:package:test-pkg-a")

	result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})

	assert.Equal(t, 2, result.Version)
	require.Len(t, result.Assets, 1)
	require.Len(t, result.Assets[0].Packages, 1)
	assert.Equal(t, "files/N:package:test-pkg-a.png", result.Assets[0].Packages[0].FilePath)
}

// TestDiscoverListAssets_FilePath_MigratedVersion verifies that for a migrated
// version, whose s3_keys have no version folder, file_path drops only the
// "{dataset id}/" prefix.
func TestDiscoverListAssets_FilePath_MigratedVersion(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)
	insertDiscoverTestAsset(t, db, "thumb-a", 1, "", 5001)
	publishDiscoverVersion(t, db, 3, publishSucceededStatus)
	markDiscoverVersionMigrated(t, db, 3)
	addDiscoverFile(t, db, 3, "N:package:test-pkg-a", "77/files/scan.png")

	result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})

	assert.Equal(t, 3, result.Version)
	require.Len(t, result.Assets, 1)
	require.Len(t, result.Assets[0].Packages, 1)
	assert.Equal(t, "files/scan.png", result.Assets[0].Packages[0].FilePath)
}

// TestDiscoverListAssets_FilePath_FirstS3KeyWins verifies a package with two
// files in the version gets the first s3_key in sort order, not the first one
// inserted, so its file_path is stable.
func TestDiscoverListAssets_FilePath_FirstS3KeyWins(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)
	insertDiscoverTestAsset(t, db, "thumb-a", 1, "", 5001)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus)
	addDiscoverFile(t, db, 1, "N:package:test-pkg-a", "77/1/files/z-scan.png")
	addDiscoverFile(t, db, 1, "N:package:test-pkg-a", "77/1/files/a-scan.png")

	result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})

	require.Len(t, result.Assets, 1)
	require.Len(t, result.Assets[0].Packages, 1)
	assert.Equal(t, "files/a-scan.png", result.Assets[0].Packages[0].FilePath)
}

// TestDiscoverListAssets_ScopeAll_Paging verifies scope=all pages over only the
// assets it returns: assets linked to an unpublished package (b) take no page
// slots and are not in total_count. If the filter ran after paging, the first
// page would come back short.
func TestDiscoverListAssets_ScopeAll_Paging(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")

	// Newest first. The two b assets must never show up or be counted.
	unlinked1 := insertDiscoverTestAsset(t, db, "unlinked-1", 1, "")
	insertDiscoverTestAsset(t, db, "thumb-b-1", 2, "", 5002)
	thumbA := insertDiscoverTestAsset(t, db, "thumb-a", 3, "", 5001)
	insertDiscoverTestAsset(t, db, "thumb-b-2", 4, "", 5002)
	unlinked2 := insertDiscoverTestAsset(t, db, "unlinked-2", 5, "")

	page := func(t *testing.T, extra map[string]string) listViewerAssetsResponse {
		params := map[string]string{"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID), "scope": "all"}
		for k, v := range extra {
			params[k] = v
		}
		return listDiscoverAssets(t, params)
	}

	t.Run("first page", func(t *testing.T) {
		result := page(t, map[string]string{"limit": "2"})
		assert.Equal(t, []string{unlinked1, thumbA}, assetIDs(result))
		assertDiscoverPage(t, result, 3, 2, 0)
	})

	t.Run("second page", func(t *testing.T) {
		result := page(t, map[string]string{"limit": "2", "offset": "2"})
		assert.Equal(t, []string{unlinked2}, assetIDs(result))
		assertDiscoverPage(t, result, 3, 2, 2)
	})

	t.Run("no limit reports the default of 100", func(t *testing.T) {
		result := page(t, nil)
		assert.Equal(t, []string{unlinked1, thumbA, unlinked2}, assetIDs(result))
		assertDiscoverPage(t, result, 3, 100, 0)
	})
}

// TestDiscoverListAssets_AssetTypeFilter verifies asset_type keeps only assets
// of exactly that type.
func TestDiscoverListAssets_AssetTypeFilter(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	insertDiscoverTestAsset(t, db, "thumb", 1, "")
	zarrID := insertDiscoverTestAsset(t, db, "zarr", 2, "")
	setDiscoverTestAssetType(t, db, zarrID, "ome-zarr")
	// Dataset mode needs a succeeded version to list anything.
	publishDiscoverVersion(t, db, 1, publishSucceededStatus)

	result := listDiscoverAssets(t, map[string]string{
		"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
		"asset_type": "ome-zarr",
	})
	assert.Equal(t, []string{zarrID}, assetIDs(result))
}

// TestDiscoverListAssets_PageValidation verifies a bad limit or offset is a 400
// that names the bad param, before any DB lookup.
func TestDiscoverListAssets_PageValidation(t *testing.T) {
	cases := []struct {
		name, param, value string
	}{
		{name: "limit 0", param: "limit", value: "0"},
		{name: "limit over the max", param: "limit", value: "501"},
		{name: "limit not a number", param: "limit", value: "ten"},
		{name: "negative offset", param: "offset", value: "-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := discoverListStatus(t, map[string]string{"dataset_id": "42", tc.param: tc.value})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, body, tc.param)
		})
	}
}

// TestDiscoverListAssets_ScopeValidation verifies scope is rejected when it is not
// 'all' or is combined with package_id, before any DB lookup.
func TestDiscoverListAssets_ScopeValidation(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]string
	}{
		{name: "unknown scope", params: map[string]string{"dataset_id": "42", "scope": "everything"}},
		{name: "scope with package_id", params: map[string]string{"package_id": "N:package:abc", "scope": "all"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := newTestRequest("GET", "/discover/assets", "discover-scope", tc.params, "")
			resp, err := NewDiscoverHandler(req).handleDiscover(context.Background())
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, resp.Body, "scope")
		})
	}
}

// markDiscoverTestAssetCreated sets an asset's status back to 'created' (the DB
// default), which is what an asset still being uploaded looks like.
func markDiscoverTestAssetCreated(t *testing.T, db *store.TestDB, assetID string) {
	t.Helper()
	_, err := db.DB.Exec(`UPDATE "2".viewer_assets SET status = 'created' WHERE id = $1`, assetID)
	require.NoError(t, err)
}

// TestDiscoverListAssets_PackageMode_ExcludesChatFigures verifies package mode
// leaves out a chat figure even when it is linked to the package, while a
// normal asset linked to the same package is still returned.
func TestDiscoverListAssets_PackageMode_ExcludesChatFigures(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)
	sessionID := createTestChatSession(t, db, 2, 1)

	// Both assets are linked to package a (5001); only the second is a chat figure.
	thumbID := insertDiscoverTestAsset(t, db, "thumb-a", 1, "", 5001)
	insertDiscoverTestAsset(t, db, "chat-a", 2, sessionID, 5001)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")

	result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})

	assert.Equal(t, []string{thumbID}, assetIDs(result))
}

// TestDiscoverListAssets_ExcludesCreatedAssets verifies every discover mode
// (package mode, default dataset mode, scope=all) leaves out assets still in
// 'created' status. Each mode has a ready asset in the same position (same
// links) that is returned, so only the status differs.
func TestDiscoverListAssets_ExcludesCreatedAssets(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	createTestPackages(t, db, 2, 1)

	// Newest first: two unlinked assets, then two linked to package a (5001).
	// In each pair the first is ready and the second is still 'created'.
	readyUnlinkedID := insertDiscoverTestAsset(t, db, "ready-unlinked", 1, "")
	createdUnlinkedID := insertDiscoverTestAsset(t, db, "created-unlinked", 2, "")
	readyLinkedID := insertDiscoverTestAsset(t, db, "ready-a", 3, "", 5001)
	createdLinkedID := insertDiscoverTestAsset(t, db, "created-a", 4, "", 5001)
	markDiscoverTestAssetCreated(t, db, createdUnlinkedID)
	markDiscoverTestAssetCreated(t, db, createdLinkedID)

	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")

	t.Run("package mode", func(t *testing.T) {
		result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})
		assert.Equal(t, []string{readyLinkedID}, assetIDs(result))
	})

	t.Run("default dataset mode", func(t *testing.T) {
		result := listDiscoverAssets(t, map[string]string{
			"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
		})
		assert.Equal(t, []string{readyUnlinkedID}, assetIDs(result))
	})

	t.Run("scope=all", func(t *testing.T) {
		result := listDiscoverAssets(t, map[string]string{
			"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
			"scope":      "all",
		})
		assert.Equal(t, []string{readyUnlinkedID, readyLinkedID}, assetIDs(result))
	})
}

// TestDiscoverStub_ReplacesLeftoverSchema verifies setupDiscoverStub still works
// when an interrupted earlier run left a discover schema behind: the old schema
// (and its tables) is dropped and the stub's tables are created fresh.
func TestDiscoverStub_ReplacesLeftoverSchema(t *testing.T) {
	db := setupAssetsTestDB(t)

	// Simulate a leftover schema from a run that never reached its cleanup.
	_, err := db.DB.Exec(`
		DROP SCHEMA IF EXISTS discover CASCADE;
		CREATE SCHEMA discover;
		CREATE TABLE discover.leftover_junk (id INT);`)
	require.NoError(t, err)

	setupDiscoverStub(t, db)

	var junkTable *string
	require.NoError(t, db.DB.QueryRow(`SELECT to_regclass('discover.leftover_junk')::text`).Scan(&junkTable))
	assert.Nil(t, junkTable, "the leftover table should have been dropped")

	// The stub is usable: a list call against the published dataset succeeds.
	// Dataset mode needs a succeeded version to list anything.
	assetID := insertDiscoverTestAsset(t, db, "unlinked", 1, "")
	publishDiscoverVersion(t, db, 1, publishSucceededStatus)
	result := listDiscoverAssets(t, map[string]string{
		"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
	})
	assert.Equal(t, []string{assetID}, assetIDs(result))
}
