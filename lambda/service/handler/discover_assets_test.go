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
	var exists bool
	require.NoError(t, db.DB.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'discover')`).Scan(&exists))
	require.False(t, exists, "test database already has a discover schema; the stub would clash with it")

	_, err := db.DB.Exec(`
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

// insertDiscoverTestAsset inserts a viewer asset in org 2 / dataset 1, created
// minutesAgo minutes ago, linked to the given package integer IDs. chatSessionID
// may be empty. Returns the asset ID.
func insertDiscoverTestAsset(t *testing.T, db *store.TestDB, name string, minutesAgo int, chatSessionID string, packageIntIDs ...int) string {
	t.Helper()
	var chat interface{}
	if chatSessionID != "" {
		chat = chatSessionID
	}
	var id string
	require.NoError(t, db.DB.QueryRow(`
		INSERT INTO "2".viewer_assets (dataset_id, name, asset_type, properties, s3_bucket, created_by, chat_session_id, created_at)
		VALUES (1, $1, 'thumb', '{}', 'test-storage-bucket', 101, $2, NOW() - make_interval(mins => $3)) RETURNING id`,
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

// TestDiscoverListAssets_ScopeAll_NoPublishedVersion verifies that with no
// successfully published version, scope=all returns only unlinked assets.
func TestDiscoverListAssets_ScopeAll_NoPublishedVersion(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	ids := discoverAssetFixtures(t, db)

	result := listDiscoverAssets(t, map[string]string{
		"dataset_id": fmt.Sprint(discoverTestPublishedDatasetID),
		"scope":      "all",
	})
	assert.Equal(t, []string{ids["unlinked"]}, assetIDs(result))
}

// TestDiscoverListAssets_DatasetDefault_UnlinkedOnly verifies dataset mode without
// scope still returns only assets with no package links, and now excludes
// chat-scoped assets.
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

// TestDiscoverListAssets_PackageMode_ReturnsLinkedPackageIDs verifies package mode
// returns the package's assets with every linked package node ID, now read in the
// same query as the assets.
func TestDiscoverListAssets_PackageMode_ReturnsLinkedPackageIDs(t *testing.T) {
	db := setupAssetsTestDB(t)
	setupDiscoverStub(t, db)
	ids := discoverAssetFixtures(t, db)
	publishDiscoverVersion(t, db, 1, publishSucceededStatus, "N:package:test-pkg-a")

	result := listDiscoverAssets(t, map[string]string{"package_id": "N:package:test-pkg-a"})

	require.Equal(t, []string{ids["thumb-a"], ids["multi-ac"]}, assetIDs(result))
	assert.Equal(t, []string{"N:package:test-pkg-a"}, result.Assets[0].PackageIDs)
	assert.Equal(t, []string{"N:package:test-pkg-a", "N:package:test-pkg-c"}, result.Assets[1].PackageIDs)
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
