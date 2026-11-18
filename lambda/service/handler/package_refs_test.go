package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPackageRefsByAsset_ZeroOneAndTwoLinks checks that one call returns the
// right refs for assets with 1 and 2 linked packages, and leaves out the asset
// with no links.
func TestPackageRefsByAsset_ZeroOneAndTwoLinks(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	noLinks := insertDiscoverTestAsset(t, db, "no-links", 0, "")
	oneLink := insertDiscoverTestAsset(t, db, "one-link", 0, "", 5002)
	twoLinks := insertDiscoverTestAsset(t, db, "two-links", 0, "", 5001, 5003)

	refs, err := packageRefsByAsset(context.Background(), assetsTestOrgID, []string{noLinks, oneLink, twoLinks})
	require.NoError(t, err)

	_, found := refs[noLinks]
	assert.False(t, found, "an asset with no links should not be in the map")

	assert.Equal(t, []packageRef{
		{NodeID: "N:package:test-pkg-b", Name: "Test Package 1"},
	}, refs[oneLink])

	assert.Equal(t, []packageRef{
		{NodeID: "N:package:test-pkg-a", Name: "Test Package 0"},
		{NodeID: "N:package:test-pkg-c", Name: "Test Package 2"},
	}, refs[twoLinks])

	assert.Len(t, refs, 2, "only the two linked assets should be in the map")
}

// TestPackageRefsByAsset_ReturnsNodeIDAndName checks that each ref has both the
// package node ID and the package name filled in.
func TestPackageRefsByAsset_ReturnsNodeIDAndName(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	assetID := insertDiscoverTestAsset(t, db, "linked-to-c", 0, "", 5003)

	refs, err := packageRefsByAsset(context.Background(), assetsTestOrgID, []string{assetID})
	require.NoError(t, err)

	require.Len(t, refs[assetID], 1)
	assert.Equal(t, "N:package:test-pkg-c", refs[assetID][0].NodeID)
	assert.Equal(t, "Test Package 2", refs[assetID][0].Name)
}

// TestPackageRefsByAsset_OrderedByNodeID checks that refs come back sorted by
// package node ID, not by the order the links were inserted.
func TestPackageRefsByAsset_OrderedByNodeID(t *testing.T) {
	db := setupAssetsTestDB(t)
	createTestPackages(t, db, 2, 1)

	// Link c (5003) first, then a (5001).
	assetID := insertDiscoverTestAsset(t, db, "linked-c-then-a", 0, "", 5003, 5001)

	refs, err := packageRefsByAsset(context.Background(), assetsTestOrgID, []string{assetID})
	require.NoError(t, err)

	// a comes before c.
	assert.Equal(t, []string{"N:package:test-pkg-a", "N:package:test-pkg-c"}, packageNodeIDs(refs[assetID]))
}

// TestPackageRefsByAsset_EmptyInput checks that no asset IDs gives an empty,
// non-nil map without touching the database, and that packageNodeIDs(nil) gives
// an empty, non-nil slice (so JSON shows [] rather than null).
func TestPackageRefsByAsset_EmptyInput(t *testing.T) {
	// With PennsieveDB set to nil, any query would panic. So if this test
	// passes, no query ran.
	originalDB := PennsieveDB
	PennsieveDB = nil
	t.Cleanup(func() { PennsieveDB = originalDB })

	refs, err := packageRefsByAsset(context.Background(), assetsTestOrgID, []string{})
	require.NoError(t, err)
	assert.NotNil(t, refs)
	assert.Empty(t, refs)

	refs, err = packageRefsByAsset(context.Background(), assetsTestOrgID, nil)
	require.NoError(t, err)
	assert.NotNil(t, refs)
	assert.Empty(t, refs)

	ids := packageNodeIDs(nil)
	assert.NotNil(t, ids)
	assert.Empty(t, ids)
}
