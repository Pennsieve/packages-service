package handler

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withViewerAssetsBucket(t *testing.T, bucket string) {
	t.Helper()
	previous := ViewerAssetsBucket
	ViewerAssetsBucket = bucket
	t.Cleanup(func() { ViewerAssetsBucket = previous })
}

func TestCloudFrontPathPrefixForBucket(t *testing.T) {
	withViewerAssetsBucket(t, "pennsieve-prod-storage-use1")

	assert.Equal(t, "", cloudFrontPathPrefixForBucket("pennsieve-prod-storage-use1"),
		"the default viewer assets bucket is served without a prefix")
	assert.Equal(t, "", cloudFrontPathPrefixForBucket("PENNSIEVE-PROD-STORAGE-USE1"),
		"bucket names compare case-insensitively")
	assert.Equal(t, "", cloudFrontPathPrefixForBucket(""),
		"an asset without a bucket falls back to the default origin")
	assert.Equal(t, "/b11f440c", cloudFrontPathPrefixForBucket("prd-sparc-storage-use1"),
		"other buckets use the hash prefix that routes to their origin")
}

func TestViewerAssetURL_UsesAssetBucket(t *testing.T) {
	withViewerAssetsBucket(t, "pennsieve-prod-storage-use1")
	setupCloudFrontConfig()
	defer resetCloudFrontConfig()

	assert.Equal(t,
		"https://test.cloudfront.net/b11f440c/O367/D3103/asset-1/",
		viewerAssetURL(367, 3103, "asset-1", "prd-sparc-storage-use1"))
	assert.Equal(t,
		"https://test.cloudfront.net/O367/D3103/asset-2/",
		viewerAssetURL(367, 3103, "asset-2", "pennsieve-prod-storage-use1"))
}

func TestViewerAssetURL_NoCloudFront(t *testing.T) {
	resetCloudFrontConfig()
	assert.Equal(t, "", viewerAssetURL(367, 3103, "asset-1", "prd-sparc-storage-use1"))
}

func TestViewerAssetsResourcePattern(t *testing.T) {
	setupCloudFrontConfig()
	defer resetCloudFrontConfig()

	tests := []struct {
		name     string
		prefixes []string
		want     string
	}{
		{"single external bucket", []string{"/b11f440c", "/b11f440c"}, "https://test.cloudfront.net/b11f440c/O367/D3103/*"},
		{"single default bucket", []string{""}, "https://test.cloudfront.net/O367/D3103/*"},
		{"mixed buckets", []string{"", "/b11f440c"}, "https://test.cloudfront.net/*O367/D3103/*"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, viewerAssetsResourcePattern(367, 3103, tt.prefixes))
		})
	}
}

func TestSignViewerAssetsPolicy(t *testing.T) {
	setupCloudFrontConfig()
	defer resetCloudFrontConfig()

	components, err := signViewerAssetsPolicy(367, 3103, []string{"", "/b11f440c"})
	require.NoError(t, err)

	assert.Equal(t, "https://test.cloudfront.net/O367/D3103/", components.BaseURL,
		"base URL uses the first (newest) asset's prefix")
	assert.Equal(t, "test-key-id", components.KeyPairID)
	assert.NotEmpty(t, components.Signature)
	require.NotNil(t, components.PolicyInfo)
	assert.Equal(t, "https://test.cloudfront.net/*O367/D3103/*", components.PolicyInfo.ResourcePattern)

	// The policy must allow both the prefixed and the unprefixed asset paths.
	pattern := components.PolicyInfo.ResourcePattern
	assert.True(t, wildcardMatch(pattern, "https://test.cloudfront.net/b11f440c/O367/D3103/asset-1/zarr.json"))
	assert.True(t, wildcardMatch(pattern, "https://test.cloudfront.net/O367/D3103/asset-2/zarr.json"))
	assert.False(t, wildcardMatch(pattern, "https://test.cloudfront.net/O367/D31030/asset-3/zarr.json"))
	assert.False(t, wildcardMatch(pattern, "https://test.cloudfront.net/O1367/D3103/asset-4/zarr.json"))
}

func TestSignViewerAssetsPolicy_SingleBucketKeepsExactScope(t *testing.T) {
	setupCloudFrontConfig()
	defer resetCloudFrontConfig()

	components, err := signViewerAssetsPolicy(367, 3103, []string{"/b11f440c"})
	require.NoError(t, err)
	assert.Equal(t, "https://test.cloudfront.net/b11f440c/O367/D3103/", components.BaseURL)
	require.NotNil(t, components.PolicyInfo)
	assert.Equal(t, "https://test.cloudfront.net/b11f440c/O367/D3103/*", components.PolicyInfo.ResourcePattern)
}

func TestSignViewerAssetsPolicy_Errors(t *testing.T) {
	setupCloudFrontConfig()
	defer resetCloudFrontConfig()

	_, err := signViewerAssetsPolicy(367, 3103, nil)
	assert.Error(t, err)

	cloudfrontPrivateKey = nil
	_, err = signViewerAssetsPolicy(367, 3103, []string{""})
	assert.Error(t, err)
}

// wildcardMatch reports whether s matches a CloudFront policy resource pattern,
// where '*' matches zero or more characters.
func wildcardMatch(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	for i, part := range parts[1:] {
		if i == len(parts)-2 {
			return strings.HasSuffix(s, part)
		}
		idx := strings.Index(s, part)
		if idx < 0 {
			return false
		}
		s = s[idx+len(part):]
	}
	return true
}
