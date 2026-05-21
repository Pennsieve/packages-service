package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiscoverListAssets_RequiresExactlyOneOfPackageOrDataset verifies that the
// public /discover/assets endpoint rejects requests that pass neither or both
// of the package_id and dataset_id query parameters.
func TestDiscoverListAssets_RequiresExactlyOneOfPackageOrDataset(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]string
	}{
		{name: "neither", params: nil},
		{name: "both", params: map[string]string{"package_id": "N:package:abc", "dataset_id": "42"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := newTestRequest("GET", "/discover/assets", "discover-"+tc.name, tc.params, "")
			handler := NewDiscoverHandler(req)
			resp, err := handler.handleDiscover(context.Background())
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, resp.Body, "exactly one")
		})
	}
}

// TestDiscoverListAssets_InvalidDatasetID verifies that a non-numeric dataset_id
// is rejected with 400 before any DB lookup.
func TestDiscoverListAssets_InvalidDatasetID(t *testing.T) {
	req := newTestRequest("GET", "/discover/assets", "discover-bad-id",
		map[string]string{"dataset_id": "not-a-number"}, "")
	handler := NewDiscoverHandler(req)
	resp, err := handler.handleDiscover(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, resp.Body, "positive integer")
}
