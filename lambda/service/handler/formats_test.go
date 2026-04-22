package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pennsieve/packages-service/service/internal/formats_registry"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/organization"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func formatsClaims() *authorizer.Claims {
	return &authorizer.Claims{
		OrgClaim:  &organization.Claim{IntId: 1},
		UserClaim: &user.Claim{Id: 101, NodeId: "N:user:test-101"},
	}
}

func TestFormatsRoute_GetReturnsRegistry(t *testing.T) {
	req := newTestRequest(http.MethodGet, "/formats", "formats-req-1", nil, "")
	h := NewHandler(req, formatsClaims()).WithService(new(MockPackagesService))

	resp, err := h.handle(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var got formatsregistry.Formats
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &got))
	assert.Equal(t, formatsregistry.All().SchemaVersion, got.SchemaVersion)
	assert.Equal(t, len(formatsregistry.All().Formats), len(got.Formats))
}

func TestFormatsRoute_GetContainsKnownFormat(t *testing.T) {
	req := newTestRequest(http.MethodGet, "/formats", "formats-req-2", nil, "")
	h := NewHandler(req, formatsClaims()).WithService(new(MockPackagesService))

	resp, err := h.handle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var got formatsregistry.Formats
	require.NoError(t, json.Unmarshal([]byte(resp.Body), &got))

	var foundPDF bool
	for _, f := range got.Formats {
		if f.ID == "pdf" {
			foundPDF = true
			assert.Equal(t, "PDF", f.Name)
			assert.Contains(t, f.MediaTypes, "application/pdf")
			break
		}
	}
	assert.True(t, foundPDF, "expected pdf format in response")
}

func TestFormatsRoute_MethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			req := newTestRequest(method, "/formats", "formats-req-"+method, nil, "")
			h := NewHandler(req, formatsClaims()).WithService(new(MockPackagesService))

			resp, err := h.handle(context.Background())
			require.NoError(t, err)
			assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
		})
	}
}
