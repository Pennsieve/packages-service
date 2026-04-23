package formatsregistry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAll_SchemaVersion(t *testing.T) {
	all := All()
	assert.Equal(t, "1.0.0", all.SchemaVersion)
	assert.NotEmpty(t, all.Formats)
}

func TestAll_UniqueIDs(t *testing.T) {
	seen := make(map[string]struct{})
	for _, f := range All().Formats {
		_, dup := seen[f.ID]
		require.Falsef(t, dup, "duplicate format id: %q", f.ID)
		seen[f.ID] = struct{}{}
	}
}

func TestAll_RequiredFields(t *testing.T) {
	for _, f := range All().Formats {
		assert.NotEmptyf(t, f.ID, "id required (name=%q)", f.Name)
		assert.NotEmptyf(t, f.Name, "name required (id=%q)", f.ID)
		assert.NotEmptyf(t, f.MediaTypes, "mediaTypes required (id=%q)", f.ID)
	}
}

func TestGet_Hit(t *testing.T) {
	f, ok := Get("pdf")
	require.True(t, ok)
	assert.Equal(t, "pdf", f.ID)
	assert.Equal(t, "PDF", f.Name)
	assert.Contains(t, f.MediaTypes, "application/pdf")
	assert.Contains(t, f.Extensions, ".pdf")
}

func TestGet_Miss(t *testing.T) {
	f, ok := Get("does-not-exist")
	assert.False(t, ok)
	assert.Equal(t, Format{}, f)
}

func TestGet_CaseSensitive(t *testing.T) {
	_, ok := Get("PDF")
	assert.False(t, ok, "lookup should be case-sensitive on id")
}
