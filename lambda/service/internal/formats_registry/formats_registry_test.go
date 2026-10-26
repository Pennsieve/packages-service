package formatsregistry

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAll_SchemaVersion(t *testing.T) {
	all := All()
	assert.Equal(t, "1.1.0", all.SchemaVersion)
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

// An EDAM id is the short form, format_ and four digits, and only formats
// EDAM actually defines carry one (EDF, MEF and NWB don't).
func TestAll_EDAMIds(t *testing.T) {
	re := regexp.MustCompile(`^format_\d{4}$`)
	n := 0
	for _, f := range All().Formats {
		if f.EDAM == "" {
			continue
		}
		n++
		assert.Regexpf(t, re, f.EDAM, "edam for %q", f.ID)
	}
	assert.NotZero(t, n, "some formats carry an EDAM id")
	for _, id := range []string{"edf", "mef", "nwb"} {
		f, ok := Get(id)
		require.True(t, ok)
		assert.Emptyf(t, f.EDAM, "%s has no EDAM format", id)
	}
	hdf5, _ := Get("hdf5")
	assert.Equal(t, "format_3590", hdf5.EDAM)
}

// A format's kind is empty (a single file) or directory (a folder, stored as
// one zip per package). The folder formats are OME-Zarr and TS-Zarr.
func TestAll_Kinds(t *testing.T) {
	var dirs []string
	for _, f := range All().Formats {
		assert.Containsf(t, []string{"", "directory"}, f.Kind, "kind for %q", f.ID)
		if f.Kind == "directory" {
			dirs = append(dirs, f.ID)
		}
	}
	for _, id := range []string{"mef3", "nwb-zarr", "ome-zarr", "ts-zarr", "zarr", "open-ephys-binary", "natus-neuroworks", "illumina-run"} {
		assert.Containsf(t, dirs, id, "%s is a folder format", id)
	}
}

// Folder extensions belong to folder formats, start with a dot, and differ
// from every stored extension, so a folder and its compressed file are never
// confused.
func TestAll_FolderExtensions(t *testing.T) {
	stored := map[string]bool{}
	for _, f := range All().Formats {
		for _, e := range f.Extensions {
			stored[e] = true
		}
	}
	seen := map[string]string{}
	for _, f := range All().Formats {
		if f.Kind != "directory" {
			assert.Emptyf(t, f.FolderExtensions, "%q isn't a folder format", f.ID)
			continue
		}
		for _, e := range f.FolderExtensions {
			assert.Regexpf(t, `^\.[a-z0-9.]+$`, e, "folder extension of %q", f.ID)
			assert.Falsef(t, stored[e], "%q is also a stored extension", e)
			if prev, dup := seen[e]; dup {
				t.Errorf("formats %q and %q share folder extension %q", prev, f.ID, e)
			}
			seen[e] = f.ID
		}
	}
	mef3, _ := Get("mef3")
	assert.Equal(t, []string{".mefd"}, mef3.FolderExtensions)
}

// Structures are what app.yml's conformsTo names (bids, bids@1.9): ids follow
// its pattern, are unique and don't clash with a format id, and each links to
// its specification.
func TestStructures(t *testing.T) {
	re := regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	seen := map[string]bool{}
	for _, s := range All().Structures {
		assert.Regexpf(t, re, s.ID, "structure id")
		assert.Falsef(t, seen[s.ID], "duplicate structure id %q", s.ID)
		seen[s.ID] = true
		_, clash := Get(s.ID)
		assert.Falsef(t, clash, "structure %q has the id of a format", s.ID)
		assert.NotEmptyf(t, s.Name, "name for %q", s.ID)
		assert.Regexpf(t, `^https://`, s.Spec, "spec for %q", s.ID)
		assert.Regexpf(t, `^https://`, s.URI, "uri for %q", s.ID)
		// A content root is one relative folder, not a path out of the tree.
		if s.ContentRoot != "" {
			assert.Regexpf(t, `^[a-z0-9_-]+$`, s.ContentRoot, "contentRoot for %q", s.ID)
		}
		for _, v := range s.Versions {
			assert.Regexpf(t, `^[0-9A-Za-z.+-]+$`, v, "version of %q", s.ID)
		}
	}
	for _, id := range []string{"bids", "sparc-sds", "pennsieve-published-dataset", "ro-crate"} {
		assert.Truef(t, seen[id], "%s is listed", id)
	}
	ppd, _ := getStructure("pennsieve-published-dataset")
	assert.Equal(t, "files", ppd.ContentRoot)
}

func getStructure(id string) (Structure, bool) {
	for _, s := range All().Structures {
		if s.ID == id {
			return s, true
		}
	}
	return Structure{}, false
}

// A structure Pennsieve defines (its uri on schemas.pennsieve.io) has its
// specification in structures/<id>/v<version>.md, which terraform publishes
// at its spec link. Every listed version has a file, the spec link names one
// of them, and every file belongs to a structure and a listed version.
func TestStructures_PennsieveSpecsArePublished(t *testing.T) {
	const host = "https://schemas.pennsieve.io/structures/"
	files, err := filepath.Glob("structures/*/v*.md")
	require.NoError(t, err)
	onDisk := map[string]bool{}
	for _, f := range files {
		onDisk[filepath.ToSlash(f)] = true
	}
	owned := map[string]bool{}
	for _, s := range All().Structures {
		if !strings.HasPrefix(s.URI, host) {
			continue
		}
		owned[s.ID] = true
		assert.Equalf(t, host+s.ID, s.URI, "uri of %q is unversioned", s.ID)
		require.NotEmptyf(t, s.Versions, "%q lists its versions", s.ID)
		for _, v := range s.Versions {
			_, err := os.Stat("structures/" + s.ID + "/v" + v + ".md")
			if v == s.Versions[len(s.Versions)-1] {
				assert.NoErrorf(t, err, "%q: the current version %s has no specification file", s.ID, v)
			}
		}
		version := strings.TrimPrefix(s.Spec, host+s.ID+"/v")
		assert.Truef(t, onDisk["structures/"+s.ID+"/v"+version+".md"], "%q: spec %s has no file", s.ID, s.Spec)
		assert.Containsf(t, s.Versions, version, "%q: spec names a version it doesn't list", s.ID)
	}
	for f := range onDisk {
		parts := strings.Split(f, "/")
		id, version := parts[1], strings.TrimSuffix(strings.TrimPrefix(parts[2], "v"), ".md")
		require.Truef(t, owned[id], "%s: no Pennsieve structure %q", f, id)
		s, _ := getStructure(id)
		assert.Containsf(t, s.Versions, version, "%s: version %s isn't listed", f, version)
	}
	assert.True(t, owned["pennsieve-published-dataset"])
}
