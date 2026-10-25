package formatsregistry

import (
	"regexp"
	"strings"
	"testing"

	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/stretchr/testify/assert"
)

// File types the registry lists that pennsieve-go-core's fileType enum
// doesn't define.
var fileTypesNotInGoCore = map[string]bool{
	// Legacy: present on older packages (Scala FileType), never assigned on
	// upload. No extension maps to them in pennsieve-api either; old importers
	// set them. The registry keeps describing them.
	"BCI2000":             true,
	"BNI1":                true,
	"Blackrock":           true,
	"DirectoryViewerData": true,
	"FSFast":              true,
	"StaticViewerData":    true,
	"WFDB":                true,
	// Added to go-core in pennsieve-go-core#63 (.mat/.mex were classified as
	// MATLAB, the script type); remove from this allowlist once
	// packages-service depends on that version.
	"MAT": true,
	// Added in pennsieve-go-core#64; remove once packages-service depends on
	// that version.
	"OMEZarr": true,
	"TSZarr":  true,
}

// Every go-core file type has a registry entry, so a package of that type
// always has a format.
func TestFileTypes_EveryGoCoreTypeIsListed(t *testing.T) {
	listed := map[string]bool{}
	for _, f := range All().Formats {
		for _, ft := range f.FileTypes {
			listed[ft] = true
		}
	}
	for name := range fileType.Dict {
		assert.Truef(t, listed[name], "go-core file type %q has no format in formats.json", name)
	}
}

// Every fileTypes value in the registry is a real go-core file type, apart
// from the commented exceptions above.
func TestFileTypes_RegistryTypesExistInGoCore(t *testing.T) {
	for _, f := range All().Formats {
		for _, ft := range f.FileTypes {
			if fileTypesNotInGoCore[ft] {
				continue
			}
			_, ok := fileType.Dict[ft]
			assert.Truef(t, ok, "format %q lists file type %q, which go-core doesn't define", f.ID, ft)
		}
	}
}

// De facto x- types with no IANA registration, kept as they are: changing a
// media type changes what stored packages say they are.
var unregisteredMediaTypes = map[string]bool{
	"application/x-bam":         true,
	"application/x-cram":        true,
	"application/x-fasta":       true,
	"application/x-fastq":       true,
	"application/x-hdf":         true,
	"application/x-hdf5":        true,
	"application/x-ipynb+json":  true,
	"application/x-matlab-data": true,
	"application/x-nifti":       true,
	"application/x-nifti-gz":    true,
	"application/x-sh":          true,
	"application/x-vcf":         true,
	"text/x-c++src":             true,
	"text/x-csharp":             true,
	"text/x-dockerfile":         true,
	"text/x-java-source":        true,
	"text/x-matlab":             true,
	"text/x-python":             true,
	"text/x-r":                  true,
	"video/x-msvideo":           true,
}

// Media types shared by more than one format. Each format should have its
// own; NWB (application/x-hdf5 with HDF5) should move to application/x-nwb.
var sharedMediaTypes = map[string]bool{
	"application/x-hdf5":       true, // hdf5, nwb
	"image/tiff":               true, // tiff, ome-tiff
	"application/octet-stream": true, // generic-data, data
	"application/postscript":   true, // adobe-illustrator, eps
}

// The convention (README.md): an IANA type where one exists, otherwise
// application/vnd.pennsieve.<id>; one format per media type.
func TestMediaTypes_FollowTheConvention(t *testing.T) {
	shape := regexp.MustCompile(`^[a-z]+/[a-z0-9][a-z0-9.+-]*$`)
	owner := map[string]string{}
	for _, f := range All().Formats {
		for _, m := range f.MediaTypes {
			assert.Regexpf(t, shape, m, "format %q media type", f.ID)
			sub := m[strings.Index(m, "/")+1:]
			if strings.HasPrefix(sub, "x-") {
				assert.Truef(t, unregisteredMediaTypes[m], "format %q uses unregistered %q: use application/vnd.pennsieve.%s", f.ID, m, f.ID)
			}
			if strings.HasPrefix(m, "application/vnd.pennsieve.") {
				assert.Equalf(t, "application/vnd.pennsieve."+f.ID, m, "a Pennsieve media type is named after its format id")
			}
			if prev, ok := owner[m]; ok && !sharedMediaTypes[m] {
				t.Errorf("formats %q and %q share media type %q", prev, f.ID, m)
			}
			owner[m] = f.ID
		}
	}
}
