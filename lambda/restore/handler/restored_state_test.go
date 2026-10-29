package handler

import (
	"testing"

	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/stretchr/testify/assert"
)

func TestFileRestoredState(t *testing.T) {
	files := func(statuses ...string) []store.File {
		var fs []store.File
		for _, s := range statuses {
			fs = append(fs, store.File{ScanStatus: s})
		}
		return fs
	}
	for name, tt := range map[string]struct {
		sourceFiles []store.File
		expected    string
	}{
		"clean":                      {files("clean"), "READY"},
		"format validated":           {files("format_validated"), "READY"},
		"unscanned (over size cap)":  {files("unscanned"), "READY"},
		"not required":               {files("not_required"), "READY"},
		"infected":                   {files("infected"), "INFECTED"},
		"pending":                    {files("pending"), "UPLOADED"},
		"scanning":                   {files("scanning"), "UPLOADED"},
		"failed":                     {files("failed"), "UPLOADED"},
		"no source file":             {nil, "UPLOADED"},
		"legacy multi-file, all ok":  {files("clean", "unscanned"), "READY"},
		"legacy multi-file, waiting": {files("clean", "pending"), "UPLOADED"},
		"legacy multi-file, one bad": {files("pending", "infected"), "INFECTED"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, FileRestoredState(tt.sourceFiles).String())
		})
	}
	// CollectionRestoredState is unchanged.
	assert.Equal(t, packageState.Ready, CollectionRestoredState)
}
