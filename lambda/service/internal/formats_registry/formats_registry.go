package formatsregistry

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed formats.json
var formatsData []byte

// Kind is "directory" for a format that is a folder (OME-Zarr), stored as one
// zip per package; empty means a single file.
type Format struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	MediaTypes  []string `json:"mediaTypes"`
	FileTypes   []string `json:"fileTypes,omitempty"`
	Extensions  []string `json:"extensions,omitempty"`
	EDAM        string   `json:"edam,omitempty"`
}

// Structure is a folder layout a whole input or output can follow (BIDS), named
// by app.yml's conformsTo. It isn't a file format: it has no media type.
type Structure struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Spec        string `json:"spec"`
	Validator   string `json:"validator,omitempty"`
}

type Formats struct {
	SchemaVersion string      `json:"schemaVersion"`
	Formats       []Format    `json:"formats"`
	Structures    []Structure `json:"structures"`
}

var loaded Formats

func init() {
	if err := json.Unmarshal(formatsData, &loaded); err != nil {
		panic(fmt.Sprintf("formatsregistry: invalid formats.json: %v", err))
	}
}

func All() Formats {
	return loaded
}

func Get(id string) (Format, bool) {
	for _, f := range loaded.Formats {
		if f.ID == id {
			return f, true
		}
	}
	return Format{}, false
}
