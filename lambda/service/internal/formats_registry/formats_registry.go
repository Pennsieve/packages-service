package formatsregistry

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed formats.json
var formatsData []byte

type Format struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	MediaTypes  []string `json:"mediaTypes"`
	FileTypes   []string `json:"fileTypes,omitempty"`
	Extensions  []string `json:"extensions,omitempty"`
	EDAM        string   `json:"edam,omitempty"`
}

type Formats struct {
	SchemaVersion string   `json:"schemaVersion"`
	Formats       []Format `json:"formats"`
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
