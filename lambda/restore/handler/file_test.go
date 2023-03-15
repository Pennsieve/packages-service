package handler

import (
	"fmt"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetOriginalName(t *testing.T) {
	expected := "file.txt"
	nodeId := "N:package:12345"
	for name, testData := range map[string]struct {
		prefix      string
		expectError bool
	}{
		"no prefix":   {prefix: "", expectError: true},
		"bad prefix":  {prefix: "NotWhatIsExpected_", expectError: true},
		"good prefix": {prefix: fmt.Sprintf("__%s__%s_", packageState.Deleted, nodeId), expectError: false},
	} {
		deletedName := fmt.Sprintf("%s%s", testData.prefix, expected)
		t.Run(name, func(t *testing.T) {
			actual, err := getOriginalName(deletedName, nodeId)
			if testData.expectError {
				assert.Error(t, err)
			} else {
				if assert.NoError(t, err) {
					assert.Equal(t, expected, actual)
				}
			}
		})
	}
}
