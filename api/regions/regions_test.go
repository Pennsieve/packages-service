package regions

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestForBucket(t *testing.T) {
	for name, tt := range map[string]struct {
		bucket   string
		expected string
	}{
		"us-east-1 suffix":     {bucket: "pennsieve-dev-storage-use1", expected: "us-east-1"},
		"af-south-1 suffix":    {bucket: "pennsieve-dev-storage-afs1", expected: "af-south-1"},
		"no recognized suffix": {bucket: "pennsieve-dev-storage", expected: "us-east-1"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, ForBucket(tt.bucket))
		})
	}
}
