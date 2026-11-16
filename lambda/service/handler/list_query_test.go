package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A listQuery with no mode is a caller bug. It should come back as an error,
// not quietly run as some other mode.
func TestListQueryBuild_NoModeIsAnError(t *testing.T) {
	_, _, err := listQuery{orgID: 2, datasetID: 1}.build()
	assert.ErrorContains(t, err, "unknown mode")
}
