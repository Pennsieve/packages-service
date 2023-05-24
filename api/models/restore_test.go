package models

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnmarshallRestoreRequest(t *testing.T) {
	packageId := "N:package:1234"
	body := fmt.Sprintf(`{"nodeIds": [%q]}`, packageId)
	var request RestoreRequest
	err := json.Unmarshal([]byte(body), &request)
	if assert.NoError(t, err) {
		assert.Len(t, request.NodeIds, 1)
		assert.Equal(t, packageId, request.NodeIds[0])
		assert.Empty(t, request.UserId)
	}
}
