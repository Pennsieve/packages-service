package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func loadFromFile(t *testing.T, db *sql.DB, sqlFile string) {
	path := filepath.Join("testdata", sqlFile)
	sqlBytes, ioErr := os.ReadFile(path)
	if assert.NoError(t, ioErr) {
		sqlStr := string(sqlBytes)
		_, err := db.Exec(sqlStr)
		assert.NoError(t, err)
	}
}

func truncate(t *testing.T, db *sql.DB, orgID int, table string) {
	query := fmt.Sprintf("TRUNCATE TABLE \"%d\".%s CASCADE", orgID, table)
	_, err := db.Exec(query)
	assert.NoError(t, err)
}

func TestTransitionPackageState(t *testing.T) {
	config := PostgresConfigFromEnv()
	db, err := config.Open()
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	assert.NoErrorf(t, err, "could not open DB with config %s", config)
	loadFromFile(t, db, "folder-nav-test.sql")
	defer truncate(t, db, 2, "packages")

	store := NewQueries(db, 2)
	expectedDatasetId := int64(1)
	expectedNodeId := "N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8"
	expectedState := packageState.Restoring
	actual, err := store.TransitionPackageState(context.Background(), expectedDatasetId, expectedNodeId, packageState.Deleted, expectedState)
	if assert.NoError(t, err) {
		assert.Equal(t, expectedNodeId, actual.NodeId)
		assert.Equal(t, int(expectedDatasetId), actual.DatasetId)
		assert.Equal(t, expectedState, actual.PackageState)
	}
}

func TestTransitionPackageStateNoTransition(t *testing.T) {
	config := PostgresConfigFromEnv()
	db, err := config.Open()
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	assert.NoErrorf(t, err, "could not open DB with config %s", config)
	expectedOrdId := 2
	loadFromFile(t, db, "folder-nav-test.sql")
	defer truncate(t, db, expectedOrdId, "packages")

	store := NewQueries(db, 2)
	expectedDatasetId := int64(1)
	expectedNodeId := "N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8"
	// This package is marked as DELETED in the SQL file.
	expectedState := packageState.Deleted
	// But this test will try to move it from UPLOADED to RESTORING incorrectly
	incorrectCurrentState := packageState.Uploaded
	requestedFinalState := packageState.Restoring
	_, err = store.TransitionPackageState(context.Background(), expectedDatasetId, expectedNodeId, incorrectCurrentState, requestedFinalState)
	if assert.Error(t, err) {
		assert.IsType(t, models.PackageNotFoundError{}, err)
		assert.Equal(t, expectedNodeId, err.(models.PackageNotFoundError).Id.NodeId)
		assert.Equal(t, expectedDatasetId, err.(models.PackageNotFoundError).DatasetId.Id)
		assert.Equal(t, expectedOrdId, err.(models.PackageNotFoundError).OrgId)
	}
	verifyStateQuery := fmt.Sprintf(`SELECT state from "%d".packages WHERE node_id = $1`, expectedOrdId)
	var actualState packageState.State
	err = db.QueryRow(verifyStateQuery, expectedNodeId).Scan(&actualState)
	if assert.NoError(t, err) {
		assert.Equal(t, expectedState, actualState, "state modified, but should not have been")
	}
}
