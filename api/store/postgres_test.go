package store

import (
	"context"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransitionPackageState(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	ExecSQLFile(t, db, "folder-nav-test.sql")
	defer Truncate(t, db, 2, "packages")

	store := NewQueries(db, 2, NoLogger{})
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
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrdId := 2
	ExecSQLFile(t, db, "folder-nav-test.sql")
	defer Truncate(t, db, expectedOrdId, "packages")

	store := NewQueries(db, 2, NoLogger{})
	expectedDatasetId := int64(1)
	expectedNodeId := "N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8"
	// This package is marked as DELETED in the SQL file.
	expectedState := packageState.Deleted
	// But this test will try to move it from UPLOADED to RESTORING incorrectly
	incorrectCurrentState := packageState.Uploaded
	requestedFinalState := packageState.Restoring
	_, err := store.TransitionPackageState(context.Background(), expectedDatasetId, expectedNodeId, incorrectCurrentState, requestedFinalState)
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

func TestQueries_TransitionDescendantPackageState(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrdId := 2
	ExecSQLFile(t, db, "update-desc-test.sql")
	defer Truncate(t, db, expectedOrdId, "packages")
	expectedRestoringNames := []string{"one-file-deleted-1.csv", "one-file-deleted-2", "one-dir-deleted-1", "two-file-deleted-1.csv", "two-dir-deleted-1", "three-file-deleted-1.png"}
	store := NewQueries(db, expectedOrdId, NoLogger{})
	restoring, err := store.TransitionDescendantPackageState(context.Background(), 1, 4, packageState.Deleted, packageState.Restoring)
	if assert.NoError(t, err) {
		assert.Len(t, restoring, len(expectedRestoringNames))
		for _, expectedName := range expectedRestoringNames {
			assert.Conditionf(t, func() (success bool) {
				for _, actual := range restoring {
					if actual.Name == expectedName {
						success = true
						break
					}
				}
				return
			}, "expected package name %s missing from %v", expectedName, restoring)
		}
		assert.Condition(t, func() bool {
			for _, actual := range restoring {
				if !assert.Equal(t, packageState.Restoring, actual.PackageState) {
					return false
				}
			}
			return true
		})
	}
}

func TestQueries_UpdatePackageName(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrdId := 2
	ExecSQLFile(t, db, "update-package-name-test.sql")
	defer Truncate(t, db, expectedOrdId, "packages")

	checkResultQuery := fmt.Sprintf(`SELECT name from "%d".packages where id = $1`, expectedOrdId)
	store := NewQueries(db, expectedOrdId, NoLogger{})

	for name, testData := range map[string]struct {
		packageId        int64
		newName          string
		expectedRowCount int64
		expectError      bool
	}{
		"root no error":                  {int64(1), "update.txt", int64(1), false},
		"root duplicate name":            {int64(1), "another-file.txt", int64(-1), true},
		"no error":                       {int64(7), "update.csv", int64(1), false},
		"package with id does not exist": {int64(10), "update.txt", int64(0), false},
		"duplicate name":                 {int64(7), "another-one-file.csv", int64(-1), true},
		"same name":                      {int64(7), "one-file.csv", int64(1), false},
	} {
		t.Run(name, func(t *testing.T) {
			actualCount, err := store.UpdatePackageName(context.Background(), testData.packageId, testData.newName)
			if testData.expectError {
				assert.Error(t, err)
				err, ok := err.(models.PackageNameUniquenessError)
				if assert.True(t, ok) {
					assert.Equal(t, err.Name, testData.newName)
					assert.Equal(t, err.Id.Id, testData.packageId)
					assert.Equal(t, err.OrgId, expectedOrdId)
					assert.NotNil(t, err.SQLError)
				}
			} else {
				if assert.NoError(t, err) {
					assert.Equal(t, testData.expectedRowCount, actualCount)
					if actualCount > 0 {
						var actualNewName string
						err := db.QueryRow(checkResultQuery, testData.packageId).Scan(&actualNewName)
						if assert.NoError(t, err) {
							assert.Equal(t, testData.newName, actualNewName)
						}
					}
				}
			}
		})
	}
}
