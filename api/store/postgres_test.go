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
	expectedOrgId := 2
	ExecSQLFile(t, db, "folder-nav-test.sql")
	defer Truncate(t, db, expectedOrgId, "packages")

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
		assert.Equal(t, expectedOrgId, err.(models.PackageNotFoundError).OrgId)
	}
	verifyStateQuery := fmt.Sprintf(`SELECT state from "%d".packages WHERE node_id = $1`, expectedOrgId)
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
	expectedOrgId := 2
	ExecSQLFile(t, db, "update-desc-test.sql")
	defer Truncate(t, db, expectedOrgId, "packages")
	expectedRestoringNames := []string{"one-file-deleted-1.csv", "one-file-deleted-2", "one-dir-deleted-1", "two-file-deleted-1.csv", "two-dir-deleted-1", "three-file-deleted-1.png"}
	store := NewQueries(db, expectedOrgId, NoLogger{})
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
		verifyStateQuery := fmt.Sprintf(`SELECT state from "%d".packages WHERE node_id = $1`, expectedOrgId)

		for _, r := range restoring {
			var actualState packageState.State
			err = db.QueryRow(verifyStateQuery, r.NodeId).Scan(&actualState)
			if assert.NoError(t, err) {
				assert.Equal(t, packageState.Restoring, actualState)
			}
		}
	}
}

func TestQueries_UpdatePackageName(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrgId := 2
	ExecSQLFile(t, db, "update-package-name-test.sql")
	defer Truncate(t, db, expectedOrgId, "packages")

	checkResultQuery := fmt.Sprintf(`SELECT name from "%d".packages where id = $1`, expectedOrgId)
	store := NewQueries(db, expectedOrgId, NoLogger{})

	for name, testData := range map[string]struct {
		packageId        int64
		newName          string
		expectedRowCount int64
		expectedError    error
	}{
		"root no error":                  {int64(1), "update.txt", int64(1), nil},
		"root duplicate name":            {int64(1), "another-file.txt", int64(-1), models.PackageNameUniquenessError{}},
		"no error":                       {int64(7), "update.csv", int64(1), nil},
		"package with id does not exist": {int64(10), "update.txt", int64(0), models.PackageNotFoundError{}},
		"duplicate name":                 {int64(7), "another-one-file.csv", int64(-1), models.PackageNameUniquenessError{}},
		"no change":                      {int64(7), "one-file.csv", int64(1), nil},
	} {
		t.Run(name, func(t *testing.T) {
			err := store.UpdatePackageName(context.Background(), testData.packageId, testData.newName)
			if testData.expectedError == nil {
				if assert.NoError(t, err) {
					var actualNewName string
					err := db.QueryRow(checkResultQuery, testData.packageId).Scan(&actualNewName)
					if assert.NoError(t, err) {
						assert.Equal(t, testData.newName, actualNewName)
					}
				}
			} else {
				if assert.IsType(t, testData.expectedError, err) {
					switch err := err.(type) {
					case models.PackageNameUniquenessError:
						assert.Equal(t, testData.newName, err.Name)
						assert.Equal(t, testData.packageId, err.Id.Id)
						assert.Equal(t, expectedOrgId, err.OrgId)
						assert.NotNil(t, err.SQLError)

					case models.PackageNotFoundError:
						assert.Equal(t, expectedOrgId, err.OrgId)
						assert.Equal(t, testData.packageId, err.Id.Id)
					}
				}
			}
		})
	}
}

func TestQueries_IncrementDatasetStorage(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()

	expectedOrgId := 2
	expectedDatasetId := int64(1)
	initialSize := int64(1023)
	insertQuery := fmt.Sprintf(`INSERT INTO "%d".dataset_storage (dataset_id, size) VALUES ($1, $2)`, expectedOrgId)
	checkQuery := fmt.Sprintf(`SELECT size from "%d".dataset_storage WHERE dataset_id = $1`, expectedOrgId)

	for name, increment := range map[string]int64{
		"positive increment": int64(879),
		"negative increment": int64(-435),
	} {
		if _, err := db.Exec(insertQuery, expectedDatasetId, initialSize); err != nil {
			assert.FailNow(t, "error setting up dataset_storage table", err)
		}
		store := NewQueries(db, expectedOrgId, NoLogger{})

		t.Run(name, func(t *testing.T) {
			err := store.IncrementDatasetStorage(context.Background(), expectedDatasetId, increment)
			if assert.NoError(t, err) {
				var actual int64
				err = db.QueryRow(checkQuery, expectedDatasetId).Scan(&actual)
				if assert.NoError(t, err) {
					assert.Equal(t, initialSize+increment, actual)
				}
			}
		})

		Truncate(t, db, expectedOrgId, "dataset_storage")
	}
}

func TestQueries_GetPackageSizes(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()

	expectedOrgId := 2
	ExecSQLFile(t, db, "package-sizes-test.sql")
	defer func() {
		Truncate(t, db, expectedOrgId, "files")
		Truncate(t, db, expectedOrgId, "packages")
	}()

	store := NewQueries(db, expectedOrgId, NoLogger{})
	sizeByPackageId, err := store.GetPackageSizes(context.Background(), 1, 2, 3, 15)
	if assert.NoError(t, err) {
		assert.Len(t, sizeByPackageId, 3)
		assert.Equal(t, int64(72158), sizeByPackageId[1])
		assert.Equal(t, int64(2939946+10), sizeByPackageId[2])
		assert.Equal(t, int64(10+14+14), sizeByPackageId[3])
	}
}
