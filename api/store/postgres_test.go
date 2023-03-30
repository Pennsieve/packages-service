package store

import (
	"context"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/stretchr/testify/assert"
	"math/rand"
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

func TestQueries_IncrementOrganizationStorage(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()

	expectedOrgId := 2
	expectedInitialSize := int64(1023)
	insertQuery := `INSERT INTO pennsieve.organization_storage (organization_id, size) VALUES ($1, $2)`
	checkQuery := `SELECT size from pennsieve.organization_storage WHERE organization_id = $1`

	for name, data := range map[string]struct {
		initialSize int64 // zero means no previous organization_storage row for the org
		increment   int64
	}{
		"positive increment, existing dataset": {expectedInitialSize, int64(879)},
		"negative increment, existing dataset": {expectedInitialSize, int64(-435)},
		"positive increment, new dataset":      {0, int64(879)},
		"negative increment, new dataset":      {0, int64(-435)},
	} {
		if data.initialSize != 0 {
			if _, err := db.Exec(insertQuery, expectedOrgId, data.initialSize); err != nil {
				assert.FailNow(t, "error setting up organization_storage table", err)
			}
		}
		store := NewQueries(db, expectedOrgId, NoLogger{})

		t.Run(name, func(t *testing.T) {
			err := store.IncrementOrganizationStorage(context.Background(), int64(expectedOrgId), data.increment)
			if assert.NoError(t, err) {
				var actual int64
				err = db.QueryRow(checkQuery, expectedOrgId).Scan(&actual)
				if assert.NoError(t, err) {
					assert.Equal(t, data.initialSize+data.increment, actual)
				}
			}
		})

		TruncatePennsieve(t, db, "organization_storage")
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
	expectedInitialSize := int64(1023)
	insertQuery := fmt.Sprintf(`INSERT INTO "%d".dataset_storage (dataset_id, size) VALUES ($1, $2)`, expectedOrgId)
	checkQuery := fmt.Sprintf(`SELECT size from "%d".dataset_storage WHERE dataset_id = $1`, expectedOrgId)

	for name, data := range map[string]struct {
		initialSize int64 // zero means no previous dataset_storage row for the dataset
		increment   int64
	}{
		"positive increment, existing dataset": {expectedInitialSize, int64(879)},
		"negative increment, existing dataset": {expectedInitialSize, int64(-435)},
		"positive increment, new dataset":      {0, int64(879)},
		"negative increment, new dataset":      {0, int64(-435)},
	} {
		if data.initialSize != 0 {
			if _, err := db.Exec(insertQuery, expectedDatasetId, data.initialSize); err != nil {
				assert.FailNow(t, "error setting up dataset_storage table", err)
			}
		}
		store := NewQueries(db, expectedOrgId, NoLogger{})

		t.Run(name, func(t *testing.T) {
			err := store.IncrementDatasetStorage(context.Background(), expectedDatasetId, data.increment)
			if assert.NoError(t, err) {
				var actual int64
				err = db.QueryRow(checkQuery, expectedDatasetId).Scan(&actual)
				if assert.NoError(t, err) {
					assert.Equal(t, data.initialSize+data.increment, actual)
				}
			}
		})

		Truncate(t, db, expectedOrgId, "dataset_storage")
	}
}

func TestQueries_IncrementPackageStorage(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrgId := 2

	ExecSQLFile(t, db, "increment-package-storage-test.sql")
	defer Truncate(t, db, expectedOrgId, "packages")

	expectedPackageId := int64(1)
	expectedInitialSize := int64(1023)

	insertQuery := fmt.Sprintf(`INSERT INTO "%d".package_storage (package_id, size) VALUES ($1, $2)`, expectedOrgId)
	checkQuery := fmt.Sprintf(`SELECT size from "%d".package_storage WHERE package_id = $1`, expectedOrgId)

	for name, data := range map[string]struct {
		initialSize int64 // zero means no previous package_storage row for the package
		increment   int64
	}{
		"positive increment, existing package": {expectedInitialSize, int64(879)},
		"negative increment, existing package": {expectedInitialSize, int64(-435)},
		"positive increment, new package":      {0, int64(879)},
		"negative increment, new package":      {0, int64(-435)},
	} {
		if data.initialSize != 0 {
			if _, err := db.Exec(insertQuery, expectedPackageId, data.initialSize); err != nil {
				assert.FailNow(t, "error setting up package_storage table", err)
			}
		}
		store := NewQueries(db, expectedOrgId, NoLogger{})

		t.Run(name, func(t *testing.T) {
			err := store.IncrementPackageStorage(context.Background(), expectedPackageId, data.increment)
			if assert.NoError(t, err) {
				var actual int64
				err = db.QueryRow(checkQuery, expectedPackageId).Scan(&actual)
				if assert.NoError(t, err) {
					assert.Equal(t, data.initialSize+data.increment, actual)
				}
			}
		})

		Truncate(t, db, expectedOrgId, "package_storage")
	}
}

func TestQueries_IncrementPackageStorageAncestors(t *testing.T) {
	db := OpenDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrgId := 2
	ExecSQLFile(t, db, "folder-nav-test.sql")
	defer Truncate(t, db, expectedOrgId, "packages")
	defer Truncate(t, db, expectedOrgId, "package_storage")

	// These are the ancestors of package with id == 43, starting with its parent.
	expectedAncestorIds := []int64{35, 24, 12, 6}
	insertQuery := fmt.Sprintf(`INSERT INTO "%d".package_storage (package_id, size) VALUES ($1, $2)`, expectedOrgId)
	ancestorIdToInitialSize := map[int64]int64{}
	for _, id := range expectedAncestorIds {
		initialSize := rand.Int63()
		ancestorIdToInitialSize[id] = initialSize
		if _, err := db.Exec(insertQuery, id, initialSize); err != nil {
			assert.FailNow(t, "error setting up package_storage table", err)
		}
	}

	store := NewQueries(db, expectedOrgId, NoLogger{})
	increment := int64(92)
	err := store.IncrementPackageStorageAncestors(context.Background(), expectedAncestorIds[0], increment)
	if assert.NoError(t, err) {
		checkQuery := fmt.Sprintf(`SELECT package_id, size from "%d".package_storage`, expectedOrgId)
		var rowCount int
		rows, err := db.Query(checkQuery)
		if assert.NoError(t, err) {
			defer rows.Close()
			for rows.Next() {
				rowCount++
				var ancestorId, actualSize int64
				err = rows.Scan(&ancestorId, &actualSize)
				if assert.NoError(t, err) {
					expectedInitial := ancestorIdToInitialSize[ancestorId]
					assert.Equal(t, expectedInitial+increment, actualSize)
				}
			}
			assert.Equal(t, len(expectedAncestorIds), rowCount)
			assert.NoError(t, rows.Err())
		}
	}
}
