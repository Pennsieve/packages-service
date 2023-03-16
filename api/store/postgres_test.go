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
	"time"
)

// pingUntilReady pings the db up to 10 times, stopping when
// a ping is successful. Used because there have been problems on Jenkins with
// the test DB not being fully started and ready to make connections.
// But there must be a better way.
func pingUntilReady(db *sql.DB) error {
	var err error
	wait := 100 * time.Millisecond
	for i := 0; i < 10; i++ {
		if err = db.Ping(); err == nil {
			return nil
		}
		time.Sleep(wait)
		wait = 2 * wait

	}
	return err
}

func openDB(t *testing.T) *sql.DB {
	config := PostgresConfigFromEnv()
	db, err := config.Open()
	if err != nil {
		assert.FailNowf(t, "cannot open database", "config: %s, err: %v", config, err)
	}
	if err = pingUntilReady(db); err != nil {
		assert.FailNow(t, "cannot ping database", err)
	}
	return db
}

func loadFromFile(t *testing.T, db *sql.DB, sqlFile string) {
	path := filepath.Join("testdata", sqlFile)
	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		assert.FailNowf(t, "unable to read SQL file", "%s: %v", path, err)
	}
	sqlStr := string(sqlBytes)
	_, err = db.Exec(sqlStr)
	if err != nil {
		assert.FailNowf(t, "error executing SQL file", "%s: %v", path, err)
	}
}

func truncate(t *testing.T, db *sql.DB, orgID int, table string) {
	query := fmt.Sprintf("TRUNCATE TABLE \"%d\".%s CASCADE", orgID, table)
	_, err := db.Exec(query)
	assert.NoError(t, err)
}

func TestTransitionPackageState(t *testing.T) {
	db := openDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
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
	db := openDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
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
	db := openDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrdId := 2
	loadFromFile(t, db, "update-desc-test.sql")
	defer truncate(t, db, expectedOrdId, "packages")
	expectedRestoringNames := []string{"one-file-deleted-1.csv", "one-file-deleted-2", "one-dir-deleted-1", "two-file-deleted-1.csv", "two-dir-deleted-1", "three-file-deleted-1.png"}
	store := NewQueries(db, expectedOrdId)
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
	db := openDB(t)
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	expectedOrdId := 2
	loadFromFile(t, db, "update-package-name-test.sql")
	defer truncate(t, db, expectedOrdId, "packages")

	checkResultQuery := fmt.Sprintf(`SELECT name from "%d".packages where id = $1`, expectedOrdId)
	store := NewQueries(db, expectedOrdId)

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
