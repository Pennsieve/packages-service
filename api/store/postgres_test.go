package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// pingUntilReady pings the db up to 10 times, stopping when
// a ping is successful. Used because there have been problems with
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

func TestDBConnect(t *testing.T) {
	config := PostgresConfigFromEnv()

	db, err := config.OpenAtSchema("pennsieve")
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	if assert.NoErrorf(t, err, "could not open postgres DB with config %s", config) {
		err = pingUntilReady(db)
		assert.NoErrorf(t, err, "could not ping postgres DB with config %s", config)
	}
}

func TestGetDatasetByNodeId(t *testing.T) {
	config := PostgresConfigFromEnv()
	db, err := config.Open()
	defer func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	assert.NoErrorf(t, err, "could not open DB with config %s", config)

	orgId := 3
	store := NewQueries(db, orgId)
	input := pgdb.Dataset{
		Id:           1,
		Name:         "Test Dataset",
		State:        "READY",
		Description:  sql.NullString{},
		NodeId:       sql.NullString{String: "N:dataset:1234", Valid: true},
		Role:         sql.NullString{String: "editor", Valid: true},
		Tags:         pgdb.Tags{"test", "sql"},
		Contributors: pgdb.Contributors{},
		StatusId:     int32(1),
	}
	insert := fmt.Sprintf("INSERT INTO \"%d\".datasets (id, name, state, description, node_id, role, tags, contributors, status_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", orgId)
	_, err = db.Exec(insert, input.Id, input.Name, input.State, input.Description, input.NodeId, input.Role, input.Tags, input.Contributors, input.StatusId)
	defer truncate(t, db, orgId, "datasets")

	if assert.NoError(t, err) {
		actual, err := store.GetDatasetByNodeId(context.Background(), input.NodeId.String)
		if assert.NoError(t, err) {
			assert.Equal(t, input.Name, actual.Name)
			assert.Equal(t, input.State, actual.State)
			assert.Equal(t, input.NodeId, actual.NodeId)
			assert.Equal(t, input.Role, actual.Role)
			assert.Equal(t, input.StatusId, actual.StatusId)

			assert.Equal(t, input.Tags, actual.Tags)
			assert.Equal(t, input.Contributors, actual.Contributors)
			assert.False(t, actual.Description.Valid)
		}
	}

}

func TestGetTrashcanPaginated(t *testing.T) {
	rootNodeIdToExpectedLevel := map[int64]TrashcanLevel{
		// Level zero
		0: {
			"N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8": {
				Name:  "root-file-deleted-1.txt",
				Type:  packageType.Text,
				State: packageState.Deleted,
			},
			"N:collection:82c127ca-b72b-4d8b-a0c3-a9e4c7b14654": {
				Name:  "root-dir-deleted-1",
				Type:  packageType.Collection,
				State: packageState.Deleted,
			},
			"N:collection:180d4f48-ea2b-435c-ac69-780eeaf89745": {
				Name:  "root-dir-1",
				Type:  packageType.Collection,
				State: packageState.Ready,
			},
		},
		// Level one: root-dir-1
		5: {
			"N:package:7a1e270b-eb23-4b26-b106-d32101399a8a": {
				Name:  "one-file-deleted-1.csv",
				Type:  packageType.CSV,
				State: packageState.Deleted,
			},
			"N:collection:e9bfe050-b375-43a1-91ec-b519439ad011": {
				Name:  "one-dir-1",
				Type:  packageType.Collection,
				State: packageState.Ready,
			},
			"N:collection:b8ab062e-e7d0-4668-b098-c322ae460820": {
				Name:  "one-dir-deleted-1",
				Type:  packageType.Collection,
				State: packageState.Deleted,
			},
		},
		// Level two: root-dir-1/one-dir-1
		9: {
			"N:package:b234f34b-a827-4df1-ac79-e9c0db53915c": {
				Name:  "two-file-deleted-1.csv",
				Type:  packageType.CSV,
				State: packageState.Deleted,
			},
			"N:package:06d2e3d0-e084-4866-8bfc-206655ec4d5c": {
				Name:  "two-file-deleted-2.csv",
				Type:  packageType.CSV,
				State: packageState.Deleted,
			},
			"N:collection:113d3c44-af35-408f-9fcc-0e4aa0b20a5d": {
				Name:  "two-dir-1",
				Type:  packageType.Collection,
				State: packageState.Ready,
			},
			"N:collection:a3d2d4a4-039c-4525-b99f-148690006b4f": {
				Name:  "two-dir-deleted-1",
				Type:  packageType.Collection,
				State: packageState.Deleted,
			},
		},
		// Level three: root-dir-1/one-dir-1/two-dir-1
		21: {
			"N:package:53c00fad-426e-42d4-b242-f5237d2eec64": {
				Name:  "three-file-deleted-1.txt",
				Type:  packageType.Text,
				State: packageState.Deleted,
			},
			"N:collection:98d2c5e1-0be5-48e1-bbc0-10290e8fc6a0": {
				Name:  "three-dir-1",
				Type:  packageType.Collection,
				State: packageState.Ready,
			},
			"N:collection:ab0ae7fd-96d1-4f61-af0c-f7b6e7ea7639": {
				Name:  "three-dir-deleted-1",
				Type:  packageType.Collection,
				State: packageState.Deleted,
			},
			"N:collection:f4136743-e930-401e-88bb-e7ef34789a88": {
				Name:  "three-dir-deleted-2",
				Type:  packageType.Collection,
				State: packageState.Deleted,
			},
		},
		// Level four: root-dir-1/one-dir-1/two-dir-1/three-dir-1
		31: {
			"N:package:8180a4dd-bf19-4476-ae54-79018dc14821": {
				Name:  "four-file-deleted-1.png",
				Type:  packageType.Image,
				State: packageState.Deleted,
			},
		},
		// Level four: root-dir-1/one-dir-1/two-dir-1/three-dir-deleted-1
		33: {
			"N:package:67c7567e-183e-4701-8543-8630aba5fbc2": {
				Name:  "four-file-deleted-1",
				Type:  packageType.Unsupported,
				State: packageState.Deleted,
			},
		},
		// Level four: root-dir-1/one-dir-1/two-dir-1/three-dir-deleted-2
		34: {
			"N:package:c4d0049b-4cf8-4729-935c-67e9701d33b8": {
				Name:  "four-file-deleted-1.png",
				Type:  packageType.Image,
				State: packageState.Deleted,
			},
		},
		// Level three: root-dir-1/one-dir-1/two-dir-deleted-1
		22: {
			"N:package:14298d95-0b87-4b15-b8fe-3007980657df": {
				Name:  "three-file-deleted-1.csv",
				Type:  packageType.CSV,
				State: packageState.Deleted,
			},
		},
		// Level two: root-dir-1/one-dir-deleted-1
		10: {
			"N:package:bb5970ae-594d-42d2-a223-f38a55eaa3b8": {
				Name:  "two-file-deleted-1.csv",
				Type:  packageType.CSV,
				State: packageState.Deleted,
			},
		},
		// Level one: root-dir-deleted-1
		4: {
			"N:package:8d18065b-e7d7-4792-8de4-6fc7ecb79a46": {
				Name:  "one-file-deleted-1.csv",
				Type:  packageType.CSV,
				State: packageState.Deleted,
			},
			"N:package:40443908-a2e1-474c-8367-d04ffbda7947": {
				Name:  "one-file-deleted-2",
				Type:  packageType.Unsupported,
				State: packageState.Deleted,
			},
			"N:collection:8397346c-b824-4ee7-a49d-892860892d41": {
				Name:  "one-dir-deleted-1",
				Type:  packageType.Collection,
				State: packageState.Deleted,
			},
		},
		// Level two: root-dir-deleted-1/one-dir-deleted-1
		15: {
			"N:package:d9ee5d8f-0f27-4179-ae9e-8b914a719543": {
				Name:  "two-file-deleted-1.csv",
				Type:  packageType.CSV,
				State: packageState.Deleted,
			},
			"N:collection:92907aeb-a524-4b74-960c-ddda270bf1ce": {
				Name:  "two-dir-deleted-1",
				Type:  packageType.Collection,
				State: packageState.Deleted,
			},
		},
		// Level three: root-dir-deleted-1/one-dir-deleted-1/two-dir-deleted-1
		26: {
			"N:package:6974bfb6-2714-4f80-8942-c34357dfeee0": {
				Name:  "three-file-deleted-1.png",
				Type:  packageType.Image,
				State: packageState.Deleted,
			},
		},
		// Folder with no deleted file or folder descendants
		6: {},
	}
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
	for rootId, expectedLevel := range rootNodeIdToExpectedLevel {
		t.Run(fmt.Sprintf("GetTrashcan starting at folder %d", rootId), func(t *testing.T) {
			testGetTrashcanLevel(t, store, rootId, expectedLevel)
		})
	}

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

func testGetTrashcanLevel(t *testing.T, store DatasetsStore, rootFolderId int64, expectedLevel TrashcanLevel) {
	var page *PackagePage
	var err error
	if rootFolderId == 0 {
		page, err = store.GetTrashcanRootPaginated(context.Background(), 1, 10, 0)
	} else {
		page, err = store.GetTrashcanPaginated(context.Background(), 1, rootFolderId, 10, 0)
	}

	if assert.NoError(t, err) {
		assert.Equal(t, len(expectedLevel), page.TotalCount)
		actualLevel := summarize(page.Packages)
		assert.Equal(t, expectedLevel, actualLevel)
	}

}

func TestGetPackageByNodeId_BadPackage(t *testing.T) {
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
	ordId := 2
	datasetId := int64(1)
	store := NewQueries(db, ordId)
	badRootNodeId := "N:collection:bad"
	_, err = store.GetDatasetPackageByNodeId(context.Background(), datasetId, badRootNodeId)
	if assert.Error(t, err) {
		assert.Equal(t, models.PackageNotFoundError{OrgId: ordId, Id: models.PackageNodeId(badRootNodeId), DatasetId: models.DatasetIntId(datasetId)}, err)
	}

}

type PackageSummary struct {
	Name  string
	Type  packageType.Type
	State packageState.State
}

// ExpectedLevel maps a collection package id to a summary of its trashcan results
type TrashcanLevel map[string]PackageSummary

func summarize(packages []pgdb.Package) TrashcanLevel {
	summary := make(map[string]PackageSummary, len(packages))
	for _, p := range packages {
		summary[p.NodeId] = PackageSummary{
			Name:  p.Name,
			Type:  p.PackageType,
			State: p.PackageState,
		}
	}
	return summary
}
