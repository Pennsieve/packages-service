package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
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
			actual, err := GetOriginalName(deletedName, nodeId)
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

func TestNewNameParts(t *testing.T) {
	for name, testData := range map[string]struct {
		input        string
		expectedBase string
		expectedExt  string
	}{
		"no extension":      {"test", "test", ""},
		"extension":         {"test.txt", "test", ".txt"},
		"more than one dot": {"test.main.txt", "test.main", ".txt"},
		"final dot":         {"test.", "test", "."},
	} {
		t.Run(name, func(t *testing.T) {
			actual := NewNameParts(testData.input)
			assert.Equal(t, testData.expectedBase, actual.Base)
			assert.Equal(t, testData.expectedExt, actual.Ext)
		})
	}
}

func TestNameParts_Next(t *testing.T) {
	parts := NewNameParts("file.txt")

	first := parts.Next()
	assert.Equal(t, "file-restored_1.txt", first)
	assert.True(t, parts.More())

	second := parts.Next()
	assert.Equal(t, "file-restored_2.txt", second)
	assert.True(t, parts.More())
}

func TestNameParts_Limit(t *testing.T) {
	parts := NameParts{
		Base:  "file",
		Ext:   ".txt",
		i:     0,
		limit: 2,
		more:  true,
	}

	first := parts.Next()
	assert.Equal(t, "file-restored_1.txt", first)
	assert.True(t, parts.More())

	afterLimit := parts.Next()
	assert.Regexp(t, "file-restored_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.txt", afterLimit)
	assert.False(t, parts.More())
}

func TestRestoreName(t *testing.T) {
	db := store.OpenDB(t)
	defer db.Close()
	orgId := 2
	for name, d := range map[string]struct {
		id             int64
		nodeId         string
		name           string
		expectedResult string
	}{"simple rename": {
		int64(1),
		"N:package:ae253796-256a-4b9e-ba80-1c4c5a2afe6b",
		"__DELETED__N:package:ae253796-256a-4b9e-ba80-1c4c5a2afe6b_file.txt",
		"file.txt",
	}, "conflict with non-deleted file": {
		int64(2),
		"N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8",
		"__DELETED__N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8_another-file.txt",
		"another-file-restored_1.txt",
	}} {
		db.ExecSQLFile("restore-package-name-test.sql")
		sqlFactory := store.NewPostgresStoreFactory(db.DB)
		ctx := context.Background()
		handler := NewMessageHandler(events.SQSMessage{}, NewBaseStore(sqlFactory, nil, nil))
		restoreInfo := models.RestorePackageInfo{
			Id:     d.id,
			NodeId: d.nodeId,
			Name:   d.name,
		}
		t.Run(name, func(t *testing.T) {
			err := handler.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
				return handler.restoreName(ctx, restoreInfo, store)
			})
			if assert.NoError(t, err) {
				query := fmt.Sprintf(`SELECT name from "%d".packages where id = $1`, orgId)
				var actualName string
				err = db.QueryRow(query, restoreInfo.Id).Scan(&actualName)
				if assert.NoError(t, err) {
					assert.Equal(t, d.expectedResult, actualName)
				}
			}
		})
		db.Truncate(orgId, "packages")

	}
}

func TestRestoreName_ConflictWithDeletedFile(t *testing.T) {
	db := store.OpenDB(t)
	defer db.Close()
	orgId := 2
	db.ExecSQLFile("restore-package-name-test.sql")
	defer db.Truncate(orgId, "packages")

	sqlFactory := store.NewPostgresStoreFactory(db.DB)
	ctx := context.Background()
	handler := NewMessageHandler(events.SQSMessage{}, NewBaseStore(sqlFactory, nil, nil))
	restoreInfo1 := models.RestorePackageInfo{
		Id:     5,
		NodeId: "N:collection:180d4f48-ea2b-435c-ac69-780eeaf89745",
		Name:   "__DELETED__N:collection:180d4f48-ea2b-435c-ac69-780eeaf89745_root-dir",
	}
	restoreInfo2 := models.RestorePackageInfo{
		Id:     6,
		NodeId: "N:collection:0f197fab-cb7b-4414-8f7c-27d7aafe7c53",
		Name:   "__DELETED__N:collection:0f197fab-cb7b-4414-8f7c-27d7aafe7c53_root-dir",
	}

	err := handler.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		err := handler.restoreName(ctx, restoreInfo1, store)
		if assert.NoError(t, err) {
			err = handler.restoreName(ctx, restoreInfo2, store)
			assert.NoError(t, err)
		}
		return nil
	})
	if assert.NoError(t, err) {
		query := fmt.Sprintf(`SELECT name from "%d".packages where id = $1`, orgId)

		var actualName1 string
		err = db.QueryRow(query, restoreInfo1.Id).Scan(&actualName1)
		if assert.NoError(t, err) {
			assert.Equal(t, "root-dir", actualName1)
		}

		var actualName2 string
		err = db.QueryRow(query, restoreInfo2.Id).Scan(&actualName2)
		if assert.NoError(t, err) {
			assert.Equal(t, "root-dir-restored_1", actualName2)
		}
	}

}
