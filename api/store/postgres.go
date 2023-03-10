package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	pg "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"strings"
)

var (
	packagesColumns      = []string{"id", "name", "type", "state", "node_id", "parent_id", "dataset_id", "owner_id", "size", "import_id", "attributes", "created_at", "updated_at"}
	packageColumnsString = strings.Join(packagesColumns, ", ")
)

type SQLStoreFactory interface {
	NewSimpleStore(orgId int) SQLStore
	ExecStoreTx(ctx context.Context, orgId int, fn func(store SQLStore) error) error
}

func NewSQLStoreFactory(pennsieveDB *sql.DB) SQLStoreFactory {
	return &sqlStoreFactory{DB: pennsieveDB}
}

type sqlStoreFactory struct {
	DB *sql.DB
}

// NewSimpleStore returns a PackagesStore instance that
// will run statements directly on database
func (f *sqlStoreFactory) NewSimpleStore(orgId int) SQLStore {
	return NewQueries(f.DB, orgId)
}

// ExecStoreTx will execute the function fn, passing in a new SQLStore instance that
// is backed by a database transaction. Any methods fn runs against the passed in SQLStore will run
// in this transaction. If fn returns a non-nil error, the transaction will be rolled back.
// Otherwise, the transaction will be committed.
func (f *sqlStoreFactory) ExecStoreTx(ctx context.Context, orgId int, fn func(store SQLStore) error) error {
	tx, err := f.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := NewQueries(tx, orgId)
	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}

type Queries struct {
	db    pg.DBTX
	OrgId int
}

func NewQueries(db pg.DBTX, orgId int) *Queries {
	return &Queries{db: db, OrgId: orgId}
}

func (q *Queries) TransitionPackageState(ctx context.Context, datasetId int64, packageId string, expectedState, targetState packageState.State) (*pgdb.Package, error) {
	query := fmt.Sprintf(`UPDATE "%d".packages SET state = $1 WHERE node_id = $2 AND dataset_id = $3 AND state = $4 RETURNING %s`, q.OrgId, packageColumnsString)
	var pkg pgdb.Package
	if err := q.db.QueryRowContext(ctx, query, targetState, packageId, datasetId, expectedState).Scan(
		&pkg.Id,
		&pkg.Name,
		&pkg.PackageType,
		&pkg.PackageState,
		&pkg.NodeId,
		&pkg.ParentId,
		&pkg.DatasetId,
		&pkg.OwnerId,
		&pkg.Size,
		&pkg.ImportId,
		&pkg.Attributes,
		&pkg.CreatedAt,
		&pkg.UpdatedAt); errors.Is(err, sql.ErrNoRows) {
		return &pkg, models.PackageNotFoundError{Id: models.PackageNodeId(packageId), OrgId: q.OrgId, DatasetId: models.DatasetIntId(datasetId)}
	} else {
		return &pkg, err
	}
}

func (q *Queries) GetDatasetByNodeId(ctx context.Context, dsNodeId string) (*pgdb.Dataset, error) {
	const datasetColumns = "id, name, state, description, updated_at, created_at, node_id, permission_bit, type, role, status, automatically_process_packages, license, tags, contributors, banner_id, readme_id, status_id, publication_status_id, size, etag, data_use_agreement_id, changelog_id"
	var ds pgdb.Dataset
	query := fmt.Sprintf(`SELECT %s FROM "%d".datasets WHERE node_id = $1`, datasetColumns, q.OrgId)
	if err := q.db.QueryRowContext(ctx, query, dsNodeId).Scan(
		&ds.Id,
		&ds.Name,
		&ds.State,
		&ds.Description,
		&ds.UpdatedAt,
		&ds.CreatedAt,
		&ds.NodeId,
		&ds.PermissionBit,
		&ds.Type,
		&ds.Role,
		&ds.Status,
		&ds.AutomaticallyProcessPackages,
		&ds.License,
		&ds.Tags,
		&ds.Contributors,
		&ds.BannerId,
		&ds.ReadmeId,
		&ds.StatusId,
		&ds.PublicationStatusId,
		&ds.Size,
		&ds.ETag,
		&ds.DataUseAgreementId,
		&ds.ChangelogId); errors.Is(err, sql.ErrNoRows) {
		return &ds, models.DatasetNotFoundError{Id: models.DatasetNodeId(dsNodeId), OrgId: q.OrgId}
	} else {
		return &ds, err
	}
}

type SQLStore interface {
	GetDatasetByNodeId(ctx context.Context, dsNodeId string) (*pgdb.Dataset, error)
	TransitionPackageState(ctx context.Context, datasetId int64, packageId string, expectedState, targetState packageState.State) (*pgdb.Package, error)
}
