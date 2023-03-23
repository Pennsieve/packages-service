package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	pg "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"strings"
)

const (
	uniqueViolationCode       = "23505"
	rootPackageNameConstraint = "packages_name_dataset_id__parent_id_null_idx"
	packageNameConstraint     = "packages_name_dataset_id_parent_id__parent_id_not_null_idx"
)

var (
	packagesColumns      = []string{"id", "name", "type", "state", "node_id", "parent_id", "dataset_id", "owner_id", "size", "import_id", "attributes", "created_at", "updated_at"}
	packageColumnsString = strings.Join(packagesColumns, ", ")
)

type SQLStoreFactory interface {
	NewSimpleStore(orgId int, logger logging.Logger) SQLStore
	ExecStoreTx(ctx context.Context, orgId int, logger logging.Logger, fn func(store SQLStore) error) error
}

func NewSQLStoreFactory(pennsieveDB *sql.DB) SQLStoreFactory {
	return &sqlStoreFactory{DB: pennsieveDB}
}

type sqlStoreFactory struct {
	DB *sql.DB
}

// NewSimpleStore returns a PackagesStore instance that
// will run statements directly on database
func (f *sqlStoreFactory) NewSimpleStore(orgId int, logger logging.Logger) SQLStore {
	return NewQueries(f.DB, orgId, logger)
}

// ExecStoreTx will execute the function fn, passing in a new SQLStore instance that
// is backed by a database transaction. Any methods fn runs against the passed in SQLStore will run
// in this transaction. If fn returns a non-nil error, the transaction will be rolled back.
// Otherwise, the transaction will be committed.
func (f *sqlStoreFactory) ExecStoreTx(ctx context.Context, orgId int, logger logging.Logger, fn func(store SQLStore) error) error {
	tx, err := f.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := NewQueries(tx, orgId, logger)
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
	logging.Logger
}

func NewQueries(db pg.DBTX, orgId int, logger logging.Logger) *Queries {
	return &Queries{db: db, OrgId: orgId, Logger: logger}
}

func (q *Queries) UpdatePackageName(ctx context.Context, packageId int64, newName string) (int64, error) {
	query := fmt.Sprintf(`UPDATE "%d".packages SET name = $1 WHERE id = $2`, q.OrgId)
	res, err := q.db.ExecContext(ctx, query, newName, packageId)
	if err != nil {
		if err, ok := err.(*pq.Error); ok && err.Code == uniqueViolationCode && (err.Constraint == rootPackageNameConstraint || err.Constraint == packageNameConstraint) {
			return 0, models.PackageNameUniquenessError{
				OrgId:    q.OrgId,
				Id:       models.PackageIntId(packageId),
				Name:     newName,
				SQLError: err,
			}
		}
		return 0, err
	}
	return res.RowsAffected()
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

func (q *Queries) TransitionDescendantPackageState(ctx context.Context, datasetId int64, parentId int64, expectedState, targetState packageState.State) ([]*pgdb.Package, error) {
	query := fmt.Sprintf(`WITH RECURSIVE nodes(id) AS (
							SELECT id FROM "%[1]d".packages
                             	WHERE parent_id = $1
							 	AND dataset_id = $2
								AND state = $3
							UNION ALL
                             SELECT o.id FROM "%[1]d".packages o
								JOIN nodes n on n.id = o.parent_id
								WHERE state = $3)
				UPDATE "%[1]d".packages
				SET state = $4
				WHERE state = $3 AND id IN (SELECT id FROM nodes n)
				RETURNING %s`, q.OrgId, packageColumnsString)
	var updated []*pgdb.Package
	rows, err := q.db.QueryContext(ctx, query, parentId, datasetId, expectedState, targetState)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			q.LogWarnWithFields(log.Fields{"error": closeErr}, "ignoring error while closing row in defer")
		}
	}()

	for rows.Next() {
		var pkg pgdb.Package
		if err = rows.Scan(&pkg.Id,
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
			&pkg.UpdatedAt); err != nil {
			return updated, err
		}
		updated = append(updated, &pkg)
	}
	if err = rows.Err(); err != nil {
		return updated, err
	}
	return updated, nil
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

func (q *Queries) IncrementDatasetStorage(ctx context.Context, datasetId int64, sizeIncrement int64) (int64, error) {
	query := fmt.Sprintf(`UPDATE "%d".dataset_storage SET size = COALESCE(size, 0) + $1 WHERE dataset_id = $2`, q.OrgId)
	res, err := q.db.ExecContext(ctx, query, datasetId, sizeIncrement)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (q *Queries) NewSavepoint(ctx context.Context, name string) error {
	stmt := fmt.Sprintf("SAVEPOINT %s", name)
	_, err := q.db.ExecContext(ctx, stmt)
	return err
}

func (q *Queries) RollbackToSavepoint(ctx context.Context, name string) error {
	stmt := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)
	_, err := q.db.ExecContext(ctx, stmt)
	return err
}

func (q *Queries) ReleaseSavepoint(ctx context.Context, name string) error {
	stmt := fmt.Sprintf("RELEASE SAVEPOINT %s", name)
	_, err := q.db.ExecContext(ctx, stmt)
	return err
}

type SQLStore interface {
	UpdatePackageName(ctx context.Context, packageId int64, newName string) (int64, error)
	GetDatasetByNodeId(ctx context.Context, dsNodeId string) (*pgdb.Dataset, error)
	TransitionPackageState(ctx context.Context, datasetId int64, packageId string, expectedState, targetState packageState.State) (*pgdb.Package, error)
	TransitionDescendantPackageState(ctx context.Context, datasetId int64, parentId int64, expectedState, targetState packageState.State) ([]*pgdb.Package, error)
	NewSavepoint(ctx context.Context, name string) error
	RollbackToSavepoint(ctx context.Context, name string) error
	ReleaseSavepoint(ctx context.Context, name string) error
	IncrementDatasetStorage(ctx context.Context, datasetId int64, sizeIncrement int64) (int64, error)
	logging.Logger
}
