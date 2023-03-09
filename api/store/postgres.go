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
	"strconv"
	"strings"
)

var (
	packagesColumns            = []string{"id", "name", "type", "state", "node_id", "parent_id", "dataset_id", "owner_id", "size", "import_id", "attributes", "created_at", "updated_at"}
	packageColumnsString       = strings.Join(packagesColumns, ", ")
	getTrashcanPageQueryFormat = `WITH RECURSIVE trash(id, node_id, type, parent_id, name, state, id_path) AS
                                  (
									SELECT id, node_id, type, %[1]s, name, state, ARRAY [id]
									FROM "%[4]d".packages
									WHERE parent_id %[2]s
									AND dataset_id = $1
                                  UNION ALL
									SELECT p.id, p.node_id, p.type, p.parent_id, p.name, p.state, id_path || p.id
									FROM "%[4]d".packages p
									JOIN trash t ON t.id = p.parent_id
									WHERE t.state <> 'DELETED'
                                  )
                                  SELECT %[3]s, COUNT(*) OVER() as total_count
                                  FROM trash t JOIN "%[4]d".packages p ON t.id = p.id
                                  WHERE t.parent_id %[2]s
  					              AND EXISTS(SELECT 1 from trash t2 where t2.state = 'DELETED' and t.id = ANY(t2.id_path))
					              ORDER BY t.name, t.id
					              LIMIT $2 OFFSET $3;`
)

type PackagePage struct {
	TotalCount int
	Packages   []pgdb.Package
}

type SQLStoreFactory interface {
	NewSimpleStore(orgId int) DatasetsStore
	ExecStoreTx(ctx context.Context, orgId int, fn func(store DatasetsStore) error) error
}

type DatasetsStoreFactory SQLStoreFactory

func NewDatasetsStoreFactory(pennsieveDB *sql.DB) DatasetsStoreFactory {
	return &sqlStoreFactory{DB: pennsieveDB}
}

type sqlStoreFactory struct {
	DB *sql.DB
}

// NewSimpleStore returns a PackagesStore instance that
// will run statements directly on database
func (f *sqlStoreFactory) NewSimpleStore(orgId int) DatasetsStore {
	return NewQueries(f.DB, orgId)
}

// ExecStoreTx will execute the function fn, passing in a new DatasetsStore instance that
// is backed by a database transaction. Any methods fn runs against the passed in DatasetsStore will run
// in this transaction. If fn returns a non-nil error, the transaction will be rolled back.
// Otherwise, the transaction will be committed.
func (f *sqlStoreFactory) ExecStoreTx(ctx context.Context, orgId int, fn func(store DatasetsStore) error) error {
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

func (q *Queries) CountDatasetPackagesByState(ctx context.Context, datasetId int64, state packageState.State) (int, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%d".packages where dataset_id = $1 and state = $2`, q.OrgId)
	var count int
	err := q.db.QueryRowContext(ctx, query, datasetId, state).Scan(&count)
	return count, err
}

func (q *Queries) GetDatasetPackageByNodeId(ctx context.Context, datasetId int64, packageNodeId string) (*pgdb.Package, error) {
	var pckg pgdb.Package
	queryStr := fmt.Sprintf(`SELECT %s FROM "%d".packages where dataset_id = $1 and node_id = $2`, packageColumnsString, q.OrgId)
	if err := q.db.QueryRowContext(ctx, queryStr, datasetId, packageNodeId).Scan(&pckg); errors.Is(err, sql.ErrNoRows) {
		return &pckg, models.PackageNotFoundError{Id: models.PackageNodeId(packageNodeId), OrgId: q.OrgId, DatasetId: models.DatasetIntId(datasetId)}
	} else {
		return &pckg, err
	}
}

func (q *Queries) queryTrashcan(ctx context.Context, query string, datasetId int64, limit int, offset int) (*PackagePage, error) {
	rows, err := q.db.QueryContext(ctx, query, datasetId, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var page PackagePage
	var totalCount int
	packages := make([]pgdb.Package, limit)
	i := 0
	for rows.Next() {
		p := &packages[i]
		if err := rows.Scan(
			&p.Id,
			&p.Name,
			&p.PackageType,
			&p.PackageState,
			&p.NodeId,
			&p.ParentId,
			&p.DatasetId,
			&p.OwnerId,
			&p.Size,
			&p.ImportId,
			&p.Attributes,
			&p.CreatedAt,
			&p.UpdatedAt,
			&totalCount); err != nil {
			return &page, err
		}
		i++
	}
	if err := rows.Err(); err != nil {
		return &page, err
	}
	page.TotalCount = totalCount
	page.Packages = packages[:i]

	return &page, nil
}

func (q *Queries) GetTrashcanRootPaginated(ctx context.Context, datasetId int64, limit int, offset int) (*PackagePage, error) {
	getTrashcanRootPageQuery := fmt.Sprintf(getTrashcanPageQueryFormat, "null::integer", "is null", qualifiedColumns("p", packagesColumns), q.OrgId)
	return q.queryTrashcan(ctx, getTrashcanRootPageQuery, datasetId, limit, offset)
}

func (q *Queries) GetTrashcanPaginated(ctx context.Context, datasetId int64, parentId int64, limit int, offset int) (*PackagePage, error) {
	pIdStr := strconv.FormatInt(parentId, 10)
	equalPIdStr := fmt.Sprintf("= %d", parentId)
	query := fmt.Sprintf(getTrashcanPageQueryFormat, pIdStr, equalPIdStr, qualifiedColumns("p", packagesColumns), q.OrgId)
	return q.queryTrashcan(ctx, query, datasetId, limit, offset)
}

func qualifiedColumns(table string, columns []string) string {
	q := make([]string, len(columns))
	for i, c := range columns {
		q[i] = fmt.Sprintf("%s.%s", table, c)
	}
	return strings.Join(q, ", ")
}

type DatasetsStore interface {
	GetDatasetByNodeId(ctx context.Context, dsNodeId string) (*pgdb.Dataset, error)
	GetTrashcanRootPaginated(ctx context.Context, datasetId int64, limit int, offset int) (*PackagePage, error)
	GetTrashcanPaginated(ctx context.Context, datasetId int64, parentId int64, limit int, offset int) (*PackagePage, error)
	CountDatasetPackagesByState(ctx context.Context, datasetId int64, state packageState.State) (int, error)
	GetDatasetPackageByNodeId(ctx context.Context, datasetId int64, packageNodeId string) (*pgdb.Package, error)
	TransitionPackageState(ctx context.Context, datasetId int64, packageId string, expectedState, targetState packageState.State) (*pgdb.Package, error)
}
