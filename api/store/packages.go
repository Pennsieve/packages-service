package store

import (
	"fmt"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"strings"
)

type RowScanner interface {
	Scan(dest ...any) error
}

type ModelScanner struct {
	ColumnNames       []string
	ColumnNamesString string
}

func NewModelScanner(columnNames []string) *ModelScanner {
	return &ModelScanner{ColumnNames: columnNames, ColumnNamesString: strings.Join(packagesColumns, ", ")}
}

func (ms *ModelScanner) QualifiedColumnNamesString(qualifier string) string {
	q := make([]string, len(ms.ColumnNames))
	for i, c := range ms.ColumnNames {
		q[i] = fmt.Sprintf("%s.%s", qualifier, c)
	}
	return strings.Join(q, ", ")
}

type PackageScanner struct {
	*ModelScanner
	QualifiedColumnNamesString string
}

func NewPackageScanner(columnNames []string) *PackageScanner {
	ms := NewModelScanner(columnNames)
	q := ms.QualifiedColumnNamesString("packages")
	return &PackageScanner{
		ModelScanner:               ms,
		QualifiedColumnNamesString: q,
	}
}

func (s PackageScanner) Scan(scanner RowScanner, pkg *pgdb.Package) error {
	return scanner.Scan(&pkg.Id,
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
		&pkg.UpdatedAt)
}
