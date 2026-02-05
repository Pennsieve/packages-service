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
	ColumnNames                []string
	ColumnNamesString          string
	QualifiedColumnNamesString string
}

func NewModelScanner(qualifier string, columnNames []string) *ModelScanner {
	return &ModelScanner{
		ColumnNames:                columnNames,
		ColumnNamesString:          strings.Join(columnNames, ", "),
		QualifiedColumnNamesString: qualifiedColumnNamesString(qualifier, columnNames),
	}
}

func qualifiedColumnNamesString(qualifier string, columnNames []string) string {
	q := make([]string, len(columnNames))
	for i, c := range columnNames {
		q[i] = fmt.Sprintf("%s.%s", qualifier, c)
	}
	return strings.Join(q, ", ")
}

type PackageScanner struct {
	*ModelScanner
}

func NewPackageScanner(columnNames []string) *PackageScanner {
	ms := NewModelScanner("packages", columnNames)
	return &PackageScanner{
		ModelScanner: ms,
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
