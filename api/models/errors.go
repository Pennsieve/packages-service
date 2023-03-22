package models

import (
	"fmt"
)

type DatasetNotFoundError struct {
	OrgId int
	Id    DatasetId
}

func (e DatasetNotFoundError) Error() string {
	return fmt.Sprintf("dataset %s not found in workspace %d", e.Id, e.OrgId)
}

type PackageNotFoundError struct {
	OrgId     int
	Id        PackageId
	DatasetId DatasetId
}

func (e PackageNotFoundError) Error() string {
	return fmt.Sprintf("package with node id %q not found in dataset %s, workspace %d", e.Id, e.DatasetId, e.OrgId)
}

type PackageNameUniquenessError struct {
	OrgId    int
	Id       PackageId
	Name     string
	SQLError error
}

func (e PackageNameUniquenessError) Error() string {
	return fmt.Sprintf("cannot update name of package %s in workspace %d to %s: %v", e.Id, e.OrgId, e.Name, e.SQLError)
}

func (e PackageNameUniquenessError) Unwrap() error {
	return e.SQLError
}
