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
