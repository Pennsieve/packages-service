package models

import (
	"fmt"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
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

type FolderNotFoundError struct {
	OrgId      int
	NodeId     string
	DatasetId  DatasetId
	ActualType packageType.Type
}

func (e FolderNotFoundError) Error() string {
	if e.ActualType < 0 {
		return fmt.Sprintf("folder with node id %q not found in dataset %s, workspace %d", e.NodeId, e.DatasetId, e.OrgId)
	}
	return fmt.Sprintf("folder with node id %q not found in dataset %s, workspace %d (actual type %s)", e.NodeId, e.DatasetId, e.OrgId, e.ActualType)
}
