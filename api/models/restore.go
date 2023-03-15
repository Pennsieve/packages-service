package models

import (
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

type RestoreRequest struct {
	NodeIds []string `json:"nodeIds"`
}

type RestoreResponse struct {
	Success  []string  `json:"success"`
	Failures []Failure `json:"failures"`
}

type Failure struct {
	Id    string `json:"id"`
	Error string `json:"error"`
}

type RestorePackageInfo struct {
	NodeId   string           `json:"nodeId"`
	Name     string           `json:"name"`
	ParentId *int64           `json:"parentId"`
	Type     packageType.Type `json:"type"`
	Size     *int64           `json:"size"`
}

type RestorePackageMessage struct {
	OrgId     int                  `json:"orgId"`
	DatasetId int64                `json:"datasetId"`
	Packages  []RestorePackageInfo `json:"packages"`
}

func NewRestorePackageMessage(orgId int, datasetId int64, toBeRestored ...*pgdb.Package) RestorePackageMessage {
	packages := make([]RestorePackageInfo, len(toBeRestored))
	queueMessage := RestorePackageMessage{OrgId: orgId, DatasetId: datasetId, Packages: packages}
	for i, p := range toBeRestored {
		restoreInfo := RestorePackageInfo{NodeId: p.NodeId, Name: p.Name, Type: p.PackageType}
		if p.ParentId.Valid {
			restoreInfo.ParentId = &p.ParentId.Int64
		}
		if p.Size.Valid {
			restoreInfo.Size = &p.Size.Int64
		}
		packages[i] = restoreInfo

	}
	return queueMessage
}
