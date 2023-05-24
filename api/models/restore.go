package models

import (
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

type RestoreRequest struct {
	NodeIds []string `json:"nodeIds"`
	UserId  string   `json:"userId"`
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
	Id       int64            `json:"id"`
	NodeId   string           `json:"nodeId"`
	Name     string           `json:"name"`
	ParentId *int64           `json:"parentId"`
	Type     packageType.Type `json:"type"`
}

type RestorePackageMessage struct {
	OrgId     int                `json:"orgId"`
	DatasetId int64              `json:"datasetId"`
	UserId    string             `json:"userId"`
	Package   RestorePackageInfo `json:"package"`
}

func NewRestorePackageInfo(p *pgdb.Package) RestorePackageInfo {
	restoreInfo := RestorePackageInfo{Id: p.Id, NodeId: p.NodeId, Name: p.Name, Type: p.PackageType}
	if p.ParentId.Valid {
		restoreInfo.ParentId = &p.ParentId.Int64
	}
	return restoreInfo
}

func NewRestorePackageMessage(orgId int, datasetId int64, userId string, toBeRestored *pgdb.Package) RestorePackageMessage {
	restoreInfo := NewRestorePackageInfo(toBeRestored)
	queueMessage := RestorePackageMessage{OrgId: orgId, DatasetId: datasetId, UserId: userId, Package: restoreInfo}
	return queueMessage
}
