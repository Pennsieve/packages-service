package models

import "fmt"

type pennsieveId struct {
	NodeId string
	Id     int64
}

func (i pennsieveId) String() string {
	if len(i.NodeId) > 0 {
		return i.NodeId
	}
	return fmt.Sprintf("%d", i.Id)
}

func pIntId(intId int64) pennsieveId {
	return pennsieveId{Id: intId}
}

func pNodeId(nodeId string) pennsieveId {
	return pennsieveId{NodeId: nodeId}
}

type PackageId struct {
	pennsieveId
}

func PackageIntId(intId int64) PackageId {
	return PackageId{pIntId(intId)}
}

func PackageNodeId(nodeId string) PackageId {
	return PackageId{pNodeId(nodeId)}
}

type DatasetId struct {
	pennsieveId
}

func DatasetIntId(intId int64) DatasetId {
	return DatasetId{pIntId(intId)}
}

func DatasetNodeId(nodeId string) DatasetId {
	return DatasetId{pNodeId(nodeId)}
}
