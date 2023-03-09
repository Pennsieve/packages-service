package service

import (
	"context"
	"database/sql"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

type PackagesService interface {
	RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest) (*models.RestoreResponse, error)
}

type DatasetsService interface {
	GetDataset(ctx context.Context, datasetId string) (*pgdb.Dataset, error)
	GetTrashcanPage(ctx context.Context, datasetID string, rootNodeId string, limit int, offset int) (*models.TrashcanPage, error)
}

type datasetsService struct {
	StoreFactory store.DatasetsStoreFactory
	OrgId        int
}

func NewDatasetsServiceWithFactory(factory store.DatasetsStoreFactory, orgId int) DatasetsService {
	return &datasetsService{StoreFactory: factory, OrgId: orgId}
}

func NewDatasetsService(db *sql.DB, orgId int) DatasetsService {
	str := store.NewDatasetsStoreFactory(db)
	datasetsSvc := NewDatasetsServiceWithFactory(str, orgId)
	return datasetsSvc
}

func (s *datasetsService) GetTrashcanPage(ctx context.Context, datasetId string, rootNodeId string, limit int, offset int) (*models.TrashcanPage, error) {
	trashcan := models.TrashcanPage{Limit: limit, Offset: offset, Messages: []string{}}
	err := s.StoreFactory.ExecStoreTx(ctx, s.OrgId, func(q store.DatasetsStore) error {
		dataset, err := q.GetDatasetByNodeId(ctx, datasetId)
		if err != nil {
			return err
		}
		deletedCount, err := q.CountDatasetPackagesByState(ctx, dataset.Id, packageState.Deleted)
		if err != nil || deletedCount == 0 {
			return err
		}
		var page *store.PackagePage
		if len(rootNodeId) == 0 {
			page, err = q.GetTrashcanRootPaginated(ctx, dataset.Id, limit, offset)
		} else {
			rootPckg, pckgErr := q.GetDatasetPackageByNodeId(ctx, dataset.Id, rootNodeId)
			if pckgErr != nil {
				return pckgErr
			}
			if rootPckg.PackageType != packageType.Collection {
				return models.FolderNotFoundError{OrgId: s.OrgId, NodeId: rootNodeId, DatasetId: models.DatasetNodeId(datasetId), ActualType: rootPckg.PackageType}
			}
			page, err = q.GetTrashcanPaginated(ctx, dataset.Id, rootPckg.Id, limit, offset)
		}
		if err != nil {
			return err
		}
		packages := make([]models.TrashcanItem, len(page.Packages))
		for i, p := range page.Packages {
			packages[i] = models.TrashcanItem{
				ID:     p.Id,
				Name:   p.Name,
				NodeId: p.NodeId,
				Type:   p.PackageType.String(),
				State:  p.PackageState.String(),
			}
		}
		trashcan.TotalCount = page.TotalCount
		trashcan.Packages = packages
		return nil
	})
	return &trashcan, err
}

func (s *datasetsService) GetDataset(ctx context.Context, datasetId string) (*pgdb.Dataset, error) {
	q := s.StoreFactory.NewSimpleStore(s.OrgId)
	return q.GetDatasetByNodeId(ctx, datasetId)
}
