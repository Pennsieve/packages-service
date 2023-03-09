package service

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

type DatasetsService interface {
	GetDataset(ctx context.Context, datasetId string) (*pgdb.Dataset, error)
	GetTrashcanPage(ctx context.Context, datasetID string, rootNodeId string, limit int, offset int) (*models.TrashcanPage, error)
}

type PackagesService interface {
	RestorePackages(ctx context.Context, datasetId string, request *models.RestoreRequest, undo bool) (*models.RestoreResponse, error)
}

type packagesService struct {
	StoreFactory store.DatasetsStoreFactory
	OrgId        int
}

func NewPackagesServiceWithFactory(factory store.DatasetsStoreFactory, orgId int) PackagesService {
	return &packagesService{StoreFactory: factory, OrgId: orgId}
}

func NewPackagesService(db *sql.DB, orgId int) PackagesService {
	str := store.NewDatasetsStoreFactory(db)
	svc := NewPackagesServiceWithFactory(str, orgId)
	return svc
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

func (s *packagesService) RestorePackages(ctx context.Context, datasetId string, request *models.RestoreRequest, undo bool) (*models.RestoreResponse, error) {
	response := models.RestoreResponse{}
	err := s.StoreFactory.ExecStoreTx(ctx, s.OrgId, func(store store.DatasetsStore) error {
		dataset, err := store.GetDatasetByNodeId(ctx, datasetId)
		if err != nil {
			return err
		}
		var restoring []*pgdb.Package
		for _, nodeId := range request.NodeIds {
			if p, err := store.TransitionPackageState(ctx, dataset.Id, nodeId, packageState.Deleted, packageState.Restoring); err == nil {
				restoring = append(restoring, p)
				response.Success = append(response.Success, nodeId)
			} else {
				switch err.(type) {
				case models.PackageNotFoundError:
					// No error returned here because we don't want to roll back Tx in this case.
					response.Failures = append(response.Failures, models.Failure{Id: nodeId, Error: fmt.Sprintf("deleted package %s not found in dataset %s", nodeId, datasetId)})
				default:
					response.Failures = append(response.Failures, models.Failure{Id: nodeId, Error: fmt.Sprintf("unexpected error restoring package: %v", err)})
					return err
				}
			}
		}
		// Temporarily, switch states back until sqs -> lambda is complete
		if undo {
			for _, p := range restoring {
				_, err := store.TransitionPackageState(ctx, dataset.Id, p.NodeId, packageState.Restoring, packageState.Deleted)
				if err != nil {
					return fmt.Errorf("unable to reverse state of package %s: %w", p.NodeId, err)
				}
			}
		}
		return nil
	})
	return &response, err
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
