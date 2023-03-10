package service

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

type PackagesService interface {
	RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest, undo bool) (*models.RestoreResponse, error)
}

type packagesService struct {
	StoreFactory store.SQLStoreFactory
	OrgId        int
}

func NewPackagesServiceWithFactory(factory store.SQLStoreFactory, orgId int) PackagesService {
	return &packagesService{StoreFactory: factory, OrgId: orgId}
}

func NewPackagesService(db *sql.DB, orgId int) PackagesService {
	str := store.NewSQLStoreFactory(db)
	svc := NewPackagesServiceWithFactory(str, orgId)
	return svc
}

func (s *packagesService) RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest, undo bool) (*models.RestoreResponse, error) {
	response := models.RestoreResponse{}
	err := s.StoreFactory.ExecStoreTx(ctx, s.OrgId, func(store store.SQLStore) error {
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
