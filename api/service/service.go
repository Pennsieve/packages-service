package service

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

type PackagesService interface {
	RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest, undo bool) (*models.RestoreResponse, error)
}

type packagesService struct {
	SQSStoreFactory store.SQLStoreFactory
	QueueStore      store.QueueStore
	OrgId           int
}

func newPackagesServiceWithFactory(factory store.SQLStoreFactory, orgId int) *packagesService {
	return &packagesService{SQSStoreFactory: factory, OrgId: orgId}
}

func (s *packagesService) withQueueStore(queueStore store.QueueStore) *packagesService {
	s.QueueStore = queueStore
	return s
}

func NewPackagesService(db *sql.DB, awsConfig aws.Config, orgId int) (PackagesService, error) {
	str := store.NewSQLStoreFactory(db)
	svc := newPackagesServiceWithFactory(str, orgId)
	queueStore, err := store.NewQueueStore(awsConfig)
	if err != nil {
		return nil, err
	}
	return svc.withQueueStore(queueStore), nil
}

func (s *packagesService) RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest, undo bool) (*models.RestoreResponse, error) {
	response := models.RestoreResponse{}
	err := s.SQSStoreFactory.ExecStoreTx(ctx, s.OrgId, func(store store.SQLStore) error {
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
		queueMessage := models.RestorePackageMessage{NodeIds: response.Success}
		if err = s.QueueStore.SendRestorePackage(ctx, queueMessage); err != nil {
			// This will rollback Tx even though it's not a DB action.
			return err
		}
		return nil
	})
	return &response, err
}
