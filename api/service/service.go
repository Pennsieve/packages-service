package service

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

type PackagesService interface {
	RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest) (*models.RestoreResponse, error)
}

type packagesService struct {
	SQLStoreFactory store.SQLStoreFactory
	QueueStore      store.QueueStore
	OrgId           int
	logging.Logger
}

func newPackagesServiceWithFactory(factory store.SQLStoreFactory, orgId int, logger logging.Logger) *packagesService {
	return &packagesService{SQLStoreFactory: factory, OrgId: orgId, Logger: logger}
}

func (s *packagesService) withQueueStore(queueStore store.QueueStore) *packagesService {
	s.QueueStore = queueStore
	return s
}

func NewPackagesService(db *sql.DB, sqsClient *sqs.Client, orgId int, logger logging.Logger) PackagesService {
	str := store.NewPostgresStoreFactory(db).WithLogging(logger)
	svc := newPackagesServiceWithFactory(str, orgId, logger)
	queueStore := store.NewQueueStore(sqsClient)
	return svc.withQueueStore(queueStore)
}

func (s *packagesService) RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest) (*models.RestoreResponse, error) {
	response := models.RestoreResponse{Success: []string{}, Failures: []models.Failure{}}
	err := s.SQLStoreFactory.ExecStoreTx(ctx, s.OrgId, func(store store.SQLStore) error {
		dataset, err := store.GetDatasetByNodeId(ctx, datasetId)
		datasetIntId := dataset.Id
		if err != nil {
			return err
		}
		var restoring []*pgdb.Package
		for _, nodeId := range request.NodeIds {
			if p, err := store.TransitionPackageState(ctx, datasetIntId, nodeId, packageState.Deleted, packageState.Restoring); err == nil {
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
		if len(restoring) == 0 {
			return nil
		}
		restoringIds := make([]int64, len(restoring))
		for i, r := range restoring {
			restoringIds[i] = r.Id
		}
		sizeById, err := store.GetPackageSizes(ctx, restoringIds...)
		if err != nil {
			return err
		}
		queueMessage := models.NewRestorePackageMessage(s.OrgId, datasetIntId, sizeById, restoring...)
		if err = s.QueueStore.SendRestorePackage(ctx, queueMessage); err != nil {
			// This will roll back Tx even though it's not a DB action.
			return err
		}
		return nil
	})
	return &response, err
}
