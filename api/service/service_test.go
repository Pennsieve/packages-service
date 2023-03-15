package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type configMockFunction func(*MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error)

func TestTransitionPackageState(t *testing.T) {
	orgId := 7
	datasetNodeId := "N:dataset:9492034"
	datasetIntId := int64(13)
	for tName, configMock := range map[string]configMockFunction{
		"dataset not found error": func(store *MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			err := models.DatasetNotFoundError{
				Id:    models.DatasetNodeId(datasetNodeId),
				OrgId: 7,
			}
			store.OnGetDatasetByNodeIdFail(datasetNodeId, err)
			return &models.RestoreRequest{NodeIds: []string{"N:package:1234", "N:package:0987"}}, nil, err
		},
		"unexpected get dataset error": func(store *MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			err := errors.New("unexpected get dataset error")
			store.OnGetDatasetByNodeIdFail(datasetNodeId, err)
			return &models.RestoreRequest{NodeIds: []string{"N:package:1234", "N:package:0987"}}, nil, err
		},
		"package not found error": func(store *MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			store.OnGetDatasetByNodeIdReturn(datasetNodeId, &pgdb.Dataset{Id: datasetIntId})

			okIds := []string{"N:package:1234"}
			okPkgs := make([]*pgdb.Package, len(okIds))
			for i, okId := range okIds {
				okPkg := newDeletedPackage(okId, fmt.Sprintf("file_%d.txt", i), packageType.Text)
				store.OnTransitionPackageStateReturn(datasetIntId, okPkg.NodeId, packageState.Deleted, packageState.Restoring, okPkg)
				okPkgs[i] = okPkg
			}
			sqsMessage := models.NewRestorePackageMessage(orgId, datasetIntId, okPkgs...)
			store.OnSendRestorePackageReturn(sqsMessage)

			failedIdErrors := map[string]error{"N:package:0987": models.PackageNotFoundError{DatasetId: models.DatasetNodeId(datasetNodeId), OrgId: orgId}}
			var failedIds []string
			var failures []models.Failure
			for id, err := range failedIdErrors {
				failedIds = append(failedIds, id)
				if pErr, ok := err.(models.PackageNotFoundError); ok {
					pErr.Id = models.PackageNodeId(id)
				}
				store.OnTransitionPackageStateFail(datasetIntId, id, packageState.Deleted, packageState.Restoring, err)
				failures = append(failures, models.Failure{Id: id, Error: fmt.Sprintf("deleted package %s not found in dataset %s", id, datasetNodeId)})
			}

			// Not treating package not found from state transition as an error.
			return &models.RestoreRequest{NodeIds: append(okIds, failedIds...)}, &models.RestoreResponse{Success: okIds, Failures: failures}, nil
		},
		"no packages found": func(store *MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			store.OnGetDatasetByNodeIdReturn(datasetNodeId, &pgdb.Dataset{Id: datasetIntId})

			failedIdErrors := map[string]error{"N:package:0987": models.PackageNotFoundError{DatasetId: models.DatasetNodeId(datasetNodeId), OrgId: orgId}}
			var failedIds []string
			var failures []models.Failure
			for id, err := range failedIdErrors {
				failedIds = append(failedIds, id)
				if pErr, ok := err.(models.PackageNotFoundError); ok {
					pErr.Id = models.PackageNodeId(id)
				}
				store.OnTransitionPackageStateFail(datasetIntId, id, packageState.Deleted, packageState.Restoring, err)
				failures = append(failures, models.Failure{Id: id, Error: fmt.Sprintf("deleted package %s not found in dataset %s", id, datasetNodeId)})
			}

			// Not treating package not found from state transition as an error.
			return &models.RestoreRequest{NodeIds: failedIds}, &models.RestoreResponse{Success: []string{}, Failures: failures}, nil
		},
		"unexpected package state transition error": func(store *MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			store.OnGetDatasetByNodeIdReturn(datasetNodeId, &pgdb.Dataset{Id: datasetIntId})

			okIds := []string{"N:package:1234"}
			for i, okId := range okIds {
				okPkg := newDeletedPackage(okId, fmt.Sprintf("file_%d.txt", i), packageType.Text)
				store.OnTransitionPackageStateReturn(datasetIntId, okPkg.NodeId, packageState.Deleted, packageState.Restoring, okPkg)
			}

			expectedError := errors.New("unexpected package state transition")
			failedIdErrors := map[string]error{"N:package:0987": expectedError}
			var failedIds []string
			for id, err := range failedIdErrors {
				failedIds = append(failedIds, id)
				if pErr, ok := err.(models.PackageNotFoundError); ok {
					pErr.Id = models.PackageNodeId(id)
				}
				store.OnTransitionPackageStateFail(datasetIntId, id, packageState.Deleted, packageState.Restoring, err)
			}

			return &models.RestoreRequest{NodeIds: append(okIds, failedIds...)}, nil, expectedError
		},
		"unexpected sqs send error": func(store *MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			store.OnGetDatasetByNodeIdReturn(datasetNodeId, &pgdb.Dataset{Id: datasetIntId})

			okIds := []string{"N:package:1234", "N:package:0987"}
			okPkgs := make([]*pgdb.Package, len(okIds))
			for i, okId := range okIds {
				okPkg := newDeletedPackage(okId, fmt.Sprintf("file_%d.txt", i), packageType.Text)
				store.OnTransitionPackageStateReturn(datasetIntId, okPkg.NodeId, packageState.Deleted, packageState.Restoring, okPkg)
				okPkgs[i] = okPkg
			}

			sqsError := errors.New("unexpected sqs send error")
			message := models.NewRestorePackageMessage(orgId, datasetIntId, okPkgs...)
			store.OnSendRestorePackageFail(message, sqsError)
			return &models.RestoreRequest{NodeIds: okIds}, nil, sqsError
		},
		"no errors": func(store *MockPackagesStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			store.OnGetDatasetByNodeIdReturn(datasetNodeId, &pgdb.Dataset{Id: datasetIntId})

			okIds := []string{"N:package:1234", "N:package:0987"}
			okPkgs := make([]*pgdb.Package, len(okIds))
			for i, okId := range okIds {
				okPkg := newDeletedPackage(okId, fmt.Sprintf("file_%d.txt", i), packageType.Text)
				store.OnTransitionPackageStateReturn(datasetIntId, okPkg.NodeId, packageState.Deleted, packageState.Restoring, okPkg)
				okPkgs[i] = okPkg
			}

			message := models.NewRestorePackageMessage(orgId, datasetIntId, okPkgs...)
			store.OnSendRestorePackageReturn(message)

			return &models.RestoreRequest{NodeIds: []string{"N:package:1234", "N:package:0987"}}, &models.RestoreResponse{Success: okIds, Failures: []models.Failure{}}, nil
		},
	} {
		mockStore := new(MockPackagesStore)
		request, expectedResponse, expectedError := configMock(mockStore)
		mockFactory := MockFactory{mockStore: mockStore}
		service := newPackagesServiceWithFactory(&mockFactory, orgId).withQueueStore(mockStore)
		t.Run(tName, func(t *testing.T) {
			response, err := service.RestorePackages(context.Background(), datasetNodeId, *request, false)
			if mockStore.AssertExpectations(t) {
				assert.Equal(t, orgId, mockFactory.orgId)
				assert.Equal(t, expectedError, mockFactory.txError)
				if expectedError == nil {
					if assert.NoError(t, err) {
						assert.Equal(t, expectedResponse, response)
					}
				} else {
					if assert.Error(t, err) {
						assert.Equal(t, expectedError, err)
					}
				}
			}
		})
	}
}

// Use this to mock both store.SQLStore and store.QueueStore for convenience.
type MockPackagesStore struct {
	mock.Mock
}

func (m *MockPackagesStore) SendRestorePackage(ctx context.Context, restoreMessage models.RestorePackageMessage) error {
	args := m.Called(ctx, restoreMessage)
	return args.Error(0)
}

func (m *MockPackagesStore) OnSendRestorePackageReturn(restoreMessage models.RestorePackageMessage) {
	m.On("SendRestorePackage", mock.Anything, restoreMessage).Return(nil)
}

func (m *MockPackagesStore) OnSendRestorePackageFail(restoreMessage models.RestorePackageMessage, returnedError error) {
	m.On("SendRestorePackage", mock.Anything, restoreMessage).Return(returnedError)
}

func (m *MockPackagesStore) GetDatasetByNodeId(ctx context.Context, nodeId string) (*pgdb.Dataset, error) {
	args := m.Called(ctx, nodeId)
	return args.Get(0).(*pgdb.Dataset), args.Error(1)
}

func (m *MockPackagesStore) OnGetDatasetByNodeIdReturn(nodeId string, returned *pgdb.Dataset) {
	m.On("GetDatasetByNodeId", mock.Anything, nodeId).Return(returned, nil)
}

func (m *MockPackagesStore) OnGetDatasetByNodeIdFail(nodeId string, returned error) {
	m.On("GetDatasetByNodeId", mock.Anything, nodeId).Return(&pgdb.Dataset{}, returned)
}

func (m *MockPackagesStore) TransitionPackageState(ctx context.Context, datasetId int64, packageId string, expectedState, targetState packageState.State) (*pgdb.Package, error) {
	args := m.Called(ctx, datasetId, packageId, expectedState, targetState)
	return args.Get(0).(*pgdb.Package), args.Error(1)
}

func (m *MockPackagesStore) OnTransitionPackageStateReturn(datasetId int64, packageId string, expectedState, targetState packageState.State, returnedPackage *pgdb.Package) {
	m.On("TransitionPackageState", mock.Anything, datasetId, packageId, expectedState, targetState).Return(returnedPackage, nil)
}

func (m *MockPackagesStore) OnTransitionPackageStateFail(datasetId int64, packageId string, expectedState, targetState packageState.State, returnedError error) {
	m.On("TransitionPackageState", mock.Anything, datasetId, packageId, expectedState, targetState).Return(&pgdb.Package{}, returnedError)
}

type MockFactory struct {
	mockStore *MockPackagesStore
	orgId     int
	txError   error
}

func (m *MockFactory) NewSimpleStore(orgId int) store.SQLStore {
	m.orgId = orgId
	return m.mockStore
}

func (m *MockFactory) ExecStoreTx(_ context.Context, orgId int, fn func(store store.SQLStore) error) error {
	m.orgId = orgId
	m.txError = fn(m.mockStore)
	return m.txError
}

func newDeletedPackage(nodeId, origName string, packageType packageType.Type) *pgdb.Package {
	return &pgdb.Package{
		NodeId:      nodeId,
		PackageType: packageType,
		Name:        fmt.Sprintf("__%s__%s_%s", packageState.Deleted, nodeId, origName),
	}
}
