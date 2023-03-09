package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type configMockFunction func(*MockDatasetsStore) (*models.RestoreRequest, *models.RestoreResponse, error)

func TestTransitionPackageState(t *testing.T) {
	orgId := 7
	datasetNodeId := "N:dataset:9492034"
	datasetIntId := int64(13)
	for tName, configMock := range map[string]configMockFunction{
		"dataset not found error": func(store *MockDatasetsStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			err := models.DatasetNotFoundError{
				Id:    models.DatasetNodeId(datasetNodeId),
				OrgId: 7,
			}
			store.OnGetDatasetByNodeIdFail(datasetNodeId, err)
			return &models.RestoreRequest{NodeIds: []string{"N:package:1234", "N:package:0987"}}, nil, err
		},
		"unexpected get dataset error": func(store *MockDatasetsStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			err := errors.New("unexpected get dataset error")
			store.OnGetDatasetByNodeIdFail(datasetNodeId, err)
			return &models.RestoreRequest{NodeIds: []string{"N:package:1234", "N:package:0987"}}, nil, err
		},
		"package not found error": func(store *MockDatasetsStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			okIds := []string{"N:package:1234"}
			failedIds := map[string]error{"N:package:0987": models.PackageNotFoundError{DatasetId: models.DatasetNodeId(datasetNodeId), OrgId: orgId}}
			store.OnGetDatasetByNodeIdReturn(datasetNodeId, &pgdb.Dataset{Id: datasetIntId})
			var failures []models.Failure
			for id, err := range failedIds {
				if pErr, ok := err.(models.PackageNotFoundError); ok {
					pErr.Id = models.PackageNodeId(id)
				}
				store.OnTransitionPackageStateFail(datasetIntId, id, packageState.Deleted, packageState.Restoring, err)
				failures = append(failures, models.Failure{Id: id, Error: fmt.Sprintf("deleted package %s not found in dataset %s", id, datasetNodeId)})
			}
			store.OnTransitionPackageStateReturn(datasetIntId, okIds[0], packageState.Deleted, packageState.Restoring, &pgdb.Package{NodeId: okIds[0]})

			return &models.RestoreRequest{NodeIds: []string{"N:package:1234", "N:package:0987"}}, &models.RestoreResponse{Success: okIds, Failures: failures}, nil
		},
		"no errors": func(store *MockDatasetsStore) (*models.RestoreRequest, *models.RestoreResponse, error) {
			packageNodeIds := []string{"N:package:1234", "N:package:0987"}
			store.OnGetDatasetByNodeIdReturn(datasetNodeId, &pgdb.Dataset{Id: datasetIntId})
			for _, p := range packageNodeIds {
				store.OnTransitionPackageStateReturn(datasetIntId, p, packageState.Deleted, packageState.Restoring, &pgdb.Package{NodeId: p})
			}
			return &models.RestoreRequest{NodeIds: []string{"N:package:1234", "N:package:0987"}}, &models.RestoreResponse{Success: packageNodeIds}, nil
		},
	} {
		mockStore := new(MockDatasetsStore)
		request, expectedResponse, expectedError := configMock(mockStore)
		mockFactory := MockFactory{mockStore: mockStore}
		service := NewPackagesServiceWithFactory(&mockFactory, orgId)
		t.Run(tName, func(t *testing.T) {
			response, err := service.RestorePackages(context.Background(), datasetNodeId, request, false)
			mockStore.AssertExpectations(t)
			assert.Equal(t, orgId, mockFactory.orgId)
			if expectedError == nil {
				if assert.NoError(t, err) {
					assert.Equal(t, expectedResponse, response)
				}
			} else {
				if assert.Error(t, err) {
					assert.Equal(t, expectedError, err)
				}
			}
		})
	}
}

type MockReturn[T any] struct {
	Value T
	Error error
}

func (mr MockReturn[T]) ret() (T, error) {
	if err := mr.Error; err != nil {
		var r T
		return r, err
	}
	return mr.Value, nil
}

type MockDatasetsStore struct {
	mock.Mock
	GetDatasetByNodeIdReturn          MockReturn[*pgdb.Dataset]
	GetTrashcanRootPaginatedReturn    MockReturn[*store.PackagePage]
	GetTrashcanPaginatedReturn        MockReturn[*store.PackagePage]
	CountDatasetPackagesByStateReturn MockReturn[int]
	GetDatasetPackageByNodeIdReturn   MockReturn[*pgdb.Package]
	TransitionStateReturn             MockReturn[*pgdb.Package]
}

func (m *MockDatasetsStore) getExpectedErrors() []error {
	expected := make([]error, 5)
	var i int
	if err := m.GetDatasetByNodeIdReturn.Error; err != nil {
		expected[i] = err
		i++
	}
	if err := m.GetTrashcanRootPaginatedReturn.Error; err != nil {
		expected[i] = err
		i++
	}
	if err := m.GetTrashcanPaginatedReturn.Error; err != nil {
		expected[i] = err
		i++
	}
	if err := m.CountDatasetPackagesByStateReturn.Error; err != nil {
		expected[i] = err
		i++
	}
	if err := m.GetDatasetPackageByNodeIdReturn.Error; err != nil {
		expected[i] = err
		i++
	}
	if err := m.TransitionStateReturn.Error; err != nil {
		expected[i] = err
		i++
	}
	return expected[:i]
}

func (m *MockDatasetsStore) GetTrashcanRootPaginated(_ context.Context, _ int64, _ int, _ int) (*store.PackagePage, error) {
	return m.GetTrashcanRootPaginatedReturn.ret()
}

func (m *MockDatasetsStore) GetTrashcanPaginated(_ context.Context, _ int64, _ int64, _ int, _ int) (*store.PackagePage, error) {
	return m.GetTrashcanPaginatedReturn.ret()
}

func (m *MockDatasetsStore) GetDatasetByNodeId(ctx context.Context, nodeId string) (*pgdb.Dataset, error) {
	args := m.Called(ctx, nodeId)
	return args.Get(0).(*pgdb.Dataset), args.Error(1)
}

func (m *MockDatasetsStore) OnGetDatasetByNodeIdReturn(nodeId string, returned *pgdb.Dataset) {
	m.On("GetDatasetByNodeId", mock.Anything, nodeId).Return(returned, nil)
}
func (m *MockDatasetsStore) OnGetDatasetByNodeIdFail(nodeId string, returned error) {
	m.On("GetDatasetByNodeId", mock.Anything, nodeId).Return(&pgdb.Dataset{}, returned)
}

func (m *MockDatasetsStore) CountDatasetPackagesByState(_ context.Context, _ int64, _ packageState.State) (int, error) {
	return m.CountDatasetPackagesByStateReturn.ret()
}

func (m *MockDatasetsStore) GetDatasetPackageByNodeId(_ context.Context, _ int64, _ string) (*pgdb.Package, error) {
	return m.GetDatasetPackageByNodeIdReturn.ret()
}
func (m *MockDatasetsStore) TransitionPackageState(ctx context.Context, datasetId int64, packageId string, expectedState, targetState packageState.State) (*pgdb.Package, error) {
	args := m.Called(ctx, datasetId, packageId, expectedState, targetState)
	return args.Get(0).(*pgdb.Package), args.Error(1)
}

func (m *MockDatasetsStore) OnTransitionPackageStateReturn(datasetId int64, packageId string, expectedState, targetState packageState.State, returnedPackage *pgdb.Package) {
	m.On("TransitionPackageState", mock.Anything, datasetId, packageId, expectedState, targetState).Return(returnedPackage, nil)
}

func (m *MockDatasetsStore) OnTransitionPackageStateFail(datasetId int64, packageId string, expectedState, targetState packageState.State, returnedError error) {
	m.On("TransitionPackageState", mock.Anything, datasetId, packageId, expectedState, targetState).Return(&pgdb.Package{}, returnedError)
}

type MockFactory struct {
	mockStore *MockDatasetsStore
	orgId     int
}

func (m *MockFactory) NewSimpleStore(orgId int) store.DatasetsStore {
	m.orgId = orgId
	return m.mockStore
}

func (m *MockFactory) ExecStoreTx(_ context.Context, orgId int, fn func(store store.DatasetsStore) error) error {
	m.orgId = orgId
	return fn(m.mockStore)
}
