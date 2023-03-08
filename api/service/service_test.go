package service

import (
	"context"
	"errors"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTrashcanPageErrors(t *testing.T) {
	orgId := 7
	for tName, expected := range map[string]struct {
		rootNodeId string
		mockStore  MockDatasetsStore
	}{
		"dataset not found error": {"N:collection:8700", MockDatasetsStore{
			GetDatasetByNodeIdReturn: MockReturn[*pgdb.Dataset]{Error: models.DatasetNotFoundError{OrgId: orgId, Id: models.DatasetNodeId("N:dataset:9492034")}}}},
		"unexpected get dataset error": {"N:collection:8700", MockDatasetsStore{
			GetDatasetByNodeIdReturn: MockReturn[*pgdb.Dataset]{Error: errors.New("unexpected get dataset error")}}},
		"unexpected count deleted error": {"N:collection:8700", MockDatasetsStore{
			GetDatasetByNodeIdReturn:          MockReturn[*pgdb.Dataset]{Value: &pgdb.Dataset{Id: 13}},
			CountDatasetPackagesByStateReturn: MockReturn[int]{Error: errors.New("unexpected count dataset error")},
		}},
		"package not found error": {"N:collection:5790", MockDatasetsStore{
			GetDatasetByNodeIdReturn:          MockReturn[*pgdb.Dataset]{Value: &pgdb.Dataset{Id: 13}},
			CountDatasetPackagesByStateReturn: MockReturn[int]{Value: 6},
			GetDatasetPackageByNodeIdReturn:   MockReturn[*pgdb.Package]{Error: models.PackageNotFoundError{OrgId: orgId, Id: models.PackageNodeId("N:package:bad-999"), DatasetId: models.DatasetIntId(13)}},
		}},
		"unexpected trashcan error": {"N:collection:5790", MockDatasetsStore{
			GetDatasetByNodeIdReturn:          MockReturn[*pgdb.Dataset]{Value: &pgdb.Dataset{Id: 13}},
			CountDatasetPackagesByStateReturn: MockReturn[int]{Value: 6},
			GetDatasetPackageByNodeIdReturn:   MockReturn[*pgdb.Package]{Value: &pgdb.Package{Id: 57, PackageType: packageType.Collection}},
			GetTrashcanPaginatedReturn:        MockReturn[*store.PackagePage]{Error: errors.New("unexpected error")},
		}},
		"unexpected root trashcan error": {"", MockDatasetsStore{
			GetDatasetByNodeIdReturn:          MockReturn[*pgdb.Dataset]{Value: &pgdb.Dataset{Id: 13}},
			CountDatasetPackagesByStateReturn: MockReturn[int]{Value: 6},
			GetTrashcanRootPaginatedReturn:    MockReturn[*store.PackagePage]{Error: errors.New("unexpected root error")},
		}},
	} {
		mockFactory := MockFactory{&expected.mockStore, -1}
		service := NewDatasetsServiceWithFactory(&mockFactory, orgId)
		t.Run(tName, func(t *testing.T) {
			_, err := service.GetTrashcanPage(context.Background(), "N:dataset:7890", expected.rootNodeId, 10, 0)
			if assert.Error(t, err) {
				assert.Equal(t, expected.mockStore.getExpectedErrors(), []error{err})
				assert.Equal(t, orgId, mockFactory.orgId)
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
	GetDatasetByNodeIdReturn          MockReturn[*pgdb.Dataset]
	GetTrashcanRootPaginatedReturn    MockReturn[*store.PackagePage]
	GetTrashcanPaginatedReturn        MockReturn[*store.PackagePage]
	CountDatasetPackagesByStateReturn MockReturn[int]
	GetDatasetPackageByNodeIdReturn   MockReturn[*pgdb.Package]
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
	return expected[:i]
}

func (m *MockDatasetsStore) GetTrashcanRootPaginated(_ context.Context, _ int64, _ int, _ int) (*store.PackagePage, error) {
	return m.GetTrashcanRootPaginatedReturn.ret()
}

func (m *MockDatasetsStore) GetTrashcanPaginated(_ context.Context, _ int64, _ int64, _ int, _ int) (*store.PackagePage, error) {
	return m.GetTrashcanPaginatedReturn.ret()
}

func (m *MockDatasetsStore) GetDatasetByNodeId(_ context.Context, _ string) (*pgdb.Dataset, error) {
	return m.GetDatasetByNodeIdReturn.ret()
}

func (m *MockDatasetsStore) CountDatasetPackagesByState(_ context.Context, _ int64, _ packageState.State) (int, error) {
	return m.CountDatasetPackagesByStateReturn.ret()
}

func (m *MockDatasetsStore) GetDatasetPackageByNodeId(_ context.Context, _ int64, _ string) (*pgdb.Package, error) {
	return m.GetDatasetPackageByNodeIdReturn.ret()
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
