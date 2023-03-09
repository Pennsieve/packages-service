package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dataset"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"strconv"
	"testing"
)

type queryParamMap map[string]string

func (m queryParamMap) expectedInt(t *testing.T, key string, defaultValue int) int {
	expected := defaultValue
	if value, ok := m[key]; ok {
		var err error
		expected, err = strconv.Atoi(value)
		if err != nil {
			assert.FailNowf(t, "could not convert map value to int", "value [%s] at key [%s], error: %s", key, value, err)
		}
	}
	return expected
}

func (m queryParamMap) expectedLimit(t *testing.T) int {
	return m.expectedInt(t, "limit", DefaultLimit)
}

func (m queryParamMap) expectedOffset(t *testing.T) int {
	return m.expectedInt(t, "offset", DefaultOffset)
}

func TestTrashcanRoute(t *testing.T) {
	t.Skip("Need to update")
	expectedDatasetID := "N:Dataset:1234"
	for tName, expectedQueryParams := range map[string]queryParamMap{
		"without root_node_id param": {"dataset_id": expectedDatasetID},
		"with root_node_id param":    {"dataset_id": expectedDatasetID, "root_node_id": "N:collection:abcd"},
		"with limit param":           {"dataset_id": expectedDatasetID, "root_node_id": "N:collection:abcd", "limit": "30"},
		"with offset param":          {"dataset_id": expectedDatasetID, "offset": "10"},
	} {
		req := newTestRequest("GET",
			"/datasets/trashcan",
			"getTrashcanRequestID",
			expectedQueryParams,
			"")
		mockService := new(MockDatasetsService)

		claims := authorizer.Claims{
			DatasetClaim: dataset.Claim{
				Role:   dataset.Viewer,
				NodeId: expectedDatasetID,
				IntId:  1234,
			}}
		expectedLimit := expectedQueryParams.expectedLimit(t)
		expectedOffset := expectedQueryParams.expectedOffset(t)
		mockService.OnGetTrashcanPageReturn(expectedDatasetID, expectedQueryParams["root_node_id"], expectedLimit, expectedOffset, &models.TrashcanPage{})
		handler := NewHandler(req, &claims).WithService(mockService)
		t.Run(tName, func(t *testing.T) {
			_, err := handler.handle(context.Background())
			if assert.NoError(t, err) {
				mockService.AssertExpectations(t)
			}
		})

	}
}

func TestTrashcanRouteHandledErrors(t *testing.T) {
	t.Skip("Need to update")
	datasetID := "N:Dataset:1234"
	rootNodeID := "N:collection:abcd"
	for tName, tData := range map[string]struct {
		QueryParams         queryParamMap
		ServiceError        error
		ExpectedStatus      int
		ExpectedSubMessages []string
	}{
		"with too low limit": {
			QueryParams:         queryParamMap{"dataset_id": datasetID, "limit": "-1"},
			ExpectedStatus:      http.StatusBadRequest,
			ExpectedSubMessages: []string{"min value", "limit"}},
		"with too high limit": {
			QueryParams:         queryParamMap{"dataset_id": datasetID, "limit": "50000"},
			ExpectedStatus:      http.StatusBadRequest,
			ExpectedSubMessages: []string{"max value", "limit"}},
		"with too low offset": {
			QueryParams:         queryParamMap{"dataset_id": datasetID, "root_node_id": rootNodeID, "offset": "-4"},
			ExpectedStatus:      http.StatusBadRequest,
			ExpectedSubMessages: []string{"min value", "offset"}},
		"dataset not found": {
			QueryParams:         queryParamMap{"dataset_id": datasetID},
			ServiceError:        models.DatasetNotFoundError{Id: models.DatasetNodeId(datasetID)},
			ExpectedStatus:      http.StatusNotFound,
			ExpectedSubMessages: []string{"not found", datasetID},
		},
		"package not found": {
			QueryParams:         queryParamMap{"dataset_id": datasetID, "root_node_id": rootNodeID},
			ServiceError:        models.PackageNotFoundError{Id: models.PackageNodeId(rootNodeID), DatasetId: models.DatasetIntId(13)},
			ExpectedStatus:      http.StatusBadRequest,
			ExpectedSubMessages: []string{"not found", rootNodeID},
		},
		"package not a folder": {
			QueryParams:         queryParamMap{"dataset_id": datasetID, "root_node_id": rootNodeID},
			ServiceError:        models.FolderNotFoundError{NodeId: rootNodeID, ActualType: packageType.CSV},
			ExpectedStatus:      http.StatusBadRequest,
			ExpectedSubMessages: []string{"not found", rootNodeID, packageType.CSV.String()},
		},
	} {
		req := newTestRequest("GET",
			"/datasets/trashcan",
			"getTrashcanRequestID",
			tData.QueryParams,
			"")
		mockService := new(MockDatasetsService)
		claims := authorizer.Claims{
			DatasetClaim: dataset.Claim{
				Role:   dataset.Viewer,
				NodeId: datasetID,
				IntId:  1234,
			}}
		if tData.ServiceError != nil {
			mockService.OnGetTrashcanPageFail(tData.QueryParams["dataset_id"], tData.QueryParams["root_node_id"], tData.QueryParams.expectedLimit(t), tData.QueryParams.expectedOffset(t),
				tData.ServiceError)
		}
		handler := NewHandler(req, &claims).WithService(mockService)
		t.Run(tName, func(t *testing.T) {
			resp, err := handler.handle(context.Background())
			if assert.NoError(t, err) {
				mockService.AssertExpectations(t)
				assert.Equal(t, tData.ExpectedStatus, resp.StatusCode)
				for _, messageFragment := range tData.ExpectedSubMessages {
					assert.Contains(t, resp.Body, messageFragment)
				}
			}
		})

	}
}

func newTestRequest(method string, path string, requestID string, queryParams map[string]string, body string) *events.APIGatewayV2HTTPRequest {
	request := events.APIGatewayV2HTTPRequest{
		QueryStringParameters: queryParams,
		Body:                  body,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			RequestID: requestID,
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{
				Method: method,
				Path:   path,
			},
		},
	}
	return &request
}

type MockDatasetsService struct {
	mock.Mock
}

// Need to statisfy service.DatasetsService

func (m *MockDatasetsService) GetDataset(ctx context.Context, datasetId string) (*pgdb.Dataset, error) {
	args := m.Called(ctx, datasetId)
	return args.Get(0).(*pgdb.Dataset), args.Error(1)
}

func (m *MockDatasetsService) GetTrashcanPage(ctx context.Context, datasetID string, rootNodeId string, limit int, offset int) (*models.TrashcanPage, error) {
	args := m.Called(ctx, datasetID, rootNodeId, limit, offset)
	return args.Get(0).(*models.TrashcanPage), args.Error(1)
}

// Type safe convenience methods for setting up expectations

func (m *MockDatasetsService) OnGetTrashcanPageReturn(datasetID string, rootNodeId string, limit int, offset int, returnedPage *models.TrashcanPage) {
	m.On("GetTrashcanPage", mock.Anything, datasetID, rootNodeId, limit, offset).Return(returnedPage, nil)
}

func (m *MockDatasetsService) OnGetTrashcanPageFail(datasetID string, rootNodeId string, limit int, offset int, returnedError error) {
	m.On("GetTrashcanPage", mock.Anything, datasetID, rootNodeId, limit, offset).Return(&models.TrashcanPage{}, returnedError)
}

func (m *MockDatasetsService) OnGetDatasetReturn(datasetId string, returnedDataset *pgdb.Dataset) {
	m.On("GetDataset", mock.Anything, datasetId).Return(returnedDataset, nil)
}
func (m *MockDatasetsService) OnGetDatasetFail(datasetId string, returnedError error) {
	m.On("GetDataset", mock.Anything, datasetId).Return(&pgdb.Dataset{}, returnedError)
}
