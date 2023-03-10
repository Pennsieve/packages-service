package handler

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dataset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"testing"
)

type queryParamMap map[string]string

// restoreRequestBody takes an array of package node ids and returns a pair consisting of
// a models.RestoreRequest with those node ids and the same request marshalled as a string.
func restoreRequestBody(t *testing.T, packageIds ...string) (models.RestoreRequest, string) {
	requestObject := models.RestoreRequest{
		NodeIds: packageIds,
	}
	requestBody, err := json.Marshal(requestObject)
	if err != nil {
		assert.FailNow(t, "could not marshall test request body: ", requestObject, err)
	}
	return requestObject, string(requestBody)
}

func TestRestoreRoute(t *testing.T) {
	expectedDatasetID := "N:Dataset:1234"
	expectedQueryParams := map[string]string{
		"dataset_id": expectedDatasetID,
	}
	requestObject, requestBody := restoreRequestBody(t, "N:package:1234")
	req := newTestRequest("POST",
		"/packages/restore",
		"restorePackagesID",
		expectedQueryParams,
		requestBody)
	mockService := new(MockPackagesService)

	claims := authorizer.Claims{
		DatasetClaim: dataset.Claim{
			Role:   dataset.Editor,
			NodeId: expectedDatasetID,
			IntId:  1234,
		}}
	mockService.OnRestorePackagesReturn(expectedDatasetID, requestObject, true, &models.RestoreResponse{Success: []string{"N:package:1234"}})
	handler := NewHandler(req, &claims).WithService(mockService)
	_, err := handler.handle(context.Background())
	if assert.NoError(t, err) {
		mockService.AssertExpectations(t)
	}

}

func TestRestoreRouteUnauthorized(t *testing.T) {
	expectedDatasetID := "N:Dataset:1234"
	expectedQueryParams := map[string]string{
		"dataset_id": expectedDatasetID,
	}
	_, requestBody := restoreRequestBody(t, "N:package:1234")
	req := newTestRequest("POST",
		"/packages/restore",
		"restorePackagesID",
		expectedQueryParams,
		requestBody)
	mockService := new(MockPackagesService)

	claims := authorizer.Claims{
		DatasetClaim: dataset.Claim{
			Role:   dataset.Viewer,
			NodeId: expectedDatasetID,
			IntId:  1234,
		}}
	handler := NewHandler(req, &claims).WithService(mockService)
	resp, err := handler.handle(context.Background())
	if assert.NoError(t, err) {
		mockService.AssertExpectations(t)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	}

}

func TestTrashcanRouteHandledErrors(t *testing.T) {
	datasetID := "N:Dataset:1234"
	packageID := "N:collection:abcd"
	requestObject, requestBody := restoreRequestBody(t, packageID)
	for tName, tData := range map[string]struct {
		ServiceError        error
		ExpectedStatus      int
		ExpectedSubMessages []string
	}{
		"dataset not found": {
			ServiceError:        models.DatasetNotFoundError{Id: models.DatasetNodeId(datasetID)},
			ExpectedStatus:      http.StatusNotFound,
			ExpectedSubMessages: []string{"not found", datasetID},
		},
	} {
		req := newTestRequest("POST",
			"/packages/restore",
			"restorePackagesRequestID",
			queryParamMap{"dataset_id": datasetID},
			requestBody)
		mockService := new(MockPackagesService)
		claims := authorizer.Claims{
			DatasetClaim: dataset.Claim{
				Role:   dataset.Editor,
				NodeId: datasetID,
				IntId:  1234,
			}}
		if svcErr := tData.ServiceError; svcErr != nil {
			mockService.OnRestorePackagesFail(datasetID, requestObject, true, svcErr)
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

type MockPackagesService struct {
	mock.Mock
}

// Need to statisfy service.PackagesService

func (m *MockPackagesService) RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest, undo bool) (*models.RestoreResponse, error) {
	args := m.Called(ctx, datasetId, request, undo)
	return args.Get(0).(*models.RestoreResponse), args.Error(1)
}

// Type safe convenience methods for setting up expectations

func (m *MockPackagesService) OnRestorePackagesReturn(datasetId string, request models.RestoreRequest, undo bool, returnedResponse *models.RestoreResponse) {
	m.On("RestorePackages", mock.Anything, datasetId, request, undo).Return(returnedResponse, nil)
}

func (m *MockPackagesService) OnRestorePackagesFail(datasetId string, request models.RestoreRequest, undo bool, returnedError error) {
	m.On("RestorePackages", mock.Anything, datasetId, request, undo).Return(&models.RestoreResponse{}, returnedError)
}
