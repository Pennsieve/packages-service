package handler

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dataset"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/role"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"testing"
)

type queryParamMap map[string]string

// restoreRequestBody takes an array of package node ids and returns a pair consisting of
// a models.RestoreRequest with those node ids and the same request marshalled as a string.
func restoreRequestBody(t *testing.T, userId string, packageIds ...string) (models.RestoreRequest, string) {
	requestObject := models.RestoreRequest{
		NodeIds: packageIds,
		UserId:  userId,
	}
	requestBody, err := json.Marshal(requestObject)
	if err != nil {
		assert.FailNow(t, "could not marshall test request body: ", requestObject, err)
	}
	return requestObject, string(requestBody)
}

func TestRestoreRoute(t *testing.T) {
	expectedDatasetID := "N:Dataset:1234"
	expectedUserID := "N:user:101"
	expectedQueryParams := map[string]string{
		"dataset_id": expectedDatasetID,
	}
	requestObject, requestBody := restoreRequestBody(t, expectedUserID, "N:package:1234")
	req := newTestRequest("POST",
		"/packages/restore",
		"restorePackagesID",
		expectedQueryParams,
		requestBody)
	mockService := new(MockPackagesService)

	claims := authorizer.Claims{
		UserClaim: &user.Claim{
			Id:           101,
			NodeId:       expectedUserID,
			IsSuperAdmin: false,
		},
		DatasetClaim: &dataset.Claim{
			Role:   role.Editor,
			NodeId: expectedDatasetID,
			IntId:  1234,
		}}
	mockService.OnRestorePackagesReturn(expectedDatasetID, requestObject, &models.RestoreResponse{Success: []string{"N:package:1234"}})
	handler := NewHandler(req, &claims).WithService(mockService)
	_, err := handler.handle(context.Background())
	if assert.NoError(t, err) {
		mockService.AssertExpectations(t)
	}

}

func TestRestoreRouteUnauthorized(t *testing.T) {
	expectedDatasetID := "N:Dataset:1234"
	expectedUserID := "N:user:101"
	expectedQueryParams := map[string]string{
		"dataset_id": expectedDatasetID,
	}
	_, requestBody := restoreRequestBody(t, expectedUserID, "N:package:1234")
	req := newTestRequest("POST",
		"/packages/restore",
		"restorePackagesID",
		expectedQueryParams,
		requestBody)
	mockService := new(MockPackagesService)

	claims := authorizer.Claims{
		UserClaim: &user.Claim{
			Id:           101,
			NodeId:       expectedUserID,
			IsSuperAdmin: false,
		},
		DatasetClaim: &dataset.Claim{
			Role:   role.Viewer,
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
	userID := "N:user:101"
	datasetID := "N:Dataset:1234"
	packageID := "N:collection:abcd"
	requestObject, requestBody := restoreRequestBody(t, userID, packageID)
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
			UserClaim: &user.Claim{
				Id:           101,
				NodeId:       userID,
				IsSuperAdmin: false,
			},
			DatasetClaim: &dataset.Claim{
				Role:   role.Editor,
				NodeId: datasetID,
				IntId:  1234,
			}}
		if svcErr := tData.ServiceError; svcErr != nil {
			mockService.OnRestorePackagesFail(datasetID, requestObject, svcErr)
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

func (m *MockPackagesService) RestorePackages(ctx context.Context, datasetId string, request models.RestoreRequest) (*models.RestoreResponse, error) {
	args := m.Called(ctx, datasetId, request)
	return args.Get(0).(*models.RestoreResponse), args.Error(1)
}

// Type safe convenience methods for setting up expectations

func (m *MockPackagesService) OnRestorePackagesReturn(datasetId string, request models.RestoreRequest, returnedResponse *models.RestoreResponse) {
	m.On("RestorePackages", mock.Anything, datasetId, request).Return(returnedResponse, nil)
}

func (m *MockPackagesService) OnRestorePackagesFail(datasetId string, request models.RestoreRequest, returnedError error) {
	m.On("RestorePackages", mock.Anything, datasetId, request).Return(&models.RestoreResponse{}, returnedError)
}
