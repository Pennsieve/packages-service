package handler

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/permissions"
	"net/http"
)

type RestoreHandler struct {
	RequestHandler
}

func (h *RestoreHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	switch h.method {
	case "POST":
		return h.post(ctx)
	default:
		return h.logAndBuildError("method not allowed: "+h.method, http.StatusMethodNotAllowed), nil
	}

}

func (h *RestoreHandler) post(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	if authorized := authorizer.HasRole(*h.claims, permissions.CreateDeleteFiles); !authorized {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}
	datasetId, ok := h.request.QueryStringParameters["dataset_id"]
	if !ok {
		return h.logAndBuildError("query param 'dataset_id' is required", http.StatusBadRequest), nil
	}
	var request models.RestoreRequest
	if err := json.Unmarshal([]byte(h.body), &request); err != nil {
		return h.logAndBuildErrorCause("unable to unmarshal request body as RestoreRequest", http.StatusBadRequest, err), nil
	}
	request.UserId = h.claims.UserClaim.NodeId
	response, err := h.packagesService.RestorePackages(ctx, datasetId, request)
	if err == nil {
		h.logger.LogInfo("returning OK")
		return h.buildResponse(response, http.StatusOK)
	}
	switch err.(type) {
	case models.DatasetNotFoundError:
		return h.logAndBuildError(err.Error(), http.StatusNotFound), nil
	default:
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:         err,
			logging.KeyDatasetNodeID: datasetId,
		}, "restore packages failed")
		return nil, err
	}
}
