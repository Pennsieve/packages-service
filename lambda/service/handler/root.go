package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"net/http"
)

func (h *RequestHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {

	switch h.path {
	case "/restore":
		restoreHandler := RestoreHandler{RequestHandler: *h}
		return restoreHandler.handle(ctx)
	default:
		return h.logAndBuildError("resource not found: "+h.path, http.StatusNotFound), nil
	}
}
