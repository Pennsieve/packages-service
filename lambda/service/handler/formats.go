package handler

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/service/internal/formats_registry"
)

type FormatsHandler struct {
	RequestHandler
}

func (h *FormatsHandler) handleGet(_ context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	return h.buildResponse(formatsregistry.All(), http.StatusOK)
}
