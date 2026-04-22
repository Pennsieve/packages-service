package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
)

func (h *RequestHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {

	switch h.path {
	case "/restore":
		restoreHandler := RestoreHandler{RequestHandler: *h}
		return restoreHandler.handle(ctx)
	case "/download-manifest":
		downloadHandler := DownloadManifestHandler{RequestHandler: *h}
		return downloadHandler.handle(ctx)
	case "/formats":
		formatsHandler := FormatsHandler{RequestHandler: *h}
		switch h.method {
		case http.MethodGet:
			return formatsHandler.handleGet(ctx)
		default:
			return h.logAndBuildError(fmt.Sprintf("method %s not allowed on /formats", h.method), http.StatusMethodNotAllowed), nil
		}
	case "/assets":
		assetsHandler := ViewerAssetsHandler{RequestHandler: *h}
		switch h.method {
		case http.MethodPost:
			return assetsHandler.handleCreate(ctx)
		case http.MethodGet:
			return assetsHandler.handleList(ctx)
		default:
			return h.logAndBuildError(fmt.Sprintf("method %s not allowed on /assets", h.method), http.StatusMethodNotAllowed), nil
		}
	default:
		if strings.HasPrefix(h.path, "/assets/") {
			assetID := strings.TrimPrefix(h.path, "/assets/")
			assetsHandler := ViewerAssetsHandler{RequestHandler: *h}
			switch h.method {
			case http.MethodPatch:
				return assetsHandler.handleUpdate(ctx, assetID)
			case http.MethodDelete:
				return assetsHandler.handleDelete(ctx, assetID)
			default:
				return h.logAndBuildError(fmt.Sprintf("method %s not allowed on /assets/{id}", h.method), http.StatusMethodNotAllowed), nil
			}
		}
		return h.logAndBuildError("resource not found: "+h.path, http.StatusNotFound), nil
	}
}
