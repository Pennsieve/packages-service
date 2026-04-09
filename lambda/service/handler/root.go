package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
)

func (h *RequestHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {

	switch h.path {
	case "/restore":
		restoreHandler := RestoreHandler{RequestHandler: *h}
		return restoreHandler.handle(ctx)
	case "/cloudfront/sign":
		// Authenticated endpoint for generating CloudFront signed URLs
		cloudfrontHandler := CloudFrontSignedURLHandler{RequestHandler: *h}
		return cloudfrontHandler.handle(ctx)
	case "/download-manifest":
		downloadHandler := DownloadManifestHandler{RequestHandler: *h}
		return downloadHandler.handle(ctx)
	default:
		if h.path == "/assets" || strings.HasPrefix(h.path, "/assets/") {
			assetsHandler := ViewerAssetsHandler{RequestHandler: *h}
			return assetsHandler.handle(ctx)
		}
		return h.logAndBuildError("resource not found: "+h.path, http.StatusNotFound), nil
	}
}
