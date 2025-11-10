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
	case "/presign/s3":
		// Authenticated endpoint for generating presigned URLs
		s3PresignHandler := S3PresignHandler{RequestHandler: *h}
		return s3PresignHandler.handle(ctx)
	case "/proxy/s3":
		// Unauthenticated proxy endpoint that accepts presigned URLs
		s3ProxyHandler := S3ProxyHandler{RequestHandler: *h}
		return s3ProxyHandler.handle(ctx)
	case "/cloudfront/sign":
		// Authenticated endpoint for generating CloudFront signed URLs
		cloudfrontHandler := CloudFrontSignedURLHandler{RequestHandler: *h}
		return cloudfrontHandler.handle(ctx)
	default:
		return h.logAndBuildError("resource not found: "+h.path, http.StatusNotFound), nil
	}
}
