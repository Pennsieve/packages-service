package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/service"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"os"
	"strconv"
)

var PennsieveDB *sql.DB
var DiscoverDB *sql.DB
var SQSClient *sqs.Client
var S3Client *s3.Client
var AssumeRoleClient stscreds.AssumeRoleAPIClient
var ViewerAssetsBucket string

func init() {
	logging.SetDefaultFromEnv()

	// Initialize ViewerAssetsBucket from environment variable
	if bucket, ok := os.LookupEnv("VIEWER_ASSETS_BUCKET"); ok {
		ViewerAssetsBucket = bucket
		slog.Info("ViewerAssetsBucket initialized", slog.String(logging.KeyS3Bucket, ViewerAssetsBucket))
	} else {
		slog.Warn("VIEWER_ASSETS_BUCKET environment variable not set")
	}

}

func PackagesServiceHandler(ctx context.Context, request events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {
	path := request.RequestContext.HTTP.Path

	// Discover endpoints are unauthenticated — skip claim parsing
	if strings.HasPrefix(path, "/discover/") {
		handler := NewDiscoverHandlerWithContext(ctx, &request)
		return handler.handleDiscover(ctx)
	}

	// For authenticated endpoints, parse claims and create service
	claims := authorizer.ParseClaims(request.RequestContext.Authorizer.Lambda)
	handler := NewHandlerWithContext(ctx, &request, claims).WithDefaultService()
	return handler.handle(ctx)
}

// inboundTraceHeaders are the headers, in precedence order, that a caller may
// use to hand us an existing correlation id. If any is present we adopt it, so
// that a logical operation keeps one id across service boundaries; otherwise we
// mint a fresh one.
var inboundTraceHeaders = []string{"x-request-id", "x-correlation-id", "traceparent", "x-amzn-trace-id"}

// resolveTraceID returns this service's internal correlation id for one logical
// operation, and whether it was inherited from the caller.
//
// Deliberately NOT an AWS-issued id. API Gateway's RequestContext.RequestID,
// SQS's MessageId and Lambda's AwsRequestID are each minted fresh at their own
// hop, so none of them can follow an operation across service boundaries. Those
// per-hop ids are still logged, each under its own key, alongside this one.
func resolveTraceID(headers map[string]string) (traceID string, inherited bool) {
	for _, h := range inboundTraceHeaders {
		// API Gateway v2 lower-cases header names, but check both to be safe.
		if v, ok := headers[h]; ok && v != "" {
			return v, true
		}
		if v, ok := headers[http.CanonicalHeaderKey(h)]; ok && v != "" {
			return v, true
		}
	}
	return uuid.NewString(), false
}

// awsRequestID returns the Lambda invocation id, if the context carries one.
func awsRequestID(ctx context.Context) string {
	if lc, ok := lambdacontext.FromContext(ctx); ok && lc != nil {
		return lc.AwsRequestID
	}
	return ""
}

// RequestHandler wraps the incoming request with a logger and a service.PackagesService.
// Some request params are pulled out for convenience. Use NewHandler followed by WithDefaultService to have things
// initialized nicely. Use WithService in tests where a specially constructed or mock service.PackagesService is required.
type RequestHandler struct {
	request *events.APIGatewayV2HTTPRequest
	// apiGatewayRequestID is API Gateway's per-hop request id. Kept (and now
	// named unambiguously) for correlating with the API Gateway access log; it
	// is not the cross-service trace id — see traceID.
	apiGatewayRequestID string
	// traceID is this service's internal correlation id for the logical
	// operation. Inherited from an inbound header when the caller supplied one.
	traceID string

	method      string
	path        string
	queryParams map[string]string
	body        string

	logger          *logging.Log
	packagesService service.PackagesService
	claims          *authorizer.Claims
}

// newRequestHandler builds the request-scoped logger shared by the
// authenticated and unauthenticated (discover) entrypoints, attaching both the
// per-hop AWS ids and this service's own trace id so every downstream log line
// carries all of them.
func newRequestHandler(ctx context.Context, request *events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*RequestHandler, bool) {
	apiGatewayReqID := request.RequestContext.RequestID
	traceID, inherited := resolveTraceID(request.Headers)

	fields := logging.Fields{
		logging.KeyTraceID:             traceID,
		logging.KeyAPIGatewayRequestID: apiGatewayReqID,
	}
	if awsReqID := awsRequestID(ctx); awsReqID != "" {
		fields[logging.KeyAWSRequestID] = awsReqID
	}

	return &RequestHandler{
		request:             request,
		apiGatewayRequestID: apiGatewayReqID,
		traceID:             traceID,

		method:      request.RequestContext.HTTP.Method,
		path:        request.RequestContext.HTTP.Path,
		queryParams: request.QueryStringParameters,
		body:        request.Body,

		logger: logging.NewLogWithFields(fields),
		claims: claims,
	}, inherited
}

// NewHandler creates a RequestHandler that has its logger field initialized with useful fields.
func NewHandler(request *events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) *RequestHandler {
	return NewHandlerWithContext(context.Background(), request, claims)
}

// NewHandlerWithContext is NewHandler with the invocation context available, so
// the Lambda request id can be picked up from lambdacontext.
func NewHandlerWithContext(ctx context.Context, request *events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) *RequestHandler {
	requestHandler, inheritedTrace := newRequestHandler(ctx, request, claims)

	requestHandler.logger.LogInfoWithFields(logging.Fields{
		logging.KeyMethod:      requestHandler.method,
		logging.KeyPath:        requestHandler.path,
		logging.KeyQueryParams: requestHandler.queryParams,
		logging.KeyRequestBody: requestHandler.body,
		logging.KeyClaims:      requestHandler.claims,
		"traceIdInherited":     inheritedTrace,
	}, "creating RequestHandler")

	return requestHandler
}

// WithDefaultService adds a new service.PackagesService to the RequestHandler that
// has been initialized to use PennsieveDB as the SQL database pointed to the
// workspace in the RequestHandler's OrgClaim.
func (h *RequestHandler) WithDefaultService() *RequestHandler {
	svc := service.NewPackagesService(PennsieveDB, SQSClient, int(h.claims.OrgClaim.IntId), h.logger)
	h.packagesService = svc
	return h
}

// WithService simply attaches the passed in service.PackagesService to the RequestHandler. Used for
// tests that do not need to use PennsieveDB.
func (h *RequestHandler) WithService(service service.PackagesService) *RequestHandler {
	h.packagesService = service
	return h
}

// logAndBuildError logs a static message and renders the client error body.
// The trace id (not the per-hop API Gateway id) is what goes in the response,
// since that is the id a caller can quote to find the whole operation in the
// logs — including any hop this service made downstream.
func (h *RequestHandler) logAndBuildError(message string, status int) *events.APIGatewayV2HTTPResponse {
	return h.logAndBuildErrorWithFields(message, status, nil)
}

// logAndBuildErrorCause is logAndBuildError for the common case of "static
// message + the error that caused it". The error becomes a structured field
// rather than being interpolated into the message, so that the message stays
// groupable in DataDog while the detail is still queryable.
func (h *RequestHandler) logAndBuildErrorCause(message string, status int, err error) *events.APIGatewayV2HTTPResponse {
	return h.logAndBuildErrorWithFields(message, status, logging.Fields{logging.KeyError: err})
}

func (h *RequestHandler) logAndBuildErrorWithFields(message string, status int, fields logging.Fields) *events.APIGatewayV2HTTPResponse {
	if fields == nil {
		fields = logging.Fields{}
	}
	fields[logging.KeyStatusCode] = status
	h.logger.LogErrorWithFields(fields, message)
	// requestID is retained verbatim so the client-visible error contract does
	// not change; traceId is added because that is the id that follows the
	// operation across hops.
	errorBody := fmt.Sprintf("{'message': '%s (requestID: %s, traceId: %s)'}", message, h.apiGatewayRequestID, h.traceID)
	return buildResponseFromString(errorBody, status)
}

func (h *RequestHandler) queryParamAsInt(paramName string, minValue, maxValue, defaultValue int) (int, error) {
	strValue, ok := h.request.QueryStringParameters[paramName]
	if !ok {
		return defaultValue, nil
	}
	v, err := strconv.Atoi(strValue)
	if err != nil {
		return 0, err
	}
	if v < minValue {
		return 0, fmt.Errorf("%d is less than min value %d for %q", v, minValue, paramName)
	}
	if v > maxValue {
		return 0, fmt.Errorf("%d is more than max value %d for %q", v, maxValue, paramName)
	}
	return v, nil
}

func (h *RequestHandler) buildResponse(body any, status int) (*events.APIGatewayV2HTTPResponse, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:       err,
			logging.KeyRequestBody: fmt.Sprintf("%v", body),
		}, "error marshalling response body")
		return nil, err
	}
	return buildResponseFromString(string(bodyBytes), status), nil
}

func buildResponseFromString(body string, status int) *events.APIGatewayV2HTTPResponse {
	response := events.APIGatewayV2HTTPResponse{
		Body:       body,
		StatusCode: status,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}
	return &response
}

// NewDiscoverHandler creates a RequestHandler for unauthenticated discover endpoints.
// No claims are parsed since these routes don't require authentication.
func NewDiscoverHandler(request *events.APIGatewayV2HTTPRequest) *RequestHandler {
	return NewDiscoverHandlerWithContext(context.Background(), request)
}

// NewDiscoverHandlerWithContext is NewDiscoverHandler with the invocation
// context available, so the Lambda request id can be picked up.
func NewDiscoverHandlerWithContext(ctx context.Context, request *events.APIGatewayV2HTTPRequest) *RequestHandler {
	requestHandler, inheritedTrace := newRequestHandler(ctx, request, nil)

	requestHandler.logger.LogInfoWithFields(logging.Fields{
		logging.KeyMethod:      requestHandler.method,
		logging.KeyPath:        requestHandler.path,
		logging.KeyQueryParams: requestHandler.queryParams,
		"traceIdInherited":     inheritedTrace,
	}, "creating discover RequestHandler (unauthenticated)")

	return requestHandler
}

func (h *RequestHandler) handleDiscover(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	switch h.path {
	case "/discover/assets":
		discoverHandler := DiscoverCloudFrontSignedURLHandler{RequestHandler: *h}
		switch h.method {
		case http.MethodGet:
			return discoverHandler.handleListAssets(ctx)
		case http.MethodOptions:
			return discoverHandler.handleOptions(ctx)
		default:
			return h.logAndBuildError(fmt.Sprintf("method %s not allowed on /discover/assets", h.method), http.StatusMethodNotAllowed), nil
		}
	default:
		return h.logAndBuildError("resource not found: "+h.path, http.StatusNotFound), nil
	}
}
