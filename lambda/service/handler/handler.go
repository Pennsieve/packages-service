package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/service"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

var PennsieveDB *sql.DB
var SQSClient *sqs.Client

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	if level, ok := os.LookupEnv("LOG_LEVEL"); !ok {
		log.SetLevel(log.InfoLevel)
	} else {
		if ll, err := log.ParseLevel(level); err == nil {
			log.SetLevel(ll)
		} else {
			log.SetLevel(log.InfoLevel)
			log.Warnf("could not set log level to %q: %v", level, err)
		}
	}
}

func PackagesServiceHandler(ctx context.Context, request events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {
	claims := authorizer.ParseClaims(request.RequestContext.Authorizer.Lambda)
	handler := NewHandler(&request, claims).WithDefaultService()
	return handler.handle(ctx)
}

// RequestHandler wraps the incoming request with a logger and a service.PackagesService.
// Some request params are pulled out for convenience. Use NewHandler followed by WithDefaultService to have things
// initialized nicely. Use WithService in tests where a specially constructed or mock service.PackagesService is required.
type RequestHandler struct {
	request   *events.APIGatewayV2HTTPRequest
	requestID string

	method      string
	path        string
	queryParams map[string]string
	body        string

	logger          *log.Entry
	packagesService service.PackagesService
	claims          *authorizer.Claims
}

// NewHandler creates a RequestHandler that has its logger field initialized with useful fields.
func NewHandler(request *events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) *RequestHandler {
	method := request.RequestContext.HTTP.Method
	path := request.RequestContext.HTTP.Path
	reqID := request.RequestContext.RequestID
	logger := log.WithFields(log.Fields{
		"requestID": reqID,
	})
	requestHandler := RequestHandler{
		request:   request,
		requestID: reqID,

		method:      method,
		path:        path,
		queryParams: request.QueryStringParameters,
		body:        request.Body,

		logger: logger,
		claims: claims,
	}
	logger.WithFields(log.Fields{
		"method":      requestHandler.method,
		"path":        requestHandler.path,
		"queryParams": requestHandler.queryParams,
		"requestBody": requestHandler.body,
		"claims":      requestHandler.claims}).Info("creating RequestHandler")

	return &requestHandler
}

// WithDefaultService adds a new service.PackagesService to the RequestHandler that
// has been initialized to use PennsieveDB as the SQL database pointed to the
// workspace in the RequestHandler's OrgClaim.
func (h *RequestHandler) WithDefaultService() *RequestHandler {
	svc := service.NewPackagesService(PennsieveDB, SQSClient, int(h.claims.OrgClaim.IntId), &logging.Log{Entry: h.logger})
	h.packagesService = svc
	return h
}

// WithService simply attaches the passed in service.PackagesService to the RequestHandler. Used for
// tests that do not need to use PennsieveDB.
func (h *RequestHandler) WithService(service service.PackagesService) *RequestHandler {
	h.packagesService = service
	return h
}

func (h *RequestHandler) logAndBuildError(message string, status int) *events.APIGatewayV2HTTPResponse {
	h.logger.Error(message)
	errorBody := fmt.Sprintf("{'message': '%s (requestID: %s)'}", message, h.requestID)
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
		h.logger.Errorf("error marshalling body: [%v]: %s", body, err)
		return nil, err
	}
	return buildResponseFromString(string(bodyBytes), status), nil
}

func buildResponseFromString(body string, status int) *events.APIGatewayV2HTTPResponse {
	response := events.APIGatewayV2HTTPResponse{
		Body:       body,
		StatusCode: status,
	}
	return &response
}
