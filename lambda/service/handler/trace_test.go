package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveTraceID_InheritsInboundHeader(t *testing.T) {
	for _, h := range []string{"x-request-id", "X-Request-Id", "traceparent", "x-amzn-trace-id", "x-correlation-id"} {
		id, inherited := resolveTraceID(map[string]string{h: "inbound-123"})
		assert.True(t, inherited, "header %s should be adopted", h)
		assert.Equal(t, "inbound-123", id, "header %s", h)
	}
}

func TestResolveTraceID_PrecedenceOrder(t *testing.T) {
	id, inherited := resolveTraceID(map[string]string{
		"x-amzn-trace-id": "amzn",
		"x-request-id":    "preferred",
	})
	assert.True(t, inherited)
	assert.Equal(t, "preferred", id)
}

func TestResolveTraceID_GeneratesWhenAbsent(t *testing.T) {
	id, inherited := resolveTraceID(nil)
	assert.False(t, inherited)
	assert.NotEmpty(t, id)

	other, _ := resolveTraceID(nil)
	assert.NotEqual(t, id, other, "generated trace ids must be unique per invocation")
}

func TestResolveTraceID_IgnoresEmptyHeaderValue(t *testing.T) {
	id, inherited := resolveTraceID(map[string]string{"x-request-id": ""})
	assert.False(t, inherited, "an empty header value must not be adopted")
	assert.NotEmpty(t, id)
}

// The trace id must be distinct from API Gateway's per-hop request id: they are
// different kinds of identifier and must not collapse into one field.
func TestNewRequestHandler_TraceIDDistinctFromAPIGatewayRequestID(t *testing.T) {
	req := newTestRequest("GET", "/formats", "apigw-req-1", nil, "")
	h, _ := newRequestHandler(context.Background(), req, nil)
	assert.Equal(t, "apigw-req-1", h.apiGatewayRequestID)
	assert.NotEmpty(t, h.traceID)
	assert.NotEqual(t, h.apiGatewayRequestID, h.traceID)
}
