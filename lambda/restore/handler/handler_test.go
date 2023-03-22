package handler

import (
	"context"
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strings"
	"testing"
)

func TestNewMessageHandler(t *testing.T) {
	assert := assert.New(t)
	expectedMessageId := "new-message-handler-message-id"
	message := events.SQSMessage{
		MessageId: expectedMessageId,
	}
	sqlFactory := new(MockSQLFactory)
	handler := NewMessageHandler(message, &Store{SQLFactory: sqlFactory})

	assert.Equal(message, handler.Message)
	assert.Equal(sqlFactory, handler.Store.SQLFactory)
	assert.NotNil(handler.Logger)

	assert.Equal(expectedMessageId, handler.newBatchItemFailure().ItemIdentifier)

	wrappedError := errors.New("inner error")
	err := handler.errorf("had a problem with %d: %w", 4, wrappedError)
	assert.True(errors.Is(err, wrappedError))
	assert.Equal(wrappedError, errors.Unwrap(err))
	assert.True(strings.HasPrefix(err.Error(), m))
	assert.Contains(err.Error(), "had a problem with 4")
	assert.Contains(err.Error(), wrappedError.Error())

}

type MockSQLFactory struct {
	mock.Mock
}

func (m *MockSQLFactory) NewSimpleStore(orgId int) store.SQLStore {
	args := m.Called(orgId)
	return args.Get(0).(store.SQLStore)
}

func (m *MockSQLFactory) ExecStoreTx(ctx context.Context, orgId int, fn func(store store.SQLStore) error) error {
	args := m.Called(ctx, orgId, fn)
	return args.Error(0)
}
