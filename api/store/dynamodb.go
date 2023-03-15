package store

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type dynamodbStore struct {
	Client *dynamodb.Client
}

type NoSQLStore interface {
}

func NewNoSQLStore(dynamodbClient *dynamodb.Client) ObjectStore {
	return &dynamodbStore{Client: dynamodbClient}
}
