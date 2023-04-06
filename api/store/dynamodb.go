package store

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/packages-service/api/logging"
	"github.com/pennsieve/packages-service/api/models"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	maxGetItemBatch             = 100
	maxWriteItemBatch           = 25
	DeleteRecordTableNameEnvKey = "DELETE_RECORD_DYNAMODB_TABLE_NAME"
)

var (
	deleteMarkerVersionProjection = "NodeId, S3Bucket, S3Key, S3ObjectVersion, ObjectSize"
	deleteRecordTable             string
)

func init() {
	deleteRecordTable = os.Getenv(DeleteRecordTableNameEnvKey)
}

type DynamoDBStore struct {
	Client *dynamodb.Client
}

func NewDynamoDBStore(client *dynamodb.Client) *DynamoDBStore {
	return &DynamoDBStore{Client: client}
}

func (d *DynamoDBStore) WithLogging(log *logging.Log) NoSQLStore {
	return &dynamodbStore{
		DynamoDBStore: d,
		Log:           log,
	}
}

type dynamodbStore struct {
	*DynamoDBStore
	*logging.Log
}

type S3ObjectInfo struct {
	NodeId    string `dynamodbav:"NodeId"`
	Bucket    string `dynamodbav:"S3Bucket"`
	Key       string `dynamodbav:"S3Key"`
	VersionId string `dynamodbav:"S3ObjectVersion"`
	Size      string `dynamodbav:"ObjectSize"`
}

func (o *S3ObjectInfo) GetSize() (int64, error) {
	if len(o.Size) == 0 {
		return 0, nil
	}
	size, err := strconv.ParseInt(o.Size, 10, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

type GetDeleteMarkerVersionsResponse map[string]*S3ObjectInfo

type NoSQLStore interface {
	GetDeleteMarkerVersions(ctx context.Context, restoring ...*models.RestorePackageInfo) (GetDeleteMarkerVersionsResponse, error)
	RemoveDeleteRecords(ctx context.Context, restoring []*models.RestorePackageInfo) error
	logging.Logger
}

func (d *dynamodbStore) GetDeleteMarkerVersions(ctx context.Context, restoring ...*models.RestorePackageInfo) (GetDeleteMarkerVersionsResponse, error) {
	deleteMarkerVersions := GetDeleteMarkerVersionsResponse{}
	for i := 0; i < len(restoring); i += maxGetItemBatch {
		j := i + maxGetItemBatch
		if j > len(restoring) {
			j = len(restoring)
		}
		batch := restoring[i:j]
		keys := make([]map[string]types.AttributeValue, len(batch))
		for i, r := range batch {
			keys[i] = map[string]types.AttributeValue{"NodeId": &types.AttributeValueMemberS{Value: r.NodeId}}
		}
		items, err := d.getBatchItemsSingleTable(ctx, deleteRecordTable, &deleteMarkerVersionProjection, keys)
		if err != nil {
			return nil, fmt.Errorf("error reading delete records from %s: %w", deleteRecordTable, err)
		}
		for _, item := range items {
			objectInfo := S3ObjectInfo{}
			if err := attributevalue.UnmarshalMap(item, &objectInfo); err != nil {
				return nil, fmt.Errorf("error unmarshalling %v: %w", item, err)
			}
			deleteMarkerVersions[objectInfo.NodeId] = &objectInfo
		}

	}
	return deleteMarkerVersions, nil
}

func (d *dynamodbStore) getBatchItemsSingleTable(ctx context.Context, tableName string, projectionExpression *string, keys []map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	var items []map[string]types.AttributeValue
	makeOneRequest := func(ctx context.Context, input *dynamodb.BatchGetItemInput) (unprocessedKeys types.KeysAndAttributes, err error) {
		var output *dynamodb.BatchGetItemOutput
		output, err = d.Client.BatchGetItem(ctx, input)
		if err != nil {
			return
		}
		responses, ok := output.Responses[tableName]
		if !ok {
			err = fmt.Errorf("unexpected error: no responses for table %s", tableName)
			return
		}
		items = append(items, responses...)
		unprocessedKeys = output.UnprocessedKeys[tableName]
		return
	}

	requestKeys := types.KeysAndAttributes{Keys: keys, ProjectionExpression: projectionExpression}
	input := dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{tableName: requestKeys}}
	unprocessed, err := makeOneRequest(ctx, &input)
	if err != nil {
		return nil, err
	}
	retryCount := 1
	for len(unprocessed.Keys) > 0 {
		waitDuration := time.Duration(retryCount)*time.Second + (time.Duration(rand.Intn(1000)) * time.Millisecond)
		time.Sleep(waitDuration)
		log.Infof("retrying %d unprocessed items out of an original %d after a wait of %s", len(unprocessed.Keys), len(keys), waitDuration)
		input := dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{tableName: unprocessed}}
		unprocessed, err = makeOneRequest(ctx, &input)
		if err != nil {
			return nil, err
		}
		retryCount++
	}
	return items, nil
}

func (d *dynamodbStore) RemoveDeleteRecords(ctx context.Context, restoring []*models.RestorePackageInfo) error {
	for i := 0; i < len(restoring); i += maxWriteItemBatch {
		j := i + maxWriteItemBatch
		if j > len(restoring) {
			j = len(restoring)
		}
		batch := restoring[i:j]
		keys := make([]types.WriteRequest, len(batch))
		for i, r := range batch {
			deleteRequest := types.DeleteRequest{Key: map[string]types.AttributeValue{"NodeId": &types.AttributeValueMemberS{Value: r.NodeId}}}
			keys[i] = types.WriteRequest{DeleteRequest: &deleteRequest}
		}
		err := d.deleteBatchItemsSingleTable(ctx, deleteRecordTable, keys)
		if err != nil {
			return fmt.Errorf("error removing delete records from %s: %w", deleteRecordTable, err)
		}
	}
	return nil
}

func (d *dynamodbStore) deleteBatchItemsSingleTable(ctx context.Context, tableName string, writeRequests []types.WriteRequest) error {
	makeOneRequest := func(ctx context.Context, input *dynamodb.BatchWriteItemInput) (unprocessedKeys []types.WriteRequest, err error) {
		var output *dynamodb.BatchWriteItemOutput
		output, err = d.Client.BatchWriteItem(ctx, input)
		if err != nil {
			return
		}
		unprocessedKeys = output.UnprocessedItems[tableName]
		return
	}

	input := dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{tableName: writeRequests}}
	unprocessed, err := makeOneRequest(ctx, &input)
	if err != nil {
		return err
	}
	retryCount := 1
	for unprocessed != nil && len(unprocessed) > 0 {
		waitDuration := time.Duration(retryCount)*time.Second + (time.Duration(rand.Intn(1000)) * time.Millisecond)
		time.Sleep(waitDuration)
		log.Infof("retrying %d unprocessed items out of an original %d after a wait of %s", len(unprocessed), len(writeRequests), waitDuration)
		input := dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{tableName: unprocessed}}
		unprocessed, err = makeOneRequest(ctx, &input)
		if err != nil {
			return err
		}
		retryCount++
	}
	return nil
}
