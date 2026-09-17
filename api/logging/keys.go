package logging

// Structured log attribute keys.
//
// Use these constants instead of free-text keys so that a DataDog or CloudWatch
// Insights query can filter reliably — e.g. `@packageNodeId:N:package:abc`
// across every lambda in this service, rather than having to guess which of
// packageId / packageNodeId / nodeId a given call site happened to use.
//
// Canonical spellings resolved in this pass:
//
//   - KeyPackageNodeID ("packageNodeId") is the canonical key for a package's
//     *node* id (the "N:package:..." string). It replaces the previous mix of
//     "packageId", "packageNodeId" and "nodeId" that all referred to it.
//   - KeyPackageID ("packageId") is reserved for a package's integer primary
//     key, which is a genuinely different value — keeping them distinct is the
//     point of the rename.
const (
	// Invocation / correlation identity.
	//
	// KeyTraceID is this service's own internal correlation id for one logical
	// operation. It is deliberately NOT any AWS-issued id: API Gateway, SQS and
	// Lambda each mint a fresh id per hop, so no single one of them can follow
	// an operation across service boundaries. The AWS ids are still logged, but
	// each under its own distinct key below, so a query can tell which kind of
	// id it is looking at.
	KeyTraceID = "traceId"
	// KeyAPIGatewayRequestID is API Gateway's per-request id (one HTTP hop).
	KeyAPIGatewayRequestID = "apiGatewayRequestId"
	// KeyAWSRequestID is the Lambda invocation id from lambdacontext (one
	// invocation).
	KeyAWSRequestID = "awsRequestId"
	// KeySQSMessageID is the SQS message id (one queue hop).
	KeySQSMessageID = "sqsMessageId"

	// Request shape.
	KeyMethod      = "method"
	KeyPath        = "path"
	KeyQueryParams = "queryParams"
	KeyRequestBody = "requestBody"
	KeyClaims      = "claims"
	KeyStatusCode  = "statusCode"

	// Pennsieve domain identity.
	KeyOrganizationID     = "organizationId"
	KeyDatasetID          = "datasetId"
	KeyDatasetNodeID      = "datasetNodeId"
	KeyPublishedDatasetID = "publishedDatasetId"
	KeyPackageID          = "packageId"
	KeyPackageNodeID      = "packageNodeId"
	KeyPackageState       = "packageState"
	KeyUserID             = "userId"
	KeyViewerAssetID      = "viewerAssetId"
	KeyChatSessionID      = "chatSessionId"
	KeyName               = "name"
	KeyNewName            = "newName"
	KeySize               = "size"

	// Storage / AWS resources.
	KeyS3Bucket     = "s3Bucket"
	KeyS3Key        = "s3Key"
	KeyS3Prefix     = "s3Prefix"
	KeyS3Info       = "s3Info"
	KeyDeleteMarker = "deleteMarker"
	KeyTableName    = "tableName"
	KeyRegion       = "region"
	KeyQueueURL     = "queueUrl"

	// CloudFront.
	KeyCloudFrontKeyID       = "cloudFrontKeyId"
	KeyCloudFrontPublicKeyID = "cloudFrontPublicKeyId"
	KeyCloudFrontKeyGroupID  = "cloudFrontKeyGroupId"
	KeyCloudFrontDomain      = "cloudFrontDomain"
	KeyPathPrefix            = "pathPrefix"
	KeyResourcePattern       = "resourcePattern"
	KeyBaseURL               = "baseUrl"
	KeyExpiresAt             = "expiresAt"
	KeySecretName            = "secretName"
	KeySecretVersionID       = "secretVersionId"
	KeyRotationStep          = "rotationStep"

	// Outcomes and diagnostics.
	KeyError            = "error"
	KeyPreviousError    = "previousError"
	KeyCount            = "count"
	KeyDescendantCount  = "descendantCount"
	KeyEntryID          = "entryId"
	KeyRetryCount       = "retryCount"
	KeyWaitDuration     = "waitDuration"
	KeyUnprocessedCount = "unprocessedCount"
	KeyOriginalCount    = "originalCount"
	KeyLogLevel         = "logLevel"
	KeyBody             = "body"
	KeyDeletedCount     = "deletedCount"
	KeyBlockedCount     = "blockedCount"
	KeyRequestedCount   = "requestedCount"
	KeyTotalSize        = "totalSize"
	KeyGraceHoursLeft   = "gracePeriodHoursRemaining"
)
