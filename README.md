# Packages Service

A serverless microservice that provides secure access to package assets and data files in the Pennsieve platform. Built with AWS Lambda, Go, and CloudFront for high-performance, scalable data access.

## Overview

The Packages Service handles:
- **Package restoration** from deleted/archived state
- **Presigned URL generation** for secure S3 access
- **CloudFront signed URLs** for optimized content delivery
- **Unauthenticated proxy** for cross-origin requests

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   API Gateway   │───▶│  Lambda Service │───▶│   RDS Postgres  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         │              │   CloudFront    │              │
         │              │  (Signed URLs)  │              │
         │              └─────────────────┘              │
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  S3 Package     │
                    │  Assets Bucket  │
                    └─────────────────┘
```

## API Endpoints

### 1. Package Restoration (`POST /restore`)
Restores deleted packages and collections from archived state.

**Authentication**: Required (dataset-level permissions)

**Request**:
```bash
POST /packages/restore?dataset_id=N:dataset:123
{
  "nodeIds": ["N:package:456", "N:collection:789"]
}
```

**Response**:
```json
{
  "success": ["N:package:456"],
  "failures": [
    {"id": "N:collection:789", "error": "not found"}
  ]
}
```

### 2. S3 Presigned URLs (`GET /presign/s3`)
Generates presigned URLs for direct S3 access to package files and viewer assets.

**Authentication**: Required (dataset-level permissions)

**Request**:
```bash
GET /packages/presign/s3?dataset_id=N:dataset:123&package_id=N:package:456&path=preview/thumbnail.jpg
```

**Response**: HTTP 307 redirect to presigned S3 URL

**Query Parameters**:
- `dataset_id` (required): Dataset node ID
- `package_id` (required): Package node ID  
- `path` (optional): Path to viewer asset within package
- `redirect` (optional): `false` to return JSON instead of redirect

### 3. CloudFront Signed URLs (`GET /cloudfront/sign`)
Generates CloudFront signed URLs for optimized content delivery with CDN caching.

**Authentication**: Required (dataset-level permissions)

**Request**:
```bash
GET /packages/cloudfront/sign?dataset_id=N:dataset:123&package_id=N:package:456&path=data.parquet
```

**Response**:
```json
{
  "signed_url": "https://d1234567890.cloudfront.net/O1/D123/P456/data.parquet?Expires=1699123456&Signature=...",
  "expires_at": 1699123456
}
```

**Features**:
- 1-hour URL expiration
- Optimized caching for Parquet files (30 days)
- Private distribution (signed URLs required)
- Better performance than direct S3 access

### 4. S3 Proxy (`GET /proxy/s3`)
Unauthenticated proxy for S3 requests using presigned URLs. Handles CORS for browser clients.

**Authentication**: None required (validates presigned URL)

**Request**:
```bash
GET /packages/proxy/s3?presigned_url=https://bucket.s3.amazonaws.com/key?X-Amz-Signature=...
```

**Response**: HTTP 307 redirect to presigned URL with CORS headers

**Features**:
- Validates presigned URL signatures
- Bucket allowlist support via `PROXY_ALLOWED_BUCKETS` env var
- CORS headers for browser compatibility
- HEAD request support for metadata

## Infrastructure

### Lambda Functions

1. **Service Lambda** (`lambda/service/`)
   - Main API handler for all endpoints
   - Handles authentication and authorization
   - Manages database connections and S3 operations

2. **Restore Lambda** (`lambda/restore/`)
   - Background processing for package restoration
   - Triggered by SQS messages from service lambda
   - Updates package states and metadata

### CloudFront Distribution

- **Private distribution** requiring signed URLs
- **Origin Access Control (OAC)** for secure S3 access
- **Optimized caching** for different file types:
  - Parquet files: 30-day cache
  - General files: 24-hour cache
- **Geographic distribution** with PriceClass_100 (US, Canada, Europe)

### S3 Bucket

- **Package viewer assets** storage
- **Intelligent Tiering** for cost optimization  
- **Versioning enabled** with 30-day cleanup
- **Private access** (CloudFront and service lambda only)
- **Server-side encryption** with AES256

### Database Integration

- **PostgreSQL** via RDS Proxy for connection pooling
- **Organization-based schema** (`"{org_id}".packages`, `"{org_id}".datasets`)
- **Package ownership validation** through dataset relationships

## Security

### Authentication & Authorization
- **JWT token validation** via shared authorizer lambda
- **Dataset-level permissions** enforced for all operations  
- **Package ownership verification** through database queries

### Private Content Access
- **S3 bucket policy** denies direct access
- **CloudFront signed URLs** with time-based expiration
- **Presigned URL validation** in proxy endpoint
- **Origin Access Control** for CloudFront-to-S3 communication

### Key Management
- **CloudFront signing keys** stored in AWS SSM Parameter Store
- **Private key encrypted** with KMS SecureString
- **Keys fetched once** during Lambda cold start (not per request)
- **Manual key rotation** through AWS Console

## Development

### Prerequisites
- Go 1.23+
- Docker & Docker Compose
- AWS CLI configured
- PostgreSQL client (for local development)

### Local Development

```bash
# Run tests locally
make test

# Run CI tests (Docker-based)
make test-ci

# Start local services
make local-services

# Build Lambda packages
make package

# Deploy to S3
make publish
```

### Testing

The service includes comprehensive tests covering:
- **Unit tests** for individual handlers
- **Integration tests** with PostgreSQL, MinIO, and DynamoDB
- **Authorization tests** for cross-dataset access
- **CORS and proxy functionality** tests

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `ENV` | Environment name (dev/prod) | ✓ |
| `PENNSIEVE_DOMAIN` | API domain | ✓ |
| `RDS_PROXY_ENDPOINT` | Database connection endpoint | ✓ |
| `VIEWER_ASSETS_BUCKET` | S3 bucket for package assets | ✓ |
| `CLOUDFRONT_DISTRIBUTION_DOMAIN` | CloudFront distribution domain | ✓ |
| `CLOUDFRONT_KEY_ID` | CloudFront public key ID | ✓ |
| `CLOUDFRONT_PRIVATE_KEY_SSM_PARAM` | SSM parameter name for private key | ✓ |
| `PROXY_ALLOWED_BUCKETS` | Comma-separated list of allowed S3 buckets | - |
| `RESTORE_PACKAGE_QUEUE_URL` | SQS queue for restore operations | ✓ |

## Deployment

The service is deployed using:
- **Terraform** for infrastructure provisioning
- **AWS Lambda** for serverless compute
- **API Gateway** for HTTP routing and authentication
- **CloudFormation** for resource orchestration

### CloudFront Setup

#### For CI/CD Deployment (Automated)

1. **Deploy with dummy keys** (CI/CD safe):
```bash
terraform apply
```
The deployment uses secure dummy keys by default, allowing all infrastructure to be created automatically.

2. **Replace with production keys** (after deployment):
```bash
# Generate real RSA key pair
cd terraform
./generate-cloudfront-keys.sh

# Update SSM parameters with real keys
aws ssm put-parameter \
  --name "/{environment}/{service}/cloudfront/private-key" \
  --value "$(cat .cloudfront-keys/private_key_base64.txt)" \
  --type "SecureString" \
  --overwrite

aws ssm put-parameter \
  --name "/{environment}/{service}/cloudfront/public-key" \
  --value "$(cat .cloudfront-keys/public_key.pem)" \
  --type "String" \
  --overwrite

# Create new CloudFront public key and update key group
aws cloudfront create-public-key \
  --public-key-config Name="pkg-assets-{environment}-key",CallerReference="pkg-assets-$(date +%s)",EncodedKey="$(cat .cloudfront-keys/public_key.pem)" \
  --query 'PublicKey.Id' --output text
```

3. **Update CloudFront key group** with the new public key ID:
```bash
# Get the new public key ID from the previous command, then update key group
NEW_KEY_ID="<public-key-id-from-previous-command>"
aws cloudfront update-key-group \
  --id "$(terraform output -raw cloudfront_key_group_id)" \
  --key-group-config Items="$NEW_KEY_ID",Name="package-assets-{environment}-key-group",Comment="Key group for package assets CloudFront signed URLs"
```

#### For Local Development

1. **Generate keys locally**:
```bash
cd terraform
./generate-cloudfront-keys.sh
```

2. **Deploy with local keys** (if overriding variables):
```bash
terraform apply -var="cloudfront_public_key_pem=$(cat .cloudfront-keys/public_key.pem)" \
                -var="cloudfront_private_key_base64=$(cat .cloudfront-keys/private_key_base64.txt)"
```

> **Security Note**: The `.cloudfront-keys/` directory is gitignored. Never commit real signing keys to version control. The dummy keys in variables.tf are safe for CI/CD and testing but should be replaced with real keys in production environments.

## Monitoring & Observability

- **CloudWatch Logs** for Lambda execution logs
- **X-Ray tracing** for request flow analysis  
- **CloudWatch Metrics** for performance monitoring
- **Structured logging** with request IDs for correlation

## Contributing

1. Follow Go conventions and gofmt formatting
2. Add tests for new functionality
3. Update API documentation in `terraform/packages-service.yml`
4. Ensure all tests pass: `make test && make test-ci`
5. Update this README for significant changes
