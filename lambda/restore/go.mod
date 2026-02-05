module github.com/pennsieve/packages-service/restore

go 1.23

toolchain go1.23.12

replace github.com/pennsieve/packages-service/api => ../../api

require (
	github.com/aws/aws-lambda-go v1.38.0
	github.com/aws/aws-sdk-go-v2 v1.39.6
	github.com/aws/aws-sdk-go-v2/config v1.31.19
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.22
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.52.5
	github.com/aws/aws-sdk-go-v2/service/s3 v1.90.1
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.14
	github.com/google/uuid v1.3.0
	github.com/pennsieve/packages-service/api v0.0.0-00010101000000-000000000000
	github.com/pennsieve/pennsieve-go-core v1.13.6
	github.com/sirupsen/logrus v1.9.1
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.3 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.23 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.13 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.40.1 // indirect
	github.com/aws/smithy-go v1.23.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/lib/pq v1.10.7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/sys v0.21.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
