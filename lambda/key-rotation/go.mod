module github.com/pennsieve/packages-service/lambda/key-rotation

go 1.22

toolchain go1.23.12

require (
	github.com/aws/aws-lambda-go v1.47.0
	github.com/aws/aws-sdk-go-v2 v1.37.2
	github.com/aws/aws-sdk-go-v2/config v1.28.7
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.44.0
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.37.0
)

require (
	github.com/aws/aws-sdk-go-v2/credentials v1.17.48 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.3 // indirect
	github.com/aws/smithy-go v1.22.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
)
