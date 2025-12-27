resource "aws_lambda_function" "service_lambda" {
  description   = "Lambda Function which handles requests for serverless packages-service"
  function_name = "${var.environment_name}-${var.service_name}-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2"
  role          = aws_iam_role.packages_service_lambda_role.arn
  timeout       = 300
  memory_size   = 512
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/packages-service-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                                 = var.environment_name
      PENNSIEVE_DOMAIN                    = data.terraform_remote_state.account.outputs.domain_name
      REGION                              = var.aws_region
      RDS_PROXY_ENDPOINT                  = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint
      RESTORE_PACKAGE_QUEUE_URL           = aws_sqs_queue.restore_package_queue.url
      CLOUDFRONT_DISTRIBUTION_DOMAIN      = data.terraform_remote_state.platform_infrastructure.outputs.package_assets_cloudfront_domain_name
      CLOUDFRONT_KEY_ID                   = data.terraform_remote_state.platform_infrastructure.outputs.assets_distribution_public_key
      CLOUDFRONT_KEY_GROUP_ID             = data.terraform_remote_state.platform_infrastructure.outputs.package_assets_key_group_id
      CLOUDFRONT_SIGNING_KEYS_SECRET_NAME = aws_secretsmanager_secret.cloudfront_signing_keys.name
    }
  }
}

resource "aws_lambda_function" "restore_package_lambda" {
  description   = "Lambda Function which listens to a SQS queue to process restore packages requests"
  function_name = "${var.environment_name}-restore-package-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2"
  role          = aws_iam_role.restore_package_lambda_role.arn
  timeout       = 900
  memory_size   = 256
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/restore-package-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                               = var.environment_name
      PENNSIEVE_DOMAIN                  = data.terraform_remote_state.account.outputs.domain_name,
      REGION                            = var.aws_region
      RDS_PROXY_ENDPOINT                = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      RESTORE_PACKAGE_QUEUE_URL         = aws_sqs_queue.restore_package_queue.url
      DELETE_RECORD_DYNAMODB_TABLE_NAME = data.terraform_remote_state.process_jobs_service.outputs.process_jobs_table_name
      JOBS_QUEUE_ID                     = data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_id,
    }
  }
}

# Lambda function for key rotation
resource "aws_lambda_function" "key_rotation" {
  function_name = "${var.environment_name}-${var.service_name}-key-rotation"
  role          = aws_iam_role.key_rotation_lambda.arn
  handler       = "bootstrap"
  runtime       = "provided.al2"
  architectures = ["x86_64"]
  timeout       = 60
  memory_size   = 256
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/key-rotation-${var.image_tag}.zip"

  environment {
    variables = {
      ENVIRONMENT                    = var.environment_name
      CLOUDFRONT_KEY_GROUP_ID        = data.terraform_remote_state.platform_infrastructure.outputs.package_assets_key_group_id
      KEY_ROTATION_GRACE_PERIOD_HOURS = "48"  # Grace period before removing old keys from CloudFront
    }
  }

  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-key-rotation"
    Description = "Lambda function for automatic CloudFront key rotation"
  })
}