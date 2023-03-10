resource "aws_lambda_function" "service_lambda" {
  description   = "Lambda Function which handles requests for serverless packages-service"
  function_name = "${var.environment_name}-${var.service_name}-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "packages_service"
  runtime       = "go1.x"
  role          = aws_iam_role.packages_service_lambda_role.arn
  timeout       = 300
  memory_size   = 128
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/${var.service_name}-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                       = var.environment_name
      PENNSIEVE_DOMAIN          = data.terraform_remote_state.account.outputs.domain_name,
      REGION                    = var.aws_region
      RDS_PROXY_ENDPOINT        = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      RESTORE_PACKAGE_QUEUE_URL = aws_sqs_queue.restore_package_queue.url
    }
  }
}

resource "aws_lambda_function" "restore_package_lambda" {
  description   = "Lambda Function which listens to a SQS queue to process restore packages requests"
  function_name = "${var.environment_name}-restore-package-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "restore_package"
  runtime       = "go1.x"
  role          = aws_iam_role.restore_package_lambda_role.arn
  timeout       = 300
  memory_size   = 128
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/restore-package-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                       = var.environment_name
      PENNSIEVE_DOMAIN          = data.terraform_remote_state.account.outputs.domain_name,
      REGION                    = var.aws_region
      RDS_PROXY_ENDPOINT        = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      RESTORE_PACKAGE_QUEUE_URL = aws_sqs_queue.restore_package_queue.url
    }
  }
}
