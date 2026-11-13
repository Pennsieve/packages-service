// Create log group for packages-service API Lambda.
resource "aws_cloudwatch_log_group" "packages_service_api_lambda_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.service_lambda.function_name}"
  retention_in_days = 30
  tags              = local.common_tags
}

// Packages SERVICE API GATEWAY
resource "aws_cloudwatch_log_group" "packages_service_gateway_log_group" {
  name = "${var.environment_name}/${var.service_name}/packages-api-gateway"

  retention_in_days = 30
}
