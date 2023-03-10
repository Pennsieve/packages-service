resource "aws_sqs_queue" "restore_package_queue" {
  name                       = "${var.environment_name}-restore-package-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20
  visibility_timeout_seconds = 3600
  redrive_policy             = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.restore_package_deadletter_queue.arn}\",\"maxReceiveCount\":3}"
}

resource "aws_sqs_queue" "restore_package_deadletter_queue" {
  name                       = "${var.environment_name}-restore-package-deadletter-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  message_retention_seconds  = 1209600
  receive_wait_time_seconds  = 20
  visibility_timeout_seconds = 3600
}

resource "aws_lambda_event_source_mapping" "restore_package_mapping" {
  event_source_arn = aws_sqs_queue.restore_package_queue.arn
  function_name    = aws_lambda_function.restore_package_lambda.arn
}