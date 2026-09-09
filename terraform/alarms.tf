# T1 CloudWatch alarms (EPIC 868m2zvjt; standard sets from
# pennsieve-infra-dashboard/docs/alarm-coverage-plan.md). No alarm_actions
# yet — dashboard/console-visible only.
module "service_alarms" {
  source = "git@github.com:Pennsieve/terraform-modules.git//service-alarms"

  environment_name = var.environment_name
  service_name     = var.service_name

  lambdas = {
    asset-cleanup = {
      function_name   = aws_lambda_function.asset_cleanup_lambda.function_name
      timeout_seconds = aws_lambda_function.asset_cleanup_lambda.timeout
    }
    key-rotation = {
      function_name   = aws_lambda_function.key_rotation.function_name
      timeout_seconds = aws_lambda_function.key_rotation.timeout
    }
    restore-package = {
      function_name   = aws_lambda_function.restore_package_lambda.function_name
      timeout_seconds = aws_lambda_function.restore_package_lambda.timeout
    }
    service = {
      function_name   = aws_lambda_function.service_lambda.function_name
      timeout_seconds = aws_lambda_function.service_lambda.timeout
    }
  }

  queues = {
    restore-package = {
      queue_name      = aws_sqs_queue.restore_package_queue.name
      max_age_seconds = 3600
    }
  }

  dlqs = {
    restore-package = aws_sqs_queue.restore_package_deadletter_queue.name
  }

}
