# CloudWatch Alarms for CloudFront Key Rotation Monitoring

# Alarm for key rotation Lambda errors
resource "aws_cloudwatch_metric_alarm" "key_rotation_lambda_errors" {
  alarm_name          = "${var.environment_name}-${var.service_name}-key-rotation-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "300"  # 5 minutes
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "CloudFront key rotation Lambda errors"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.key_rotation.function_name
  }

  alarm_actions = [data.terraform_remote_state.account.outputs.ops_victor_ops_sns_topic_arn]
  ok_actions    = [data.terraform_remote_state.account.outputs.ops_victor_ops_sns_topic_arn]

  tags = merge(local.common_tags, {
    Name = "${var.environment_name}-${var.service_name}-key-rotation-errors"
  })
}

# Alarm for Secrets Manager rotation failures
resource "aws_cloudwatch_metric_alarm" "secrets_rotation_failed" {
  alarm_name          = "${var.environment_name}-${var.service_name}-secrets-rotation-failed"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "RotationSucceeded"
  namespace           = "AWS/SecretsManager"
  period              = "86400"  # 24 hours
  statistic           = "Average"
  threshold           = "1"
  alarm_description   = "CloudFront key rotation failed in Secrets Manager"
  treat_missing_data  = "notBreaching"

  dimensions = {
    SecretName = aws_secretsmanager_secret.cloudfront_signing_keys.name
  }

  alarm_actions = [data.terraform_remote_state.account.outputs.ops_victor_ops_sns_topic_arn]

  tags = merge(local.common_tags, {
    Name = "${var.environment_name}-${var.service_name}-secrets-rotation-failed"
  })
}

# Alarm for service Lambda sustained errors (key loading failures)
resource "aws_cloudwatch_metric_alarm" "service_lambda_key_errors" {
  alarm_name          = "${var.environment_name}-${var.service_name}-lambda-key-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "3"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "300"  # 5 minutes
  statistic           = "Sum"
  threshold           = "25"   # Alert only on sustained errors
  alarm_description   = "Service Lambda sustained errors (possible key loading issues)"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.service_lambda.function_name
  }

  alarm_actions = [data.terraform_remote_state.account.outputs.ops_victor_ops_sns_topic_arn]

  tags = merge(local.common_tags, {
    Name = "${var.environment_name}-${var.service_name}-lambda-key-errors"
  })
}

# Alarm for scheduled key cleanup failures  
resource "aws_cloudwatch_metric_alarm" "key_cleanup_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-key-cleanup-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  threshold           = "0"
  alarm_description   = "CloudFront scheduled key cleanup failures"
  treat_missing_data  = "notBreaching"

  # Use metric query to monitor Lambda errors
  metric_query {
    id          = "cleanup_errors"
    return_data = true

    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 3600  # 1 hour - since cleanup runs every 12 hours
      stat        = "Sum"

      dimensions = {
        FunctionName = aws_lambda_function.key_rotation.function_name
      }
    }
  }

  alarm_actions = [data.terraform_remote_state.account.outputs.ops_victor_ops_sns_topic_arn]
  ok_actions    = [data.terraform_remote_state.account.outputs.ops_victor_ops_sns_topic_arn]

  tags = merge(local.common_tags, {
    Name = "${var.environment_name}-${var.service_name}-key-cleanup-failures"
  })
}