# Secrets Manager for CloudFront signing keys with automatic rotation
resource "aws_secretsmanager_secret" "cloudfront_signing_keys" {
  name                    = "${var.environment_name}-${var.service_name}-cloudfront-signing-keys"
  description             = "CloudFront signing keys for package assets with automatic rotation"
  recovery_window_in_days = 7

  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-cloudfront-signing-keys"
    Description = "CloudFront signing keys with automatic rotation"
    Service     = "packages-service"
  })
}

# Initial secret version with dummy keys (will be replaced by rotation)
resource "aws_secretsmanager_secret_version" "cloudfront_signing_keys_initial" {
  secret_id = aws_secretsmanager_secret.cloudfront_signing_keys.id
  
  secret_string = jsonencode({
    privateKey  = var.cloudfront_private_key_base64
    publicKey   = var.cloudfront_public_key_pem
    keyId       = "initial-key"
    createdAt   = timestamp()
    keyGroupId  = ""
    publicKeyId = ""
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# Secret rotation configuration
resource "aws_secretsmanager_secret_rotation" "cloudfront_keys_rotation" {
  secret_id           = aws_secretsmanager_secret.cloudfront_signing_keys.id
  rotation_lambda_arn = aws_lambda_function.key_rotation.arn

  rotation_rules {
    automatically_after_days = 30
  }

  depends_on = [
    aws_lambda_permission.allow_secretsmanager_rotation
  ]
}

# IAM role for rotation Lambda
resource "aws_iam_role" "key_rotation_lambda" {
  name = "${var.environment_name}-${var.service_name}-key-rotation-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })

  tags = local.common_tags
}

# IAM policy for rotation Lambda
resource "aws_iam_role_policy" "key_rotation_lambda" {
  name = "${var.environment_name}-${var.service_name}-key-rotation-policy"
  role = aws_iam_role.key_rotation_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:DescribeSecret",
          "secretsmanager:GetSecretValue",
          "secretsmanager:PutSecretValue",
          "secretsmanager:UpdateSecretVersionStage",
          "secretsmanager:ListSecretVersionIds"
        ]
        Resource = aws_secretsmanager_secret.cloudfront_signing_keys.arn
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetRandomPassword"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudfront:CreatePublicKey",
          "cloudfront:GetPublicKey",
          "cloudfront:GetKeyGroup",
          "cloudfront:UpdateKeyGroup",
          "cloudfront:ListPublicKeys"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "kms:GenerateDataKeyPair",
          "kms:Decrypt",
          "kms:Encrypt"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach AWS managed policy for Lambda execution
resource "aws_iam_role_policy_attachment" "key_rotation_lambda_basic" {
  role       = aws_iam_role.key_rotation_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


# Permission for Secrets Manager to invoke rotation Lambda
resource "aws_lambda_permission" "allow_secretsmanager_rotation" {
  statement_id  = "AllowSecretsManagerInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.key_rotation.function_name
  principal     = "secretsmanager.amazonaws.com"
}


# Output the secret ARN for use by other services
output "cloudfront_signing_keys_secret_arn" {
  value       = aws_secretsmanager_secret.cloudfront_signing_keys.arn
  description = "ARN of the Secrets Manager secret containing CloudFront signing keys"
  sensitive   = true
}

output "cloudfront_signing_keys_secret_name" {
  value       = aws_secretsmanager_secret.cloudfront_signing_keys.name
  description = "Name of the Secrets Manager secret containing CloudFront signing keys"
}

# EventBridge rule for scheduled CloudFront key cleanup
resource "aws_cloudwatch_event_rule" "cloudfront_key_cleanup" {
  name                = "${var.environment_name}-${var.service_name}-cloudfront-key-cleanup"
  description         = "Scheduled cleanup of expired CloudFront keys"
  schedule_expression = "rate(12 hours)"  # Run twice daily to ensure timely cleanup
  
  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-cloudfront-key-cleanup"
    Description = "EventBridge rule for CloudFront key cleanup"
    Service     = "packages-service"
  })
}

# EventBridge target to invoke key rotation Lambda for cleanup
resource "aws_cloudwatch_event_target" "cloudfront_key_cleanup_target" {
  rule      = aws_cloudwatch_event_rule.cloudfront_key_cleanup.name
  target_id = "CloudFrontKeyCleanupTarget"
  arn       = aws_lambda_function.key_rotation.arn
  
  input = jsonencode({
    Step               = "cleanupOldKeys"
    SecretId           = aws_secretsmanager_secret.cloudfront_signing_keys.arn
    ClientRequestToken = "scheduled-cleanup-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
  })
}

# Permission for EventBridge to invoke the key rotation Lambda
resource "aws_lambda_permission" "allow_eventbridge_cleanup" {
  statement_id  = "AllowEventBridgeCleanupInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.key_rotation.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cloudfront_key_cleanup.arn
}