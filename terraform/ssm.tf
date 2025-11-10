# SSM Parameters for CloudFront signing keys
resource "aws_ssm_parameter" "cloudfront_private_key" {
  name        = "/${var.environment_name}/${var.service_name}/cloudfront/private-key"
  description = "CloudFront private key for signing URLs (base64 encoded)"
  type        = "SecureString"
  value       = "dummy" # Real value to be set manually in AWS console

  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-cloudfront-private-key"
    Description = "CloudFront private key for package assets"
    Service     = "packages-service"
  })

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "cloudfront_public_key" {
  name        = "/${var.environment_name}/${var.service_name}/cloudfront/public-key"
  description = "CloudFront public key for signing URLs"
  type        = "String"
  value       = "dummy" # Real value to be set manually in AWS console

  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-cloudfront-public-key"
    Description = "CloudFront public key for package assets"
    Service     = "packages-service"
  })

  lifecycle {
    ignore_changes = [value]
  }
}

# Mark outputs as sensitive
output "cloudfront_private_key_ssm_parameter" {
  value       = aws_ssm_parameter.cloudfront_private_key.name
  description = "SSM parameter name for CloudFront private key"
  sensitive   = true
}

output "cloudfront_public_key_ssm_parameter" {
  value       = aws_ssm_parameter.cloudfront_public_key.name
  description = "SSM parameter name for CloudFront public key"
  sensitive   = true
}