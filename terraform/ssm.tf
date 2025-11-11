# SSM Parameters for CloudFront signing keys
# Uses dummy keys by default for CI/CD. Replace with real keys after deployment.
resource "aws_ssm_parameter" "cloudfront_private_key" {
  name        = "/${var.environment_name}/${var.service_name}/cloudfront-private-key"
  description = "CloudFront private key for signing URLs (base64 encoded)"
  type        = "SecureString"
  value       = var.cloudfront_private_key_base64

  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-cloudfront-private-key"
    Description = "CloudFront private key for package assets"
    Service     = "packages-service"
  })

  lifecycle {
    ignore_changes = [value]
  }
}

# SSM Parameter for CloudFront public key
resource "aws_ssm_parameter" "cloudfront_public_key" {
  name        = "/${var.environment_name}/${var.service_name}/cloudfront-public-key"
  description = "CloudFront public key for signing URLs"
  type        = "String"
  value       = var.cloudfront_public_key_pem

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
