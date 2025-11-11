# Generate CloudFront keys if they don't exist
resource "null_resource" "generate_cloudfront_keys" {
  provisioner "local-exec" {
    command = "${path.module}/generate-cloudfront-keys.sh"
  }
  
  # Run script to generate keys (script will check if they exist)
  triggers = {
    script_hash = filesha256("${path.module}/generate-cloudfront-keys.sh")
  }
}

# SSM Parameters for CloudFront signing keys
resource "aws_ssm_parameter" "cloudfront_private_key" {
  name        = "/${var.environment_name}/${var.service_name}/cloudfront/private-key"
  description = "CloudFront private key for signing URLs (base64 encoded)"
  type        = "SecureString"
  value       = sensitive(filebase64("${path.module}/.cloudfront-keys/private_key.pem"))

  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-cloudfront-private-key"
    Description = "CloudFront private key for package assets"
    Service     = "packages-service"
  })
  
  depends_on = [null_resource.generate_cloudfront_keys]
}

# SSM Parameter for CloudFront public key
resource "aws_ssm_parameter" "cloudfront_public_key" {
  name        = "/${var.environment_name}/${var.service_name}/cloudfront/public-key"
  description = "CloudFront public key for signing URLs"
  type        = "String"
  value       = file("${path.module}/.cloudfront-keys/public_key.pem")

  tags = merge(local.common_tags, {
    Name        = "${var.environment_name}-${var.service_name}-cloudfront-public-key"
    Description = "CloudFront public key for package assets"
    Service     = "packages-service"
  })
  
  depends_on = [null_resource.generate_cloudfront_keys]
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
