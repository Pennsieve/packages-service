output "service_lambda_arn" {
  value = aws_lambda_function.service_lambda.arn
}

output "service_lambda_invoke_arn" {
  value = aws_lambda_function.service_lambda.invoke_arn
}

output "service_lambda_function_name" {
  value = aws_lambda_function.service_lambda.function_name
}

output "package_assets_bucket_name" {
  value = aws_s3_bucket.package_assets.bucket
}

output "package_assets_bucket_arn" {
  value = aws_s3_bucket.package_assets.arn
}

output "package_assets_cloudfront_distribution_id" {
  value = aws_cloudfront_distribution.package_assets.id
}

output "package_assets_cloudfront_domain_name" {
  value = aws_cloudfront_distribution.package_assets.domain_name
}

output "package_assets_cloudfront_arn" {
  value = aws_cloudfront_distribution.package_assets.arn
}

output "package_assets_key_group_id" {
  value = aws_cloudfront_key_group.package_assets.id
}
