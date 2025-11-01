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
