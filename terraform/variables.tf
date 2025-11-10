variable "aws_account" {}

variable "aws_region" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "domain_name" {}

variable "image_tag" {}

variable "lambda_bucket" {
  default = "pennsieve-cc-lambda-functions-use1"
}

variable "api_domain_name" {}

variable "proxy_allowed_buckets" {
  description = "Comma-separated list of S3 bucket names allowed for the unauthenticated proxy endpoint. Leave empty to allow all buckets."
  type        = string
  default     = ""
}


locals {
  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
  }
  cors_allowed_origins  = var.environment_name == "prod" ? ["https://discover.pennsieve.io", "https://app.pennsieve.io"] : ["http://localhost:3000", "https://discover.pennsieve.net", "https://app.pennsieve.net"]
}
