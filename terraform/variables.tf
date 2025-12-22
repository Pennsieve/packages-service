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

variable "cloudfront_public_key_pem" {
  description = "CloudFront public key in PEM format (dummy key for CI, replace with real key)"
  type        = string
  default     = <<-EOT
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArEXtPKC9oh0PU3oc5viX
fyEXEEAzKoJKp0sjplsWyGqI+ZAGCjIgDsA0cMTcfFPftZM2Lv9Y5FZNz8hX2eM1
UA5twROkiUq2Zz0V3FNWrvPv+CS509uYp4bUTRWxW4pERFhnI3jm9p3zFTsv1ygR
KL9T+k0LIPyYQk1O0Nei9h/HRoPgvD8lJlum8J/nH7kaPbA3P0JaE6N5+vtv/cCZ
ujNtvoDJdZSBM6oZKbWFSaIk02uiINs3rivJzOy6PvRlphuyASw+Q8jsP9TJl3G2
JRQ7QT+g83775KVGZ+CEPtOO+Tr+azxxpCdWG7VKeEzude8FrxbdWmG3kZ93b9Wp
JQIDAQAB
-----END PUBLIC KEY-----
EOT
}

variable "cloudfront_private_key_base64" {
  description = "CloudFront private key in base64 format (dummy key for CI, replace with real key)"
  type        = string
  sensitive   = true
  default     = "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBdTFTVTFMZlZMUEhDb3pNeEgyTW80bGdPRWVQek5tMHRSZ2VMZXpWNmZmQXQwZ3VuClZUTHc3b25MUm5ycTAvSXpXN3lXUjdRa3JtQkw3alRLRW41dStxS2hid0tmQnN0SXMrYk1ZMlprcDE4Z25UeEsKTHhvUzJ0RmN6R2tQTFBnaXpza3VlbU1naFJuaVdhb0xjeWVoa2QzcXFHRWx2Vy9WREw1QWFXVGcwbkxWa2pSbwo5eis0MFJRenVWYUU4QWtBRm14Wnpvdzd4K1ZKWUtkanlra0owaTlTOXBKVjlxSkFFU0FxZUdVeHJjSWxialhmCmJjbXdJREFRQUJBb0lCQVFDbTRJSkd3d0hha2RrbgpocWlBQlUxQzNPSk9qd2JZT2dVM2RLbEJWM0Ezd0lMUlhUck1WYUU="
}


locals {
  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
  }
  cors_allowed_origins  = var.environment_name == "prod" ? ["https://discover.pennsieve.io", "https://app.pennsieve.io"] : ["http://localhost:3000", "https://discover.pennsieve.net", "https://app.pennsieve.net"]
}
