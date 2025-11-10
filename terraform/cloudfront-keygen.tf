# Lambda function to generate CloudFront key pair
resource "aws_iam_role" "cloudfront_keygen_lambda" {
  name = "${var.service_name}-${var.environment_name}-cf-keygen-role"

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
}

resource "aws_iam_role_policy" "cloudfront_keygen_lambda" {
  name = "${var.service_name}-${var.environment_name}-cf-keygen-policy"
  role = aws_iam_role.cloudfront_keygen_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:PutParameter",
          "ssm:GetParameter",
          "ssm:DeleteParameter"
        ]
        Resource = [
          "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/${var.environment_name}/${var.service_name}/cloudfront/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cloudfront_keygen_lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.cloudfront_keygen_lambda.name
}

# Lambda function code to generate RSA key pair
resource "aws_lambda_function" "cloudfront_keygen" {
  filename      = "${path.module}/cloudfront-keygen-lambda.zip"
  function_name = "${var.service_name}-${var.environment_name}-cf-keygen"
  role          = aws_iam_role.cloudfront_keygen_lambda.arn
  handler       = "index.handler"
  runtime       = "python3.11"
  timeout       = 30

  environment {
    variables = {
      ENVIRONMENT = var.environment_name
      SERVICE     = var.service_name
    }
  }

  tags = local.common_tags
}

# Create the Lambda deployment package
data "archive_file" "cloudfront_keygen_lambda" {
  type        = "zip"
  output_path = "${path.module}/cloudfront-keygen-lambda.zip"
  
  source {
    content  = file("${path.module}/cloudfront-keygen.py")
    filename = "index.py"
  }
}

# Custom resource to generate keys
resource "aws_cloudformation_stack" "cloudfront_keys" {
  name = "${var.service_name}-${var.environment_name}-cf-keys"

  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Resources = {
      CloudFrontKeyGenerator = {
        Type = "Custom::CloudFrontKeyGenerator"
        Properties = {
          ServiceToken = aws_lambda_function.cloudfront_keygen.arn
          Environment  = var.environment_name
          Service      = var.service_name
        }
      }
    }
    Outputs = {
      PublicKey = {
        Value = { "Fn::GetAtt" = ["CloudFrontKeyGenerator", "PublicKey"] }
      }
      PublicKeySSMPath = {
        Value = { "Fn::GetAtt" = ["CloudFrontKeyGenerator", "PublicKeySSMPath"] }
      }
      PrivateKeySSMPath = {
        Value = { "Fn::GetAtt" = ["CloudFrontKeyGenerator", "PrivateKeySSMPath"] }
      }
    }
  })
}

# CloudFront public key using the generated key
resource "aws_cloudfront_public_key" "package_assets" {
  comment     = "Auto-generated public key for package assets CloudFront signed URLs"
  encoded_key = aws_cloudformation_stack.cloudfront_keys.outputs["PublicKey"]
  name        = "package-assets-${var.environment_name}-public-key"
}

resource "aws_cloudfront_key_group" "package_assets" {
  comment = "Key group for package assets CloudFront signed URLs"
  items   = [aws_cloudfront_public_key.package_assets.id]
  name    = "package-assets-${var.environment_name}-key-group"
}