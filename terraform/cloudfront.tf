
# CloudFront Origin Access Control for the S3 bucket
resource "aws_cloudfront_origin_access_control" "package_assets" {
  name                              = "pkg-assets-${var.environment_name}-oac"
  description                       = "OAC for package assets S3 bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# CloudFront response headers policy for CORS
resource "aws_cloudfront_response_headers_policy" "package_assets_cors" {
  name = "pkg-assets-${var.environment_name}-cors-policy"

  cors_config {
    access_control_allow_credentials = false

    access_control_allow_headers {
      items = ["*"]
    }

    access_control_allow_methods {
      items = ["GET", "HEAD", "OPTIONS"]
    }

    access_control_allow_origins {
      items = local.cors_allowed_origins
    }

    access_control_max_age_sec = 3600
    origin_override            = true
  }
}



# CloudFront public key from variable (dummy key for CI, replace with real key after deployment)
resource "aws_cloudfront_public_key" "package_assets" {
  comment     = "Public key for package assets CloudFront signed URLs"
  encoded_key = var.cloudfront_public_key_pem
  name        = "package-assets-${var.environment_name}-public-key"
}

# CloudFront key group
resource "aws_cloudfront_key_group" "package_assets" {
  comment = "Key group for package assets CloudFront signed URLs"
  items   = [aws_cloudfront_public_key.package_assets.id]
  name    = "package-assets-${var.environment_name}-key-group"
}

# Private CloudFront distribution for the package assets bucket
resource "aws_cloudfront_distribution" "package_assets" {
  origin {
    domain_name              = aws_s3_bucket.package_assets.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.package_assets.id
    origin_id                = "S3-${aws_s3_bucket.package_assets.id}"
  }

  enabled         = true
  is_ipv6_enabled = true
  comment         = "Private CloudFront distribution for package assets with signed URLs"

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "S3-${aws_s3_bucket.package_assets.id}"
    compress               = true
    viewer_protocol_policy = "https-only"

    # Require signed URLs for all requests
    trusted_key_groups = [aws_cloudfront_key_group.package_assets.id]

    # Attach CORS response headers policy
    response_headers_policy_id = aws_cloudfront_response_headers_policy.package_assets_cors.id

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 86400   # 24 hours - good for data files that don't change often
    max_ttl     = 31536000 # 1 year
  }

  # Specific cache behavior for Parquet files with longer TTL
  ordered_cache_behavior {
    path_pattern           = "*.parquet"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "S3-${aws_s3_bucket.package_assets.id}"
    compress               = false  # Parquet files are already compressed
    viewer_protocol_policy = "https-only"
    trusted_key_groups     = [aws_cloudfront_key_group.package_assets.id]

    # Attach CORS response headers policy
    response_headers_policy_id = aws_cloudfront_response_headers_policy.package_assets_cors.id

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 2592000  # 30 days - Parquet files are typically immutable
    max_ttl     = 31536000 # 1 year
  }

  price_class = "PriceClass_100"

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  tags = merge(local.common_tags, {
    Name        = "package-assets-${var.environment_name}-cloudfront"
    Description = "Private CloudFront distribution for package assets S3 bucket"
    Service     = "packages-service"
  })

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  depends_on = [aws_s3_bucket.package_assets]
}

# S3 bucket policy to allow CloudFront access
resource "aws_s3_bucket_policy" "package_assets" {
  bucket = aws_s3_bucket.package_assets.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowCloudFrontServicePrincipal"
        Effect    = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.package_assets.arn}/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.package_assets.arn
          }
        }
      },
      {
        Sid       = "AllowAdminAccess"
        Effect    = "Allow"
        Principal = {
          AWS = [
            "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root",
            "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/admin"
          ]
        }
        Action   = "s3:*"
        Resource = [
          aws_s3_bucket.package_assets.arn,
          "${aws_s3_bucket.package_assets.arn}/*"
        ]
      }
    ]
  })

  depends_on = [aws_cloudfront_distribution.package_assets]
}

# Terraform outputs for CloudFront resources
output "cloudfront_key_group_id" {
  value       = aws_cloudfront_key_group.package_assets.id
  description = "CloudFront key group ID for package assets"
}