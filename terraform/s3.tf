# S3 bucket for package viewer assets
resource "aws_s3_bucket" "package_assets" {
  bucket = "pennsieve-${var.environment_name}-package-assets-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  tags = merge(local.common_tags, {
    Name        = "pennsieve-${var.environment_name}-package-assets-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    Description = "S3 bucket for package viewer assets"
    Service     = "packages-service"
  })
}

# Block all public access to the bucket
resource "aws_s3_bucket_public_access_block" "package_assets" {
  bucket = aws_s3_bucket.package_assets.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning on the bucket
resource "aws_s3_bucket_versioning" "package_assets" {
  bucket = aws_s3_bucket.package_assets.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Configure server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "package_assets" {
  bucket = aws_s3_bucket.package_assets.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Configure lifecycle policy to manage object versions, cleanup, and intelligent tiering
resource "aws_s3_bucket_lifecycle_configuration" "package_assets" {
  bucket = aws_s3_bucket.package_assets.id

  rule {
    id     = "intelligent_tiering_and_cleanup"
    status = "Enabled"

    # Transition to Intelligent Tiering immediately
    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# CORS configuration to allow access from web applications
resource "aws_s3_bucket_cors_configuration" "package_assets" {
  bucket = aws_s3_bucket.package_assets.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = local.cors_allowed_origins
    expose_headers  = ["Content-Length", "Content-Type", "Content-Range", "ETag", "Last-Modified", "Accept-Ranges"]
    max_age_seconds = 3600
  }
}

# CloudFront Origin Access Control for the S3 bucket
resource "aws_cloudfront_origin_access_control" "package_assets" {
  name                              = "package-assets-${var.environment_name}-oac"
  description                       = "OAC for package assets S3 bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}



# CloudFront key group for signed URLs
resource "aws_cloudfront_public_key" "package_assets" {
  comment     = "Public key for package assets CloudFront signed URLs"
  encoded_key = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx8wKG0QMpBBVr+fLLkoV\ndummy-key-replace-in-console\n-----END PUBLIC KEY-----"
  name        = "package-assets-${var.environment_name}-public-key"
  
  lifecycle {
    ignore_changes = [encoded_key]
  }
}

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

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 3600
    max_ttl     = 86400
  }

  # Cache behavior for static assets with longer TTL
  ordered_cache_behavior {
    path_pattern           = "*.css"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "S3-${aws_s3_bucket.package_assets.id}"
    compress               = true
    viewer_protocol_policy = "https-only"
    trusted_key_groups     = [aws_cloudfront_key_group.package_assets.id]

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 31536000  # 1 year
    max_ttl     = 31536000  # 1 year
  }

  ordered_cache_behavior {
    path_pattern           = "*.js"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "S3-${aws_s3_bucket.package_assets.id}"
    compress               = true
    viewer_protocol_policy = "https-only"
    trusted_key_groups     = [aws_cloudfront_key_group.package_assets.id]

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 31536000  # 1 year
    max_ttl     = 31536000  # 1 year
  }

  ordered_cache_behavior {
    path_pattern           = "*.png"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "S3-${aws_s3_bucket.package_assets.id}"
    compress               = false
    viewer_protocol_policy = "https-only"
    trusted_key_groups     = [aws_cloudfront_key_group.package_assets.id]

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 31536000  # 1 year
    max_ttl     = 31536000  # 1 year
  }

  ordered_cache_behavior {
    path_pattern           = "*.jpg"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "S3-${aws_s3_bucket.package_assets.id}"
    compress               = false
    viewer_protocol_policy = "https-only"
    trusted_key_groups     = [aws_cloudfront_key_group.package_assets.id]

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 31536000  # 1 year
    max_ttl     = 31536000  # 1 year
  }

  ordered_cache_behavior {
    path_pattern           = "*.jpeg"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "S3-${aws_s3_bucket.package_assets.id}"
    compress               = false
    viewer_protocol_policy = "https-only"
    trusted_key_groups     = [aws_cloudfront_key_group.package_assets.id]

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    min_ttl     = 0
    default_ttl = 31536000  # 1 year
    max_ttl     = 31536000  # 1 year
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

# S3 bucket policy to allow only CloudFront access
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
        Sid       = "DenyDirectS3Access"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.package_assets.arn,
          "${aws_s3_bucket.package_assets.arn}/*"
        ]
        Condition = {
          StringNotEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.package_assets.arn
          }
        }
      }
    ]
  })

  depends_on = [aws_cloudfront_distribution.package_assets]
}