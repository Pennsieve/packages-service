# S3 bucket for package viewer assets
resource "aws_s3_bucket" "package_assets" {
  bucket = "pennsieve-${var.environment_name}-pkg-assets-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  tags = merge(local.common_tags, {
    Name        = "pennsieve-${var.environment_name}-pkg-assets-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    Description = "S3 bucket for package assets"
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
