# CloudFront Viewer Assets Configuration Guide

## Overview
This Terraform configuration creates a CloudFront distribution that can serve content from multiple S3 buckets, including cross-account buckets. Each bucket's content is accessible via a specific URL path in CloudFront.

## Configuration

### 1. Configure the Buckets in Terraform
Add your viewer asset buckets to your `terraform.tfvars`:

```hcl
viewer_asset_buckets = [
  {
    bucket_name     = "company-viewer-assets-1"
    bucket_region   = "us-east-1"
    cloudfront_path = "/viewer1"        # Accessible at: https://cloudfront.net/viewer1/*
    s3_prefix       = "/viewer_assets"  # S3 prefix where assets are stored
  },
  {
    bucket_name     = "company-viewer-assets-2"
    bucket_region   = "us-west-2"
    cloudfront_path = "/viewer2"        # Accessible at: https://cloudfront.net/viewer2/*
    s3_prefix       = "/viewer_assets"  # S3 prefix where assets are stored
  }
]
```

### 2. Deploy the CloudFront Distribution
```bash
terraform plan
terraform apply
```

### 3. Apply Bucket Policies
After deployment, Terraform will output the required bucket policies in `viewer_assets_bucket_policies`. 

**IMPORTANT**: These policies must be manually applied to each S3 bucket, especially for cross-account buckets.

#### For Cross-Account Buckets:
The bucket owner must:
1. Get the policy from Terraform output: `terraform output -json viewer_assets_bucket_policies`
2. Extract the `policy_statement` for their bucket
3. **Merge** it with existing bucket policy (don't replace existing statements)
4. Apply the updated policy

#### For Same-Account Buckets:
You can apply the policy using AWS CLI:
```bash
# Get the policy for a specific bucket
terraform output -json viewer_assets_bucket_policies | jq '."bucket-name".full_policy' > policy.json

# Apply it (WARNING: This replaces the entire bucket policy)
aws s3api put-bucket-policy --bucket bucket-name --policy file://policy.json
```

To merge with existing policy:
```bash
# Get existing policy
aws s3api get-bucket-policy --bucket bucket-name --query Policy --output text | jq . > existing-policy.json

# Manually merge the statements and apply
# Or use a script to merge JSON policies
```

## How It Works

1. **Multiple Origins**: Each S3 bucket is configured as a separate origin in CloudFront
2. **Path-Based Routing**: CloudFront routes requests based on the URL path:
   - `/viewer1/*` → `s3://company-viewer-assets-1/viewer_assets/*`
   - `/viewer2/*` → `s3://company-viewer-assets-2/viewer_assets/*`
3. **Security**: All requests require signed URLs (using CloudFront key groups)
4. **Origin Access Control**: CloudFront uses OAC (Origin Access Control) for secure S3 access

## Important Notes

- **Bucket Policy Required**: Each S3 bucket MUST have the CloudFront access policy applied
- **Cross-Account**: For buckets in different AWS accounts, policies must be applied by the bucket owner
- **Policy Merging**: Always merge with existing policies to avoid breaking other access patterns
- **Signed URLs**: All content requires CloudFront signed URLs for access
- **CORS**: CORS headers are configured at the CloudFront level

## Troubleshooting

### 403 Forbidden Errors
- Verify the bucket policy has been applied
- Check the CloudFront distribution ARN in the policy matches the actual distribution
- Ensure the S3 prefix in the policy matches your configuration

### Cross-Account Issues
- Confirm the bucket owner has applied the policy
- Verify the CloudFront distribution ARN is correct
- Check that the bucket region matches the configuration

### Testing Access
```bash
# After setting up signed URL generation
curl -I "https://YOUR_CLOUDFRONT_DOMAIN/viewer1/test-file.jpg?signature=..."
```