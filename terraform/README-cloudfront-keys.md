# CloudFront Signed URLs Key Generation

This document explains how to generate the RSA key pair required for CloudFront signed URLs and store them in AWS SSM Parameter Store.

## Generate RSA Key Pair

Run the following commands to generate the private and public keys:

```bash
# Generate private key (keep this secure!)
openssl genrsa -out cloudfront-private-key.pem 2048

# Generate public key from private key
openssl rsa -pubout -in cloudfront-private-key.pem -out cloudfront-public-key.pem
```

## Storing Keys for Terraform

The keys are stored in AWS SSM Parameter Store and referenced by Terraform:

### 1. Base64 encode the private key:
```bash
base64 cloudfront-private-key.pem | tr -d '\n' > cloudfront-private-key.b64
```

### 2. Set Terraform variables:

You can either:
- Pass the keys as Terraform variables:
```bash
terraform apply \
  -var="cloudfront_private_key=$(cat cloudfront-private-key.b64)" \
  -var="cloudfront_public_key=$(cat cloudfront-public-key.pem)"
```

- Or create a `terraform.tfvars` file:
```hcl
cloudfront_private_key = "base64-encoded-private-key-here"
cloudfront_public_key = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
-----END PUBLIC KEY-----"
```

## How it Works

1. **Terraform creates SSM parameters**: The keys are stored in SSM Parameter Store at:
   - Private key: `/{environment}/{service_name}/cloudfront/private-key` (SecureString)
   - Public key: `/{environment}/{service_name}/cloudfront/public-key` (String)

2. **Lambda fetches from SSM**: During Lambda initialization (not on every invocation), the function fetches the private key from SSM using the parameter name passed via environment variable.

3. **CloudFront uses public key**: The public key is uploaded to CloudFront to verify signed URLs.

## Architecture Benefits

- **Security**: Private key is never exposed in environment variables, only the SSM parameter name
- **Rotation**: Keys can be rotated by updating SSM parameters without redeploying Lambda
- **Performance**: Key is fetched once during Lambda cold start, not on every request
- **Compliance**: Meets security best practices for secrets management

## Security Notes

- Never commit the private key to version control
- The private key in SSM is encrypted with KMS
- Lambda IAM role only has access to specific SSM parameters
- Rotate keys regularly for security
- The public key can be stored in version control as it's not sensitive