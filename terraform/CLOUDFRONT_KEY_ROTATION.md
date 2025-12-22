# CloudFront Signed URLs - Automated Key Rotation

This document describes the improved CloudFront signed URL key management system using AWS Secrets Manager with automated rotation.

## Overview

The new implementation replaces the manual key generation script with an automated AWS-native solution that provides:
- **Automatic key rotation** every 30 days
- **Zero-downtime rotation** using CloudFront key groups
- **Secure key storage** in AWS Secrets Manager
- **Audit trails** for all key operations
- **Backward compatibility** with existing SSM-based keys

## Architecture

### Components

1. **AWS Secrets Manager**: Stores the CloudFront signing key pair with automatic rotation
2. **Lambda Rotation Function**: Handles the key rotation process
3. **CloudFront Key Group**: Supports multiple active keys during rotation
4. **Service Lambda**: Uses Secrets Manager (with SSM fallback) to retrieve keys

### Key Rotation Process

The rotation follows AWS Secrets Manager's standard four-step process:

1. **createSecret**: Generate new RSA 2048-bit key pair
2. **setSecret**: Upload public key to CloudFront and update key group
3. **testSecret**: Validate the new key pair
4. **finishSecret**: Promote the new key to AWSCURRENT

## Implementation Details

### Terraform Resources

#### Secrets Manager Configuration
- **Secret**: `{environment}-{service_name}-cloudfront-signing-keys`
- **Rotation Schedule**: Every 30 days (configurable)
- **Recovery Window**: 7 days

#### Lambda Rotation Function
- **Function**: `{environment}-{service_name}-key-rotation`
- **Runtime**: Go on provided.al2
- **Permissions**: 
  - Secrets Manager operations
  - CloudFront public key and key group management
  - KMS for encryption

#### CloudFront Configuration
- **Initial Public Key**: Created from dummy keys for CI/CD
- **Key Group**: Supports multiple keys with lifecycle ignore on items
- **Distribution**: Uses trusted key groups for signed URLs

### Key Format

The secret stores a JSON object with the following structure:

```json
{
  "privateKey": "base64-encoded-PEM-private-key",
  "publicKey": "PEM-formatted-public-key",
  "keyId": "unique-key-identifier",
  "createdAt": "2024-01-01T00:00:00Z",
  "keyGroupId": "cloudfront-key-group-id",
  "publicKeyId": "cloudfront-public-key-id"
}
```

## Deployment Instructions

### Initial Deployment

1. **Deploy Terraform**:
   ```bash
   terraform apply
   ```
   This creates:
   - Secrets Manager secret with initial dummy keys
   - Lambda rotation function
   - CloudFront resources with initial key configuration

2. **Trigger Initial Rotation** (Optional):
   ```bash
   aws secretsmanager rotate-secret \
     --secret-id {environment}-{service_name}-cloudfront-signing-keys \
     --region {region}
   ```

3. **Verify Rotation**:
   ```bash
   aws secretsmanager describe-secret \
     --secret-id {environment}-{service_name}-cloudfront-signing-keys \
     --region {region}
   ```

### Manual Rotation (if needed)

To manually trigger rotation:
```bash
aws secretsmanager rotate-secret \
  --secret-id {environment}-{service_name}-cloudfront-signing-keys
```

## Key Rotation Timeline

During rotation, the system maintains multiple active keys:

1. **Day 0**: Current key (K1) is active
2. **Day 30**: Rotation triggered
   - New key (K2) generated and added to key group
   - Both K1 and K2 are active
3. **Day 30-31**: Transition period
   - New signed URLs use K2
   - Existing signed URLs with K1 continue to work
4. **After maximum URL lifetime** (e.g., 1 hour):
   - K1 can be safely removed from key group
   - Only K2 remains active

## Monitoring and Alerts

### CloudWatch Alarms

The system includes three essential CloudWatch alarms that send notifications to VictorOps/PagerDuty:

1. **Key Rotation Lambda Errors** (`{environment}-{service_name}-key-rotation-errors`)
   - Triggers: Any error in the rotation Lambda function
   - Evaluation: 1 error within 5 minutes
   - Action: Immediate alert to investigate rotation issues

2. **Secrets Rotation Failed** (`{environment}-{service_name}-secrets-rotation-failed`)
   - Triggers: When rotation doesn't succeed within 24 hours
   - Evaluation: Checks daily rotation success rate
   - Action: Alert when automatic rotation fails

3. **Service Lambda Key Errors** (`{environment}-{service_name}-lambda-key-errors`)
   - Triggers: Sustained errors in service Lambda (likely key loading issues)
   - Evaluation: More than 25 errors in 3 consecutive 5-minute periods
   - Threshold: Set high to avoid false positives from transient errors
   - Action: Alert on persistent key retrieval problems

### Alert Configuration

All alarms are configured to send notifications to the existing VictorOps/PagerDuty integration:
- Uses `data.terraform_remote_state.account.outputs.ops_victor_ops_sns_topic_arn`
- Integrates with existing on-call rotation
- Provides both alarm and OK state notifications for tracking resolution

### CloudWatch Metrics Monitored
- `AWS/Lambda/Errors` - Lambda function errors
- `AWS/Lambda/Duration` - Function execution time
- `AWS/Lambda/Throttles` - Rate limiting issues
- `AWS/SecretsManager/RotationSucceeded` - Rotation success rate
- `AWS/SecretsManager/RotationFailed` - Rotation failures

## Security Considerations

1. **Private Key Protection**:
   - Never exposed in environment variables
   - Encrypted at rest using KMS
   - Retrieved only during Lambda cold starts

2. **Access Control**:
   - Lambda has minimal required permissions
   - Secrets Manager resource-based policies
   - CloudFront operations limited to specific resources

3. **Audit Trail**:
   - All Secrets Manager operations logged in CloudTrail
   - Lambda function logs in CloudWatch
   - CloudFront API calls tracked

## Rollback Procedures

If issues occur with the new key:

1. **Revert in Secrets Manager**:
   ```bash
   aws secretsmanager update-secret-version-stage \
     --secret-id {secret-id} \
     --version-stage AWSCURRENT \
     --move-to-version-id {previous-version-id}
   ```

2. **Remove problematic key from CloudFront**:
   - Identify the problematic public key ID
   - Remove from key group via CloudFront console or API
   - Existing signed URLs with good keys continue working

## Migration from SSM

The system maintains backward compatibility during migration:

1. **Environment Variables**:
   - New: `CLOUDFRONT_SIGNING_KEYS_SECRET_NAME`
   - Legacy: `CLOUDFRONT_PRIVATE_KEY_SSM_PARAM` (fallback)

2. **Migration Steps**:
   1. Deploy new infrastructure (Secrets Manager, rotation Lambda)
   2. Update Lambda environment variables
   3. Test with Secrets Manager
   4. Remove SSM parameters once confirmed working
   5. Clean up SSM-related Terraform resources

## Comparison with Previous Implementation

| Aspect | Old (SSM + Script) | New (Secrets Manager) |
|---|---|---|
| Key Generation | Manual script | Automated Lambda |
| Storage | SSM Parameter Store | Secrets Manager |
| Rotation | Manual process | Automated every 30 days |
| Downtime | Potential during rotation | Zero-downtime |
| Key Format | Base64 in SSM | JSON with metadata |
| Audit | Limited | Full CloudTrail logging |
| Cost | Lower | Slightly higher (~$0.40/month) |

## Troubleshooting

### Common Issues

1. **Rotation Fails**:
   - Check Lambda logs in CloudWatch
   - Verify IAM permissions
   - Ensure CloudFront quotas not exceeded

2. **Signed URLs Not Working**:
   - Verify key is in CloudFront key group
   - Check Lambda is using correct key ID
   - Ensure URL expiration time is valid

3. **Lambda Timeout**:
   - Increase timeout in Terraform (default: 60s)
   - Check network connectivity if in VPC

### Debug Commands

```bash
# Check current secret value
aws secretsmanager get-secret-value \
  --secret-id {secret-id} \
  --query SecretString | jq -r | jq

# List CloudFront public keys
aws cloudfront list-public-keys

# Get key group details
aws cloudfront get-key-group --id {key-group-id}

# View Lambda logs
aws logs tail /aws/lambda/{function-name} --follow
```

## Cost Implications

Monthly costs (approximate):
- Secrets Manager: $0.40 per secret
- Lambda executions: Negligible (12 rotations/year)
- CloudFront: No additional cost for key groups
- Total: ~$0.40/month per environment

## Future Enhancements

Potential improvements:
1. SNS notifications on rotation events
2. Automatic cleanup of old CloudFront public keys
3. Multi-region secret replication
4. Custom rotation schedules based on environment
5. Integration with AWS Systems Manager for compliance reporting