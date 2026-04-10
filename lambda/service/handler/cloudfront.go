package handler

import (
	"context"
	"crypto/md5"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/cloudfront/sign"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	log "github.com/sirupsen/logrus"
)

type CloudFrontSignedURLHandler struct {
	RequestHandler
}


type CloudFrontURLComponents struct {
	BaseURL    string      `json:"base_url"`
	Policy     string      `json:"policy"`
	Signature  string      `json:"signature"`
	KeyPairID  string      `json:"key_pair_id"`
	PolicyInfo *PolicyInfo `json:"policy_info,omitempty"`
}

// CloudFrontCookies returns Set-Cookie header values for the three CloudFront
// signed cookie fields. These cookies allow the browser to make credentialed
// requests to the CloudFront distribution (e.g. from Neuroglancer).
func (c *CloudFrontURLComponents) CloudFrontCookies(domain string) []string {
	attrs := fmt.Sprintf("; Domain=%s; Path=/; Secure; SameSite=None", domain)
	return []string{
		"CloudFront-Policy=" + c.Policy + attrs,
		"CloudFront-Signature=" + c.Signature + attrs,
		"CloudFront-Key-Pair-Id=" + c.KeyPairID + attrs,
	}
}

type PolicyInfo struct {
	ResourcePattern string `json:"resource_pattern"`
	ExpiresAt       int64  `json:"expires_at"`
	ExpiresAtISO    string `json:"expires_at_iso"`
}

type CloudFrontKeyPair struct {
	PrivateKey  string    `json:"privateKey"`
	PublicKey   string    `json:"publicKey"`
	KeyID       string    `json:"keyId"`
	CreatedAt   time.Time `json:"createdAt"`
	KeyGroupID  string    `json:"keyGroupId"`
	PublicKeyID string    `json:"publicKeyId"`
}

var (
	cloudfrontDistributionDomain string
	cloudfrontKeyID              string
	cloudfrontPrivateKey         *rsa.PrivateKey
	cloudfrontKeyPair            *CloudFrontKeyPair
)

func init() {
	// Initialize CloudFront distribution domain from environment variables
	// Note: CloudFront key ID is now loaded dynamically from Secrets Manager
	if domain, ok := os.LookupEnv("CLOUDFRONT_DISTRIBUTION_DOMAIN"); ok {
		cloudfrontDistributionDomain = domain
		log.Infof("CloudFront distribution domain initialized: %s", cloudfrontDistributionDomain)
	} else {
		log.Warn("CLOUDFRONT_DISTRIBUTION_DOMAIN environment variable not set")
	}
}

// generateCloudFrontSignedURLWithPolicy generates a signed URL with custom policy for prefix access
func (h *CloudFrontSignedURLHandler) generateCloudFrontSignedURLWithPolicy(s3Prefix string, optionalPath string) (string, time.Time, error) {
	// Get the CloudFront path prefix for this organization's bucket
	cloudfrontPathPrefix, err := h.getOrganizationCloudFrontPath(context.Background(), h.claims.OrgClaim.IntId)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to get organization CloudFront path: %w", err)
	}

	// Construct the resource pattern with wildcard for all files under the prefix
	// Include organization-specific CloudFront path if it exists
	var resourcePattern string
	if cloudfrontPathPrefix != "" {
		resourcePattern = fmt.Sprintf("https://%s%s/%s*", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix)
	} else {
		// Default path (main storage bucket)
		resourcePattern = fmt.Sprintf("https://%s/%s*", cloudfrontDistributionDomain, s3Prefix)
	}

	// Set expiration time (1 hour from now)
	expiresAt := time.Now().Add(1 * time.Hour)

	// Create custom policy that allows access to all files with the prefix
	policy := &sign.Policy{
		Statements: []sign.Statement{
			{
				Resource: resourcePattern,
				Condition: sign.Condition{
					DateLessThan: sign.NewAWSEpochTime(expiresAt),
				},
			},
		},
	}

	// Create the signer
	log.Infof("Creating CloudFront signer with Key ID: %s", cloudfrontKeyID)
	signer := sign.NewURLSigner(cloudfrontKeyID, cloudfrontPrivateKey)

	// Build the base URL - if optionalPath is provided, include it for user convenience
	// The policy still allows access to all files in the prefix
	var baseURL string
	if cloudfrontPathPrefix != "" {
		if optionalPath != "" {
			baseURL = fmt.Sprintf("https://%s%s/%s%s", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix, optionalPath)
		} else {
			// Return URL pointing to the prefix
			baseURL = fmt.Sprintf("https://%s%s/%s", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix)
		}
	} else {
		if optionalPath != "" {
			baseURL = fmt.Sprintf("https://%s/%s%s", cloudfrontDistributionDomain, s3Prefix, optionalPath)
		} else {
			// Return URL pointing to the prefix
			baseURL = fmt.Sprintf("https://%s/%s", cloudfrontDistributionDomain, s3Prefix)
		}
	}

	// Sign with the custom policy
	signedURL, err := signer.SignWithPolicy(baseURL, policy)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to sign URL with policy: %w", err)
	}

	h.logger.WithFields(log.Fields{
		"resourcePattern": resourcePattern,
		"baseURL":         baseURL,
		"expiresAt":       expiresAt,
	}).Debug("generated CloudFront signed URL with prefix policy")

	return signedURL, expiresAt, nil
}

// getOrganizationCloudFrontPath returns the CloudFront path prefix for the organization's bucket
func (h *CloudFrontSignedURLHandler) getOrganizationCloudFrontPath(ctx context.Context, orgId int64) (string, error) {
	// Query organization's storage bucket
	query := `SELECT storage_bucket FROM pennsieve.organizations WHERE id = $1`
	var bucketName sql.NullString
	err := PennsieveDB.QueryRowContext(ctx, query, orgId).Scan(&bucketName)
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("failed to query organization storage bucket: %w", err)
	}

	if bucketName.Valid && bucketName.String != "" {
		// Generate deterministic 8-character path from bucket name
		pathPrefix := generateDeterministicPath(bucketName.String)
		h.logger.WithFields(log.Fields{
			"orgId":      orgId,
			"bucketName": bucketName.String,
			"pathPrefix": pathPrefix,
		}).Debug("Generated CloudFront path prefix for organization bucket")
		return "/" + pathPrefix, nil
	} else {
		// Default: use main storage bucket (no prefix)
		h.logger.WithField("orgId", orgId).Debug("Using default storage bucket for organization")
		return "", nil
	}
}

// generateDeterministicPath creates a deterministic 8-character path from bucket name
func generateDeterministicPath(bucketName string) string {
	// Create MD5 hash of bucket name for deterministic result
	hash := md5.Sum([]byte(strings.ToLower(bucketName)))
	hexString := hex.EncodeToString(hash[:])

	// Take first 8 characters and ensure they're URL-safe
	pathID := hexString[:8]

	return pathID
}

// extractURLComponents parses a signed CloudFront URL and extracts its components
func (h *CloudFrontSignedURLHandler) extractURLComponents(signedURL string, expiresAt time.Time, s3Prefix string) (*CloudFrontURLComponents, error) {
	// Parse the URL to extract query parameters
	parts := strings.Split(signedURL, "?")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid signed URL format")
	}

	baseURL := parts[0]
	queryString := parts[1]

	// Parse query parameters
	queryParts := strings.Split(queryString, "&")
	var policy, signature, keyPairID string

	for _, part := range queryParts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}

		switch kv[0] {
		case "Policy":
			policy = kv[1]
		case "Signature":
			signature = kv[1]
		case "Key-Pair-Id":
			keyPairID = kv[1]
		}
	}

	// Extract base URL without the specific file (if any)
	// Find the CloudFront domain and path prefix, then add the S3 prefix
	var extractedBaseURL string
	cloudfrontPathPrefix, err := h.getOrganizationCloudFrontPath(context.Background(), h.claims.OrgClaim.IntId)
	if err == nil {
		if cloudfrontPathPrefix != "" {
			extractedBaseURL = fmt.Sprintf("https://%s%s/%s", cloudfrontDistributionDomain, cloudfrontPathPrefix, s3Prefix)
		} else {
			extractedBaseURL = fmt.Sprintf("https://%s/%s", cloudfrontDistributionDomain, s3Prefix)
		}
	} else {
		// Fallback: extract from the actual URL
		if idx := strings.LastIndex(baseURL, "/"); idx != -1 {
			extractedBaseURL = baseURL[:idx+1]
		} else {
			extractedBaseURL = baseURL
		}
	}

	components := &CloudFrontURLComponents{
		BaseURL:   extractedBaseURL,
		Policy:    policy,
		Signature: signature,
		KeyPairID: keyPairID,
	}

	// Extract policy information if policy is available
	if policy != "" {
		policyInfo, err := h.extractPolicyInfo(policy, expiresAt)
		if err == nil {
			components.PolicyInfo = policyInfo
		}
	}

	return components, nil
}

// extractPolicyInfo decodes and extracts useful information from the base64-encoded policy
func (h *CloudFrontSignedURLHandler) extractPolicyInfo(encodedPolicy string, expiresAt time.Time) (*PolicyInfo, error) {
	// Decode base64 policy
	decodedPolicy, err := base64.StdEncoding.DecodeString(encodedPolicy)
	if err != nil {
		// Try URL-safe base64 decoding
		decodedPolicy, err = base64.URLEncoding.DecodeString(encodedPolicy)
		if err != nil {
			return nil, fmt.Errorf("failed to decode policy from base64: %w", err)
		}
	}

	// Parse JSON policy
	var policy struct {
		Statement []struct {
			Resource  string `json:"Resource"`
			Condition struct {
				DateLessThan struct {
					AwsEpochTime int64 `json:"AWS:EpochTime"`
				} `json:"DateLessThan"`
			} `json:"Condition"`
		} `json:"Statement"`
	}

	if err := json.Unmarshal(decodedPolicy, &policy); err != nil {
		return nil, fmt.Errorf("failed to parse policy JSON: %w", err)
	}

	// Extract information from first statement
	if len(policy.Statement) == 0 {
		return nil, fmt.Errorf("policy has no statements")
	}

	statement := policy.Statement[0]
	policyExpiresAt := statement.Condition.DateLessThan.AwsEpochTime

	return &PolicyInfo{
		ResourcePattern: statement.Resource,
		ExpiresAt:       policyExpiresAt,
		ExpiresAtISO:    time.Unix(policyExpiresAt, 0).UTC().Format(time.RFC3339),
	}, nil
}

func (h *CloudFrontSignedURLHandler) loadKeysFromSecretsManager(ctx context.Context, secretName string) error {
	log.Infof("Loading CloudFront keys from Secrets Manager: %s", secretName)

	// Create AWS config with explicit region
	region := os.Getenv("REGION")
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = "us-east-1" // fallback
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	log.Infof("AWS config loaded with region: %s", cfg.Region)

	// Create Secrets Manager client
	smClient := secretsmanager.NewFromConfig(cfg)

	// Get secret value
	result, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	if err != nil {
		return fmt.Errorf("failed to get CloudFront keys from Secrets Manager: %w", err)
	}

	if result.SecretString == nil {
		return fmt.Errorf("secret value is nil")
	}

	// Parse the key pair from JSON
	var keyPair CloudFrontKeyPair
	if err := json.Unmarshal([]byte(*result.SecretString), &keyPair); err != nil {
		return fmt.Errorf("failed to parse CloudFront key pair: %w", err)
	}

	cloudfrontKeyPair = &keyPair
	log.Infof("Loaded CloudFront key pair with ID: %s, created at: %s", keyPair.KeyID, keyPair.CreatedAt)

	// Decode base64 private key
	keyBytes, err := base64.StdEncoding.DecodeString(keyPair.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to decode CloudFront private key from base64: %w", err)
	}

	// Parse PEM block
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return fmt.Errorf("failed to parse PEM block from private key")
	}

	// Parse RSA private key
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse RSA private key: %w", err)
	}

	cloudfrontPrivateKey = privateKey
	cloudfrontKeyID = keyPair.PublicKeyID // Use the CloudFront public key ID for signing

	log.Infof("Successfully loaded CloudFront private key (Public Key ID: %s)", cloudfrontKeyID)
	log.Infof("CloudFront key pair details - KeyID: %s, PublicKeyID: %s, KeyGroupID: %s, CreatedAt: %s",
		keyPair.KeyID, keyPair.PublicKeyID, keyPair.KeyGroupID, keyPair.CreatedAt)
	return nil
}
