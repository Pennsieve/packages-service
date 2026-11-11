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
	"log/slog"

	"github.com/pennsieve/packages-service/api/logging"
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
		slog.Info("CloudFront distribution domain initialized",
			slog.String(logging.KeyCloudFrontDomain, cloudfrontDistributionDomain))
	} else {
		slog.Warn("CLOUDFRONT_DISTRIBUTION_DOMAIN environment variable not set")
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
	h.logger.LogDebugWithFields(logging.Fields{logging.KeyCloudFrontKeyID: cloudfrontKeyID},
		"creating CloudFront signer")
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

	h.logger.LogDebugWithFields(logging.Fields{
		logging.KeyResourcePattern: resourcePattern,
		logging.KeyBaseURL:         baseURL,
		logging.KeyExpiresAt:       expiresAt,
	}, "generated CloudFront signed URL with prefix policy")

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
		h.logger.LogDebugWithFields(logging.Fields{
			logging.KeyOrganizationID: orgId,
			logging.KeyS3Bucket:       bucketName.String,
			logging.KeyPathPrefix:     pathPrefix,
		}, "generated CloudFront path prefix for organization bucket")
		return "/" + pathPrefix, nil
	} else {
		// Default: use main storage bucket (no prefix)
		h.logger.LogDebugWithFields(logging.Fields{logging.KeyOrganizationID: orgId},
			"using default storage bucket for organization")
		return "", nil
	}
}

// cloudFrontPathPrefixForBucket returns the CloudFront path prefix that routes to
// the given bucket on the package-assets distribution: "" for the default origin
// (ViewerAssetsBucket, the main storage bucket) and "/{8-char hash}" for every
// other bucket, which the distribution serves through a per-bucket cache behavior.
func cloudFrontPathPrefixForBucket(bucketName string) string {
	if bucketName == "" || strings.EqualFold(bucketName, ViewerAssetsBucket) {
		return ""
	}
	return "/" + generateDeterministicPath(bucketName)
}

// viewerAssetURL returns the CloudFront base URL of a viewer asset stored in
// bucketName, or "" when CloudFront is not configured. The bucket is the one
// recorded on the asset, not the organization's current storage bucket: an
// organization's storage bucket can change while its existing assets stay put.
func viewerAssetURL(orgID, datasetID int64, assetID, bucketName string) string {
	if cloudfrontDistributionDomain == "" {
		return ""
	}
	return fmt.Sprintf("https://%s%s/O%d/D%d/%s/",
		cloudfrontDistributionDomain, cloudFrontPathPrefixForBucket(bucketName), orgID, datasetID, assetID)
}

// defaultViewerAssetPathPrefix returns the path prefix for the bucket new viewer
// assets of the organization go to (see resolveStorageBucket). Used to scope
// signing when there are no assets to go by. On error it also returns the
// default bucket's prefix, "", so callers can log and carry on.
func defaultViewerAssetPathPrefix(ctx context.Context, orgID int64) (string, error) {
	bucketName, err := resolveStorageBucket(ctx, orgID)
	if err != nil {
		return "", err
	}
	return cloudFrontPathPrefixForBucket(bucketName), nil
}

// viewerAssetsResourcePattern returns the signed-policy resource for a dataset's
// viewer assets under the given CloudFront path prefixes. With a single prefix
// it is that prefix's O{org}/D{dataset}/ path. When a dataset's assets live in
// more than one bucket (e.g. after its organization's storage bucket changed),
// a CloudFront policy can only hold one statement, so the pattern instead
// matches O{org}/D{dataset}/ under any prefix.
func viewerAssetsResourcePattern(orgID, datasetID int64, pathPrefixes []string) string {
	s3Prefix := fmt.Sprintf("O%d/D%d/", orgID, datasetID)
	distinct := map[string]bool{}
	for _, p := range pathPrefixes {
		distinct[p] = true
	}
	if len(distinct) == 1 {
		return fmt.Sprintf("https://%s%s/%s*", cloudfrontDistributionDomain, pathPrefixes[0], s3Prefix)
	}
	return fmt.Sprintf("https://%s/*%s*", cloudfrontDistributionDomain, s3Prefix)
}

// signViewerAssetsPolicy signs a one-hour CloudFront policy covering a dataset's
// viewer assets under the given path prefixes (at least one; the first one is
// used for BaseURL). The signing keys must already be loaded.
func signViewerAssetsPolicy(orgID, datasetID int64, pathPrefixes []string) (*CloudFrontURLComponents, error) {
	if cloudfrontPrivateKey == nil {
		return nil, fmt.Errorf("CloudFront signing keys not loaded")
	}
	if len(pathPrefixes) == 0 {
		return nil, fmt.Errorf("no CloudFront path prefix to sign for")
	}

	resourcePattern := viewerAssetsResourcePattern(orgID, datasetID, pathPrefixes)
	expiresAt := time.Now().Add(1 * time.Hour)
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

	baseURL := fmt.Sprintf("https://%s%s/O%d/D%d/", cloudfrontDistributionDomain, pathPrefixes[0], orgID, datasetID)
	signedURL, err := sign.NewURLSigner(cloudfrontKeyID, cloudfrontPrivateKey).SignWithPolicy(baseURL, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to sign URL with policy: %w", err)
	}

	parts := strings.SplitN(signedURL, "?", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid signed URL format")
	}
	components := &CloudFrontURLComponents{BaseURL: baseURL}
	for _, part := range strings.Split(parts[1], "&") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "Policy":
			components.Policy = kv[1]
		case "Signature":
			components.Signature = kv[1]
		case "Key-Pair-Id":
			components.KeyPairID = kv[1]
		}
	}
	// Built from the inputs rather than decoded from the signed policy: CloudFront's
	// policy encoding ('~' for '/') is not standard or URL-safe base64.
	components.PolicyInfo = &PolicyInfo{
		ResourcePattern: resourcePattern,
		ExpiresAt:       expiresAt.Unix(),
		ExpiresAtISO:    time.Unix(expiresAt.Unix(), 0).UTC().Format(time.RFC3339),
	}
	return components, nil
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
	h.logger.LogInfoWithFields(logging.Fields{logging.KeySecretName: secretName},
		"loading CloudFront keys from Secrets Manager")

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

	h.logger.LogDebugWithFields(logging.Fields{logging.KeyRegion: cfg.Region}, "AWS config loaded")

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
	h.logger.LogInfoWithFields(logging.Fields{
		logging.KeyCloudFrontKeyID: keyPair.KeyID,
		logging.KeyCreatedAt:       keyPair.CreatedAt,
	}, "loaded CloudFront key pair")

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

	h.logger.LogInfoWithFields(logging.Fields{
		logging.KeyCloudFrontKeyID:       keyPair.KeyID,
		logging.KeyCloudFrontPublicKeyID: keyPair.PublicKeyID,
		logging.KeyCloudFrontKeyGroupID:  keyPair.KeyGroupID,
		logging.KeyCreatedAt:             keyPair.CreatedAt,
	}, "successfully loaded CloudFront private key")
	return nil
}
