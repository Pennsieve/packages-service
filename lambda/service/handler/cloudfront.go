package handler

import (
	"bytes"
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
	"github.com/aws/aws-sdk-go-v2/aws"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/cloudfront/sign"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	log "github.com/sirupsen/logrus"
)

type CloudFrontSignedURLHandler struct {
	RequestHandler
}

type CloudFrontSignedURLResponse struct {
	SignedURL  string                   `json:"signed_url"`
	ExpiresAt  int64                    `json:"expires_at"` // Unix timestamp
	Components *CloudFrontURLComponents `json:"components,omitempty"`
}

type CloudFrontURLComponents struct {
	BaseURL    string      `json:"base_url"`
	Policy     string      `json:"policy"`
	Signature  string      `json:"signature"`
	KeyPairID  string      `json:"key_pair_id"`
	PolicyInfo *PolicyInfo `json:"policy_info,omitempty"`
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

func (h *CloudFrontSignedURLHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	switch h.method {
	case http.MethodGet:
		return h.handleGet(ctx)
	case http.MethodOptions:
		return h.handleOptions(ctx)
	default:
		return h.logAndBuildError(fmt.Sprintf("method %s not allowed", h.method), http.StatusMethodNotAllowed), nil
	}
}

func (h *CloudFrontSignedURLHandler) handleOptions(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	h.logger.Info("handling OPTIONS request for CloudFront signed URL")

	headers := map[string]string{
		"Access-Control-Allow-Origin":  "*",
		"Access-Control-Allow-Methods": "GET, OPTIONS",
		"Access-Control-Allow-Headers": "Authorization, Content-Type, Origin, Accept",
		"Access-Control-Max-Age":       "3600",
	}

	return &events.APIGatewayV2HTTPResponse{
		StatusCode: http.StatusNoContent,
		Headers:    headers,
	}, nil
}

func (h *CloudFrontSignedURLHandler) handleGet(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	// Load keys from Secrets Manager only if not already cached
	// With grace period, cached keys remain valid even after rotation
	if cloudfrontKeyID == "" || cloudfrontPrivateKey == nil {
		secretName, ok := os.LookupEnv("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME")
		if !ok {
			log.Error("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME environment variable not set")
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		if err := h.loadKeysFromSecretsManager(ctx, secretName); err != nil {
			log.Errorf("Failed to load keys from Secrets Manager: %v", err)
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		log.Info("CloudFront keys loaded and cached for this Lambda container")
	} else {
		log.Debug("Using cached CloudFront keys")
	}

	if cloudfrontDistributionDomain == "" || cloudfrontKeyID == "" || cloudfrontPrivateKey == nil {
		return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
	}

	// Get parameters from query string
	datasetID := h.queryParams["dataset_id"]
	packageID := h.queryParams["package_id"]
	// Note: path is now optional - if provided, it will be appended to the URL for user convenience
	// Trim any whitespace from the path to avoid issues with trailing spaces
	path := strings.TrimSpace(h.queryParams["path"])
	// Check if client wants URL components included in response
	includeComponents := h.queryParams["include_components"] == "true"

	// Validate required parameters
	if datasetID == "" {
		return h.logAndBuildError("missing required 'dataset_id' query parameter", http.StatusBadRequest), nil
	}
	if packageID == "" {
		return h.logAndBuildError("missing required 'package_id' query parameter", http.StatusBadRequest), nil
	}

	h.logger.WithFields(log.Fields{
		"packageId": packageID,
		"datasetId": datasetID,
		"assetPath": path,
	}).Info("handling GET request for CloudFront signed URL with prefix access")

	// Get the S3 prefix for the package
	s3Prefix, err := h.getS3PrefixForPackage(ctx, packageID, datasetID)

	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to get S3 prefix: %v", err), http.StatusInternalServerError), nil
	}

	// Generate CloudFront signed URL with custom policy for prefix access
	signedURL, expiresAt, err := h.generateCloudFrontSignedURLWithPolicy(s3Prefix, path)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to generate signed URL: %v", err), http.StatusInternalServerError), nil
	}

	// Build response
	response := CloudFrontSignedURLResponse{
		SignedURL: signedURL,
		ExpiresAt: expiresAt.Unix(),
	}

	// Extract and include URL components if requested
	if includeComponents {
		components, err := h.extractURLComponents(signedURL, expiresAt, s3Prefix)
		if err != nil {
			h.logger.WithError(err).Warn("Failed to extract URL components, continuing without them")
		} else {
			response.Components = components
		}
	}

	// Use custom encoder to avoid escaping HTML characters like &
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(response)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to marshal response: %v", err), http.StatusInternalServerError), nil
	}
	responseBody := buf.Bytes()
	// Remove trailing newline added by encoder
	if len(responseBody) > 0 && responseBody[len(responseBody)-1] == '\n' {
		responseBody = responseBody[:len(responseBody)-1]
	}

	// Build response headers with CORS
	headers := map[string]string{
		"Content-Type":                  "application/json",
		"Access-Control-Allow-Origin":   "*",
		"Access-Control-Allow-Methods":  "GET, OPTIONS",
		"Access-Control-Allow-Headers":  "Authorization, Content-Type, Origin, Accept",
		"Access-Control-Expose-Headers": "Content-Type",
	}

	h.logger.WithFields(log.Fields{
		"signedURL": signedURL,
		"expiresAt": expiresAt,
		"packageId": packageID,
		"datasetId": datasetID,
	}).Debug("returning CloudFront signed URL")

	return &events.APIGatewayV2HTTPResponse{
		StatusCode: http.StatusOK,
		Headers:    headers,
		Body:       string(responseBody),
	}, nil
}

// getS3PrefixForPackage validates and constructs the S3 prefix for all assets in a package
func (h *CloudFrontSignedURLHandler) getS3PrefixForPackage(ctx context.Context, packageNodeId, datasetNodeId string) (string, error) {
	// Query to get the internal integer IDs and validate that the package belongs to the dataset
	query := fmt.Sprintf(`
		SELECT p.id, d.id
		FROM "%d".packages p 
		JOIN "%d".datasets d ON p.dataset_id = d.id 
		WHERE p.node_id = $1 AND d.node_id = $2
	`, h.claims.OrgClaim.IntId, h.claims.OrgClaim.IntId)

	var packageIntId, datasetIntId int64
	err := PennsieveDB.QueryRowContext(ctx, query, packageNodeId, datasetNodeId).Scan(&packageIntId, &datasetIntId)
	if err != nil {
		h.logger.WithError(err).WithFields(map[string]interface{}{
			"packageNodeId": packageNodeId,
			"datasetNodeId": datasetNodeId,
		}).Error("failed to get integer IDs for package and dataset or package does not belong to dataset")
		return "", fmt.Errorf("package not found or does not belong to specified dataset: %w", err)
	}

	// Construct the S3 prefix for all assets in the package
	// Format: O{WorkspaceIntId}/D{DatasetIntId}/P{PackageIntId}/
	// Note: CloudFront origin_path="/viewer-assets" will automatically prepend viewer-assets/ when accessing S3
	assetPrefix := fmt.Sprintf("O%d/D%d/P%d/", h.claims.OrgClaim.IntId, datasetIntId, packageIntId)

	h.logger.WithFields(log.Fields{
		"packageNodeId":  packageNodeId,
		"datasetNodeId":  datasetNodeId,
		"packageIntId":   packageIntId,
		"datasetIntId":   datasetIntId,
		"workspaceIntId": h.claims.OrgClaim.IntId,
		"assetPrefix":    assetPrefix,
	}).Debug("constructed S3 prefix for package assets")

	return assetPrefix, nil
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
