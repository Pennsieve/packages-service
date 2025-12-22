package handler

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/cloudfront/sign"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	log "github.com/sirupsen/logrus"
)

type CloudFrontSignedURLHandler struct {
	RequestHandler
}

type CloudFrontSignedURLResponse struct {
	SignedURL string `json:"signed_url"`
	ExpiresAt int64  `json:"expires_at"` // Unix timestamp
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
	// Initialize CloudFront configuration from environment variables
	if domain, ok := os.LookupEnv("CLOUDFRONT_DISTRIBUTION_DOMAIN"); ok {
		cloudfrontDistributionDomain = domain
		log.Infof("CloudFront distribution domain initialized: %s", cloudfrontDistributionDomain)
	} else {
		log.Warn("CLOUDFRONT_DISTRIBUTION_DOMAIN environment variable not set")
	}

	if keyID, ok := os.LookupEnv("CLOUDFRONT_KEY_ID"); ok {
		cloudfrontKeyID = keyID
		log.Infof("CloudFront key ID initialized: %s", cloudfrontKeyID)
	} else {
		log.Warn("CLOUDFRONT_KEY_ID environment variable not set")
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
	// Load private key from Secrets Manager (fallback to SSM for backward compatibility)
	if secretName, ok := os.LookupEnv("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME"); ok {
		// Use Secrets Manager (new approach)
		if err := h.loadKeysFromSecretsManager(ctx, secretName); err != nil {
			log.Errorf("Failed to load keys from Secrets Manager: %v", err)
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}
	} else if ssmParamName, ok := os.LookupEnv("CLOUDFRONT_PRIVATE_KEY_SSM_PARAM"); ok {
		log.Infof("Loading CloudFront private key from SSM parameter: %s", ssmParamName)

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
			log.Errorf("Failed to load AWS config: %v", err)
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		log.Infof("AWS config loaded with region: %s", cfg.Region)

		// Create SSM client
		ssmClient := ssm.NewFromConfig(cfg)

		// Get parameter from SSM

		//withDecryption := fal
		ssmParam := ssmParamName

		log.Infof("ssmParam: %s", ssmParam)
		input := ssm.GetParameterInput{
			Name:           aws.String(ssmParam),
			WithDecryption: aws.Bool(true),
		}

		log.Infof("Parameter Input: %v", input)

		result, err := ssmClient.GetParameter(ctx, &input)
		if err != nil {
			log.Errorf("Failed to get CloudFront private key from SSM: %v", err)
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		if result.Parameter == nil || result.Parameter.Value == nil {
			log.Error("SSM parameter value is nil")
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		paramValue := *result.Parameter.Value
		log.Infof("SSM parameter value length: %d", len(paramValue))
		prefixLen := 50
		if len(paramValue) < prefixLen {
			prefixLen = len(paramValue)
		}
		log.Infof("SSM parameter value prefix (first 50 chars): %s", paramValue[:prefixLen])

		// Decode base64
		keyBytes, err := base64.StdEncoding.DecodeString(paramValue)
		if err != nil {
			log.Errorf("Failed to decode CloudFront private key from base64: %v", err)
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		log.Infof("Decoded key bytes length: %d", len(keyBytes))
		decodedPrefixLen := 50
		if len(keyBytes) < decodedPrefixLen {
			decodedPrefixLen = len(keyBytes)
		}
		log.Infof("Decoded key bytes prefix (first 50 chars): %s", string(keyBytes)[:decodedPrefixLen])
		
		// Log the full decoded content for debugging
		log.Infof("Full decoded key content: %s", string(keyBytes))

		// Parse PEM block
		block, _ := pem.Decode(keyBytes)
		if block == nil {
			log.Error("Failed to parse PEM block from CloudFront private key")
			log.Errorf("PEM parsing failed - full content length: %d", len(keyBytes))
			return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
		}

		// Parse private key
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			// Try PKCS8 format if PKCS1 fails
			keyInterface, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				log.Errorf("Failed to parse CloudFront private key: %v", err)
				return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
			}
			var ok bool
			key, ok = keyInterface.(*rsa.PrivateKey)
			if !ok {
				log.Error("CloudFront private key is not RSA")
				return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
			}
		}
		cloudfrontPrivateKey = key
		log.Info("CloudFront private key loaded successfully from SSM")
	} else {
		log.Warn("CLOUDFRONT_PRIVATE_KEY_SSM_PARAM environment variable not set")
	}

	if cloudfrontDistributionDomain == "" || cloudfrontKeyID == "" || cloudfrontPrivateKey == nil {
		return h.logAndBuildError("CloudFront signing not configured", http.StatusInternalServerError), nil
	}

	// Get parameters from query string
	datasetID := h.queryParams["dataset_id"]
	packageID := h.queryParams["package_id"]
	// Note: path is now optional - if provided, it will be appended to the URL for user convenience
	path := h.queryParams["path"]

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
	// Construct the resource pattern with wildcard for all files under the prefix
	// This allows access to any file within the package
	resourcePattern := fmt.Sprintf("https://%s/%s*", cloudfrontDistributionDomain, s3Prefix)
	
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
	signer := sign.NewURLSigner(cloudfrontKeyID, cloudfrontPrivateKey)

	// Build the base URL - if optionalPath is provided, include it for user convenience
	// The policy still allows access to all files in the prefix
	var baseURL string
	if optionalPath != "" {
		baseURL = fmt.Sprintf("https://%s/%s%s", cloudfrontDistributionDomain, s3Prefix, optionalPath)
	} else {
		// Return URL pointing to the prefix
		baseURL = fmt.Sprintf("https://%s/%s", cloudfrontDistributionDomain, s3Prefix)
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
	return nil
}
