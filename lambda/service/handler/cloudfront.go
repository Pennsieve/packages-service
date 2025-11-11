package handler

import (
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

var (
	cloudfrontDistributionDomain string
	cloudfrontKeyID              string
	cloudfrontPrivateKey         *rsa.PrivateKey
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
	// Validate CloudFront configuration
	// Load private key from SSM Parameter Store
	if ssmParamName, ok := os.LookupEnv("CLOUDFRONT_PRIVATE_KEY_SSM_PARAM"); ok {
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
	path := h.queryParams["path"]

	// Validate required parameters
	if datasetID == "" {
		return h.logAndBuildError("missing required 'dataset_id' query parameter", http.StatusBadRequest), nil
	}
	if packageID == "" {
		return h.logAndBuildError("missing required 'package_id' query parameter", http.StatusBadRequest), nil
	}
	if path == "" {
		return h.logAndBuildError("missing required 'path' query parameter - CloudFront signing only supports viewer assets", http.StatusBadRequest), nil
	}

	h.logger.WithFields(log.Fields{
		"packageId": packageID,
		"datasetId": datasetID,
		"assetPath": path,
	}).Info("handling GET request for CloudFront signed URL")

	// For viewer assets, validate and construct the key
	s3Key, err := h.getS3KeyForViewerAsset(ctx, packageID, datasetID, path)

	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to get S3 key: %v", err), http.StatusInternalServerError), nil
	}

	// Generate CloudFront signed URL
	signedURL, expiresAt, err := h.generateCloudFrontSignedURL(s3Key)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to generate signed URL: %v", err), http.StatusInternalServerError), nil
	}

	// Build response
	response := CloudFrontSignedURLResponse{
		SignedURL: signedURL,
		ExpiresAt: expiresAt.Unix(),
	}

	responseBody, err := json.Marshal(response)
	if err != nil {
		return h.logAndBuildError(fmt.Sprintf("failed to marshal response: %v", err), http.StatusInternalServerError), nil
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

// getS3KeyForViewerAsset validates and constructs the S3 key for viewer assets
func (h *CloudFrontSignedURLHandler) getS3KeyForViewerAsset(ctx context.Context, packageNodeId, datasetNodeId, assetPath string) (string, error) {
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

	// Construct the S3 key for the viewer asset
	// Format: O{WorkspaceIntId}/D{DatasetIntId}/P{PackageIntId}/{AssetPath}
	assetKey := fmt.Sprintf("O%d/D%d/P%d/%s", h.claims.OrgClaim.IntId, datasetIntId, packageIntId, assetPath)

	h.logger.WithFields(log.Fields{
		"packageNodeId":  packageNodeId,
		"datasetNodeId":  datasetNodeId,
		"packageIntId":   packageIntId,
		"datasetIntId":   datasetIntId,
		"workspaceIntId": h.claims.OrgClaim.IntId,
		"assetPath":      assetPath,
		"assetKey":       assetKey,
	}).Debug("constructed S3 key for viewer asset")

	return assetKey, nil
}

// generateCloudFrontSignedURL generates a signed URL for CloudFront distribution
func (h *CloudFrontSignedURLHandler) generateCloudFrontSignedURL(s3Key string) (string, time.Time, error) {
	// Construct the full URL
	resourceURL := fmt.Sprintf("https://%s/%s", cloudfrontDistributionDomain, s3Key)

	// Set expiration time (1 hour from now)
	expiresAt := time.Now().Add(1 * time.Hour)

	// Create the signer
	signer := sign.NewURLSigner(cloudfrontKeyID, cloudfrontPrivateKey)

	// Sign the URL
	signedURL, err := signer.Sign(resourceURL, expiresAt)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to sign URL: %w", err)
	}

	return signedURL, expiresAt, nil
}
