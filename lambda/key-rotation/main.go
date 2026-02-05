package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type RotationEvent struct {
	Step               string `json:"Step"`
	SecretId           string `json:"SecretId"`
	ClientRequestToken string `json:"ClientRequestToken"`
	RotationToken      string `json:"RotationToken,omitempty"`
}

type KeyPair struct {
	PrivateKey  string    `json:"privateKey"`
	PublicKey   string    `json:"publicKey"`
	KeyID       string    `json:"keyId"`
	CreatedAt   time.Time `json:"createdAt"`
	KeyGroupID  string    `json:"keyGroupId"`
	PublicKeyID string    `json:"publicKeyId"`
	// Track previous key for cleanup
	PreviousKeyID     string     `json:"previousKeyId,omitempty"`
	PreviousRotatedAt *time.Time `json:"previousRotatedAt,omitempty"`
}

func generateKeyPair(clientRequestToken string) (*KeyPair, error) {
	// Generate RSA key pair (2048 bits for CloudFront)
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	// Convert private key to PEM format
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	// Convert public key to PEM format
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	// Generate unique key ID using the unique suffix of ClientRequestToken
	// ClientRequestToken format is usually "terraform-20251222160633087200000003"
	// So we use the last 8 characters for uniqueness
	tokenSuffix := clientRequestToken
	if len(clientRequestToken) > 8 {
		tokenSuffix = clientRequestToken[len(clientRequestToken)-8:]
	}
	keyID := fmt.Sprintf("cf-key-%s", tokenSuffix)

	return &KeyPair{
		PrivateKey: base64.StdEncoding.EncodeToString(privateKeyPEM),
		PublicKey:  string(publicKeyPEM),
		KeyID:      keyID,
		CreatedAt:  time.Now(),
	}, nil
}

func createSecret(ctx context.Context, smClient *secretsmanager.Client, cfClient *cloudfront.Client, event RotationEvent) error {
	log.Printf("Creating new secret version for %s", event.SecretId)

	// Check if AWSPENDING version already exists (for idempotency)
	_, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSPENDING"),
		VersionId:    aws.String(event.ClientRequestToken),
	})

	// If secret already exists, we're good (idempotency)
	if err == nil {
		log.Printf("Secret version already exists, skipping creation")
		return nil
	}

	// Generate new key pair
	keyPair, err := generateKeyPair(event.ClientRequestToken)
	if err != nil {
		return fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Get current secret to track previous key for grace period
	currentSecret, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSCURRENT"),
	})
	// Parse previous key pair if it exists (not an error if it doesn't exist for first rotation)
	if err == nil && currentSecret.SecretString != nil {
		var oldKeyPair KeyPair
		if err := json.Unmarshal([]byte(*currentSecret.SecretString), &oldKeyPair); err == nil {
			if oldKeyPair.PublicKeyID != "" {
				// Track the previous key for cleanup after grace period
				keyPair.PreviousKeyID = oldKeyPair.PublicKeyID
				rotatedAt := time.Now()
				keyPair.PreviousRotatedAt = &rotatedAt
				log.Printf("Tracking previous key %s for grace period cleanup", oldKeyPair.PublicKeyID)
			}
		}
	}

	// Option A: Create CloudFront public key and add to key group in createSecret
	// This ensures atomicity and prevents race conditions where service lambda
	// reads the secret before PublicKeyID is populated
	publicKeyName := fmt.Sprintf("packages-%s-%s", os.Getenv("ENVIRONMENT"), keyPair.KeyID)
	var publicKeyID string

	// Check if CloudFront public key already exists (for idempotency)
	listResp, err := cfClient.ListPublicKeys(ctx, &cloudfront.ListPublicKeysInput{})
	if err != nil {
		return fmt.Errorf("failed to list CloudFront public keys: %w", err)
	}

	// Check if key already exists
	for _, item := range listResp.PublicKeyList.Items {
		if *item.Name == publicKeyName {
			log.Printf("CloudFront public key already exists: %s (ID: %s)", publicKeyName, *item.Id)
			publicKeyID = *item.Id
			break
		}
	}

	// Create public key only if it doesn't exist
	if publicKeyID == "" {
		log.Printf("Creating new CloudFront public key: %s", publicKeyName)
		publicKeyResp, err := cfClient.CreatePublicKey(ctx, &cloudfront.CreatePublicKeyInput{
			PublicKeyConfig: &types.PublicKeyConfig{
				CallerReference: aws.String(keyPair.KeyID),
				Name:            aws.String(publicKeyName),
				EncodedKey:      aws.String(keyPair.PublicKey),
				Comment:         aws.String(fmt.Sprintf("Auto-rotated key created at %s", keyPair.CreatedAt.Format(time.RFC3339))),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create CloudFront public key: %w", err)
		}
		publicKeyID = *publicKeyResp.PublicKey.Id
	}

	// Update key pair with CloudFront IDs
	keyPair.PublicKeyID = publicKeyID

	// Get the key group ID from environment
	keyGroupID := os.Getenv("CLOUDFRONT_KEY_GROUP_ID")
	if keyGroupID != "" {
		// Get current key group configuration
		keyGroupResp, err := cfClient.GetKeyGroup(ctx, &cloudfront.GetKeyGroupInput{
			Id: aws.String(keyGroupID),
		})
		if err != nil {
			return fmt.Errorf("failed to get key group: %w", err)
		}

		// Add new public key to key group (check if it's not already there)
		keyGroupConfig := keyGroupResp.KeyGroup.KeyGroupConfig
		keyAlreadyInGroup := false
		for _, existingKeyID := range keyGroupConfig.Items {
			if existingKeyID == publicKeyID {
				keyAlreadyInGroup = true
				break
			}
		}

		if !keyAlreadyInGroup {
			keyGroupConfig.Items = append(keyGroupConfig.Items, publicKeyID)

			// Update key group only if we added a new key
			_, err = cfClient.UpdateKeyGroup(ctx, &cloudfront.UpdateKeyGroupInput{
				Id:             aws.String(keyGroupID),
				KeyGroupConfig: keyGroupConfig,
				IfMatch:        keyGroupResp.ETag,
			})
			if err != nil {
				return fmt.Errorf("failed to update key group: %w", err)
			}
			log.Printf("Added public key %s to key group %s", publicKeyID, keyGroupID)
		} else {
			log.Printf("Public key %s already exists in key group", publicKeyID)
		}

		keyPair.KeyGroupID = keyGroupID
	}

	// Marshal key pair to JSON (now includes PublicKeyID)
	secretData, err := json.Marshal(keyPair)
	if err != nil {
		return fmt.Errorf("failed to marshal key pair: %w", err)
	}

	// Create new secret version with complete key pair data
	_, err = smClient.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:           aws.String(event.SecretId),
		ClientRequestToken: aws.String(event.ClientRequestToken),
		SecretString:       aws.String(string(secretData)),
		VersionStages:      []string{"AWSPENDING"},
	})
	if err != nil {
		return fmt.Errorf("failed to put secret value: %w", err)
	}

	log.Printf("Successfully created new secret version with key ID: %s and CloudFront public key ID: %s", keyPair.KeyID, keyPair.PublicKeyID)
	return nil
}

func setSecret(ctx context.Context, smClient *secretsmanager.Client, event RotationEvent) error {
	log.Printf("Setting secret for %s", event.SecretId)

	// In Option A, all CloudFront operations are moved to createSecret step
	// setSecret step is a no-op since the secret with CloudFront keys was already created
	// This follows AWS best practice: createSecret creates both the secret and external resources

	// Verify that the pending secret exists and is valid
	pendingSecret, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSPENDING"),
		VersionId:    aws.String(event.ClientRequestToken),
	})
	if err != nil {
		return fmt.Errorf("failed to get pending secret: %w", err)
	}

	// Parse and validate the key pair
	var keyPair KeyPair
	if err := json.Unmarshal([]byte(*pendingSecret.SecretString), &keyPair); err != nil {
		return fmt.Errorf("failed to unmarshal key pair: %w", err)
	}

	// Validate that CloudFront keys were properly set in createSecret
	if keyPair.PublicKeyID == "" {
		return fmt.Errorf("PublicKeyID is empty - CloudFront key creation may have failed in createSecret step")
	}

	log.Printf("Successfully validated secret - CloudFront public key ID: %s", keyPair.PublicKeyID)
	return nil
}

func testSecret(ctx context.Context, client *secretsmanager.Client, event RotationEvent) error {
	log.Printf("Testing secret for %s", event.SecretId)

	// Get the pending secret version
	pendingSecret, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSPENDING"),
		VersionId:    aws.String(event.ClientRequestToken),
	})
	if err != nil {
		return fmt.Errorf("failed to get pending secret: %w", err)
	}

	// Parse and validate the key pair
	var keyPair KeyPair
	if err := json.Unmarshal([]byte(*pendingSecret.SecretString), &keyPair); err != nil {
		return fmt.Errorf("failed to unmarshal key pair: %w", err)
	}

	// Decode and validate private key
	privateKeyPEM, err := base64.StdEncoding.DecodeString(keyPair.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to decode private key: %w", err)
	}

	block, _ := pem.Decode(privateKeyPEM)
	if block == nil {
		return fmt.Errorf("failed to decode PEM block")
	}

	_, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	log.Printf("Successfully tested secret - key is valid")
	return nil
}

func finishSecret(ctx context.Context, smClient *secretsmanager.Client, cfClient *cloudfront.Client, event RotationEvent) error {
	log.Printf("Finishing secret rotation for %s", event.SecretId)

	// Get the current version for version stage management
	currentSecret, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSCURRENT"),
	})
	if err != nil {
		return fmt.Errorf("failed to get current secret version: %w", err)
	}

	// Get the pending version to check if cleanup is needed
	pendingSecret, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSPENDING"),
		VersionId:    aws.String(event.ClientRequestToken),
	})
	if err != nil {
		return fmt.Errorf("failed to get pending secret version: %w", err)
	}

	// Parse the new key pair to check for expired keys that need cleanup
	var newKeyPair KeyPair
	if err := json.Unmarshal([]byte(*pendingSecret.SecretString), &newKeyPair); err != nil {
		return fmt.Errorf("failed to unmarshal new key pair: %w", err)
	}

	// Check if previous keys need cleanup (they should already be tracked in the secret from createSecret)
	if err := cleanupExpiredKeys(ctx, cfClient, &newKeyPair); err != nil {
		log.Printf("Warning: failed to cleanup expired keys: %v", err)
		// Don't fail the rotation
	}

	// Update version stages - move AWSPENDING to AWSCURRENT
	// This is the core function of finishSecret according to AWS best practices
	_, err = smClient.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
		SecretId:            aws.String(event.SecretId),
		VersionStage:        aws.String("AWSCURRENT"),
		MoveToVersionId:     aws.String(event.ClientRequestToken),
		RemoveFromVersionId: currentSecret.VersionId,
	})
	if err != nil {
		return fmt.Errorf("failed to update secret version stage: %w", err)
	}

	// Clean up old secret versions (remove AWSPREVIOUS labels from old versions)
	err = cleanupOldSecretVersions(ctx, smClient, event.SecretId)
	if err != nil {
		log.Printf("Warning: failed to cleanup old secret versions: %v", err)
		// Don't fail the rotation if cleanup fails
	}

	log.Printf("Successfully finished secret rotation")
	return nil
}

func handleRotation(ctx context.Context, event RotationEvent) error {
	// Log the event for debugging
	log.Printf("Rotation event received: Step=%s, SecretId=%s, ClientRequestToken=%s, RotationToken=%s",
		event.Step, event.SecretId, event.ClientRequestToken, event.RotationToken)
	log.Printf("ClientRequestToken length: %d", len(event.ClientRequestToken))

	// Validate required fields
	if event.SecretId == "" {
		return fmt.Errorf("SecretId is required")
	}
	if event.ClientRequestToken == "" {
		return fmt.Errorf("ClientRequestToken is required")
	}
	if len(event.ClientRequestToken) < 32 {
		return fmt.Errorf("ClientRequestToken must be at least 32 characters, got %d", len(event.ClientRequestToken))
	}

	// Create AWS clients
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	smClient := secretsmanager.NewFromConfig(cfg)
	cfClient := cloudfront.NewFromConfig(cfg)

	// Handle rotation steps (AWS uses both camelCase and underscore formats)
	switch event.Step {
	case "createSecret", "create_secret":
		return createSecret(ctx, smClient, cfClient, event)
	case "setSecret", "set_secret":
		return setSecret(ctx, smClient, event)
	case "testSecret", "test_secret":
		return testSecret(ctx, smClient, event)
	case "finishSecret", "finish_secret":
		return finishSecret(ctx, smClient, cfClient, event)
	case "cleanupOldKeys", "cleanup_old_keys":
		// Custom step to cleanup old keys after grace period
		return cleanupOldKeys(ctx, smClient, cfClient, event)
	default:
		return fmt.Errorf("unknown rotation step: %s", event.Step)
	}
}

// removeOldKeyFromGroup removes an old CloudFront public key from the key group and deletes it entirely
// Since signed URL policies are only valid for 1 hour, it's safe to delete the key immediately
func removeOldKeyFromGroup(ctx context.Context, cfClient *cloudfront.Client, keyGroupID, publicKeyID string) error {
	log.Printf("Removing and deleting old CloudFront public key %s from key group %s", publicKeyID, keyGroupID)

	// Get current key group configuration
	keyGroupResp, err := cfClient.GetKeyGroup(ctx, &cloudfront.GetKeyGroupInput{
		Id: aws.String(keyGroupID),
	})
	if err != nil {
		return fmt.Errorf("failed to get key group: %w", err)
	}

	// Remove the old public key from the key group
	keyGroupConfig := keyGroupResp.KeyGroup.KeyGroupConfig
	newItems := make([]string, 0, len(keyGroupConfig.Items))
	keyFound := false

	for _, existingKeyID := range keyGroupConfig.Items {
		if existingKeyID != publicKeyID {
			newItems = append(newItems, existingKeyID)
		} else {
			keyFound = true
		}
	}

	if !keyFound {
		log.Printf("Old public key %s not found in key group, skipping removal", publicKeyID)
		return nil
	}

	// Update key group with the new key list (excluding the old key)
	keyGroupConfig.Items = newItems
	_, err = cfClient.UpdateKeyGroup(ctx, &cloudfront.UpdateKeyGroupInput{
		Id:             aws.String(keyGroupID),
		KeyGroupConfig: keyGroupConfig,
		IfMatch:        keyGroupResp.ETag,
	})
	if err != nil {
		return fmt.Errorf("failed to update key group: %w", err)
	}

	log.Printf("Successfully removed old public key %s from key group", publicKeyID)

	// Now delete the CloudFront public key entirely to reclaim quota
	err = deleteCloudFrontPublicKey(ctx, cfClient, publicKeyID)
	if err != nil {
		log.Printf("Warning: failed to delete CloudFront public key %s: %v", publicKeyID, err)
		// Don't fail the entire operation if deletion fails
		return nil
	}

	log.Printf("Successfully deleted CloudFront public key %s (quota reclaimed)", publicKeyID)
	return nil
}

// deleteCloudFrontPublicKey deletes a CloudFront public key to reclaim quota
func deleteCloudFrontPublicKey(ctx context.Context, cfClient *cloudfront.Client, publicKeyID string) error {
	// Get the public key details to retrieve the ETag
	keyResp, err := cfClient.GetPublicKey(ctx, &cloudfront.GetPublicKeyInput{
		Id: aws.String(publicKeyID),
	})
	if err != nil {
		return fmt.Errorf("failed to get public key %s for deletion: %w", publicKeyID, err)
	}

	// Delete the public key using the ETag
	_, err = cfClient.DeletePublicKey(ctx, &cloudfront.DeletePublicKeyInput{
		Id:      aws.String(publicKeyID),
		IfMatch: keyResp.ETag,
	})
	if err != nil {
		return fmt.Errorf("failed to delete public key %s: %w", publicKeyID, err)
	}

	return nil
}

// cleanupExpiredKeys checks if any previous keys have exceeded the grace period and removes them
func cleanupExpiredKeys(ctx context.Context, cfClient *cloudfront.Client, keyPair *KeyPair) error {
	// Check if we have a previous key that needs cleanup
	if keyPair.PreviousKeyID == "" || keyPair.PreviousRotatedAt == nil {
		return nil
	}

	// Get grace period from environment variable (default 48 hours)
	gracePeriodHours := 48
	if envValue := os.Getenv("KEY_ROTATION_GRACE_PERIOD_HOURS"); envValue != "" {
		if hours, err := time.ParseDuration(envValue + "h"); err == nil {
			gracePeriodHours = int(hours.Hours())
		}
	}

	// Check if grace period has passed
	timeSinceRotation := time.Since(*keyPair.PreviousRotatedAt)
	if timeSinceRotation < time.Duration(gracePeriodHours)*time.Hour {
		log.Printf("Previous key %s still in grace period (%.1f hours remaining)",
			keyPair.PreviousKeyID,
			float64(gracePeriodHours)-timeSinceRotation.Hours())
		return nil
	}

	// Grace period has passed, remove the old key from the key group
	keyGroupID := os.Getenv("CLOUDFRONT_KEY_GROUP_ID")
	if keyGroupID != "" {
		if err := removeOldKeyFromGroup(ctx, cfClient, keyGroupID, keyPair.PreviousKeyID); err != nil {
			return fmt.Errorf("failed to remove expired key from group: %w", err)
		}

		// Clear the previous key tracking
		keyPair.PreviousKeyID = ""
		keyPair.PreviousRotatedAt = nil

		log.Printf("Successfully cleaned up expired key after grace period")
	}

	return nil
}

// cleanupOldKeys is a custom step that can be called manually or via scheduled event
func cleanupOldKeys(ctx context.Context, smClient *secretsmanager.Client, cfClient *cloudfront.Client, event RotationEvent) error {
	log.Printf("Running cleanup of old keys for %s", event.SecretId)

	// Get the current secret
	currentSecret, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSCURRENT"),
	})
	if err != nil {
		return fmt.Errorf("failed to get current secret: %w", err)
	}

	// Parse the key pair
	var keyPair KeyPair
	if err := json.Unmarshal([]byte(*currentSecret.SecretString), &keyPair); err != nil {
		return fmt.Errorf("failed to unmarshal key pair: %w", err)
	}

	// Store original state to check if we made changes
	originalPreviousKeyID := keyPair.PreviousKeyID

	// Check and cleanup expired keys
	if err := cleanupExpiredKeys(ctx, cfClient, &keyPair); err != nil {
		return fmt.Errorf("failed to cleanup expired keys: %w", err)
	}

	// Update the secret if we cleaned up any keys
	if originalPreviousKeyID != "" && keyPair.PreviousKeyID == "" {
		updatedSecretData, err := json.Marshal(keyPair)
		if err != nil {
			return fmt.Errorf("failed to marshal updated key pair: %w", err)
		}

		_, err = smClient.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
			SecretId:     aws.String(event.SecretId),
			SecretString: aws.String(string(updatedSecretData)),
		})
		if err != nil {
			return fmt.Errorf("failed to update secret after cleanup: %w", err)
		}

		log.Printf("Updated secret to remove cleaned up key tracking")
	} else if originalPreviousKeyID == "" {
		log.Printf("No previous keys found to cleanup")
	} else {
		log.Printf("Previous key still in grace period, no cleanup needed")
	}

	log.Printf("Successfully completed cleanup check for old keys")
	return nil
}

// cleanupOldSecretVersions removes AWSPREVIOUS labels from old secret versions
// This follows AWS best practice of removing staging labels to trigger automatic cleanup
func cleanupOldSecretVersions(ctx context.Context, smClient *secretsmanager.Client, secretId string) error {
	log.Printf("Cleaning up old secret versions for %s", secretId)

	// List all versions of the secret
	listResp, err := smClient.ListSecretVersionIds(ctx, &secretsmanager.ListSecretVersionIdsInput{
		SecretId: aws.String(secretId),
	})
	if err != nil {
		return fmt.Errorf("failed to list secret versions: %w", err)
	}

	// Find versions with AWSPREVIOUS label that we want to clean up
	// Keep only the most recent AWSPREVIOUS version for safety
	var previousVersions []string
	for _, version := range listResp.Versions {
		for _, stage := range version.VersionStages {
			if stage == "AWSPREVIOUS" {
				previousVersions = append(previousVersions, *version.VersionId)
				break
			}
		}
	}

	// Keep only the most recent AWSPREVIOUS version, remove labels from older ones
	if len(previousVersions) > 1 {
		// Sort by creation date - the listResp should already be sorted with newest first
		// Remove AWSPREVIOUS label from all but the most recent one
		for i := 1; i < len(previousVersions); i++ {
			versionId := previousVersions[i]
			log.Printf("Removing AWSPREVIOUS label from old version: %s", versionId)

			_, err = smClient.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
				SecretId:            aws.String(secretId),
				VersionStage:        aws.String("AWSPREVIOUS"),
				RemoveFromVersionId: aws.String(versionId),
			})
			if err != nil {
				log.Printf("Warning: failed to remove AWSPREVIOUS label from version %s: %v", versionId, err)
				// Continue with other versions
			} else {
				log.Printf("Successfully removed AWSPREVIOUS label from version %s", versionId)
			}
		}
	}

	return nil
}

func main() {
	lambda.Start(handleRotation)
}
