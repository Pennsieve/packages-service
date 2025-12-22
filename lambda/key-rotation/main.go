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
	Token              string `json:"Token"`
	SecretId           string `json:"SecretId"`
	VersionId          string `json:"VersionId"`
}

type KeyPair struct {
	PrivateKey    string    `json:"privateKey"`
	PublicKey     string    `json:"publicKey"`
	KeyID         string    `json:"keyId"`
	CreatedAt     time.Time `json:"createdAt"`
	KeyGroupID    string    `json:"keyGroupId"`
	PublicKeyID   string    `json:"publicKeyId"`
}

func generateKeyPair() (*KeyPair, error) {
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

	// Generate unique key ID
	keyID := fmt.Sprintf("cf-key-%d", time.Now().Unix())

	return &KeyPair{
		PrivateKey: base64.StdEncoding.EncodeToString(privateKeyPEM),
		PublicKey:  string(publicKeyPEM),
		KeyID:      keyID,
		CreatedAt:  time.Now(),
	}, nil
}

func createSecret(ctx context.Context, client *secretsmanager.Client, event RotationEvent) error {
	log.Printf("Creating new secret version for %s", event.SecretId)
	
	// Generate new key pair
	keyPair, err := generateKeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Marshal key pair to JSON
	secretData, err := json.Marshal(keyPair)
	if err != nil {
		return fmt.Errorf("failed to marshal key pair: %w", err)
	}

	// Create new secret version
	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:           aws.String(event.SecretId),
		ClientRequestToken: aws.String(event.Token),
		SecretString:       aws.String(string(secretData)),
		VersionStages:      []string{"AWSPENDING"},
	})
	if err != nil {
		return fmt.Errorf("failed to put secret value: %w", err)
	}

	log.Printf("Successfully created new secret version with key ID: %s", keyPair.KeyID)
	return nil
}

func setSecret(ctx context.Context, smClient *secretsmanager.Client, cfClient *cloudfront.Client, event RotationEvent) error {
	log.Printf("Setting secret for %s", event.SecretId)
	
	// Get the pending secret version
	pendingSecret, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSPENDING"),
		VersionId:    aws.String(event.Token),
	})
	if err != nil {
		return fmt.Errorf("failed to get pending secret: %w", err)
	}

	// Parse the key pair
	var keyPair KeyPair
	if err := json.Unmarshal([]byte(*pendingSecret.SecretString), &keyPair); err != nil {
		return fmt.Errorf("failed to unmarshal key pair: %w", err)
	}

	// Create CloudFront public key
	publicKeyResp, err := cfClient.CreatePublicKey(ctx, &cloudfront.CreatePublicKeyInput{
		PublicKeyConfig: &types.PublicKeyConfig{
			CallerReference: aws.String(keyPair.KeyID),
			Name:            aws.String(fmt.Sprintf("packages-%s-%s", os.Getenv("ENVIRONMENT"), keyPair.KeyID)),
			EncodedKey:      aws.String(keyPair.PublicKey),
			Comment:         aws.String(fmt.Sprintf("Auto-rotated key created at %s", keyPair.CreatedAt.Format(time.RFC3339))),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create CloudFront public key: %w", err)
	}

	// Update key pair with CloudFront IDs
	keyPair.PublicKeyID = *publicKeyResp.PublicKey.Id
	
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

		// Add new public key to key group
		keyGroupConfig := keyGroupResp.KeyGroup.KeyGroupConfig
		keyGroupConfig.Items = append(keyGroupConfig.Items, *publicKeyResp.PublicKey.Id)

		// Update key group
		_, err = cfClient.UpdateKeyGroup(ctx, &cloudfront.UpdateKeyGroupInput{
			Id:              aws.String(keyGroupID),
			KeyGroupConfig:  keyGroupConfig,
			IfMatch:        keyGroupResp.ETag,
		})
		if err != nil {
			return fmt.Errorf("failed to update key group: %w", err)
		}

		keyPair.KeyGroupID = keyGroupID
	}

	// Update secret with CloudFront IDs
	updatedSecretData, err := json.Marshal(keyPair)
	if err != nil {
		return fmt.Errorf("failed to marshal updated key pair: %w", err)
	}

	_, err = smClient.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:           aws.String(event.SecretId),
		ClientRequestToken: aws.String(event.Token),
		SecretString:       aws.String(string(updatedSecretData)),
		VersionStages:      []string{"AWSPENDING"},
	})
	if err != nil {
		return fmt.Errorf("failed to update secret with CloudFront IDs: %w", err)
	}

	log.Printf("Successfully set secret with CloudFront public key ID: %s", keyPair.PublicKeyID)
	return nil
}

func testSecret(ctx context.Context, client *secretsmanager.Client, event RotationEvent) error {
	log.Printf("Testing secret for %s", event.SecretId)
	
	// Get the pending secret version
	pendingSecret, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(event.SecretId),
		VersionStage: aws.String("AWSPENDING"),
		VersionId:    aws.String(event.Token),
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

func finishSecret(ctx context.Context, client *secretsmanager.Client, event RotationEvent) error {
	log.Printf("Finishing secret rotation for %s", event.SecretId)
	
	// Update version stages
	_, err := client.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
		SecretId:            aws.String(event.SecretId),
		VersionStage:        aws.String("AWSCURRENT"),
		MoveToVersionId:     aws.String(event.Token),
		RemoveFromVersionId: aws.String(event.VersionId),
	})
	if err != nil {
		return fmt.Errorf("failed to update secret version stage: %w", err)
	}

	log.Printf("Successfully finished secret rotation")
	return nil
}

func handleRotation(ctx context.Context, event RotationEvent) error {
	// Create AWS clients
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	smClient := secretsmanager.NewFromConfig(cfg)
	cfClient := cloudfront.NewFromConfig(cfg)

	// Handle rotation steps
	switch event.Step {
	case "createSecret":
		return createSecret(ctx, smClient, event)
	case "setSecret":
		return setSecret(ctx, smClient, cfClient, event)
	case "testSecret":
		return testSecret(ctx, smClient, event)
	case "finishSecret":
		return finishSecret(ctx, smClient, event)
	default:
		return fmt.Errorf("unknown rotation step: %s", event.Step)
	}
}

func main() {
	lambda.Start(handleRotation)
}