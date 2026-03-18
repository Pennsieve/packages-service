package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pennsieve/packages-service/api/regions"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lib/pq"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/permissions"
)

type ExternalBucketConfig map[string]string

const ExternalBucketsRoleMapKey = "EXTERNAL_BUCKETS_ROLE_MAP"

func ExternalBucketConfigFromEnv() (ExternalBucketConfig, error) {
	raw := os.Getenv(ExternalBucketsRoleMapKey)
	if raw == "" {
		return nil, fmt.Errorf("%s not set", ExternalBucketsRoleMapKey)
	}
	m := make(map[string]string)
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("parsing %s value [%s]: %w", ExternalBucketsRoleMapKey, raw, err)
	}
	return m, nil
}

type BucketOptions struct {
	Region              string
	CredentialsProvider *aws.CredentialsCache
}

type DownloadManifestHandler struct {
	RequestHandler
	externalBucketConfig ExternalBucketConfig
}

func (h *DownloadManifestHandler) handle(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	switch h.method {
	case "POST":
		externalBucketConfig, err := ExternalBucketConfigFromEnv()
		if err != nil {
			return h.logAndBuildError(err.Error(), http.StatusInternalServerError), nil
		}
		h.externalBucketConfig = externalBucketConfig
		return h.post(ctx)
	default:
		return h.logAndBuildError("method not allowed: "+h.method, http.StatusMethodNotAllowed), nil
	}
}

func (h *DownloadManifestHandler) post(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	if h.claims.DatasetClaim == nil {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}
	if authorized := authorizer.HasRole(*h.claims, permissions.ViewFiles); !authorized {
		return h.logAndBuildError("unauthorized", http.StatusUnauthorized), nil
	}

	datasetNodeId, ok := h.request.QueryStringParameters["dataset_id"]
	if !ok {
		return h.logAndBuildError("query param 'dataset_id' is required", http.StatusBadRequest), nil
	}

	var request models.DownloadRequest
	if err := json.Unmarshal([]byte(h.body), &request); err != nil {
		return h.logAndBuildError(fmt.Sprintf("unable to unmarshal request body: %v", err), http.StatusBadRequest), nil
	}
	if len(request.NodeIds) == 0 {
		return h.logAndBuildError("nodeIds must not be empty", http.StatusBadRequest), nil
	}

	orgId := int(h.claims.OrgClaim.IntId)

	rows, err := h.getPackageHierarchy(ctx, orgId, datasetNodeId, request.NodeIds)
	if err != nil {
		h.logger.Errorf("failed to query package hierarchy: %v", err)
		return nil, err
	}

	if len(rows) == 0 {
		resp := models.DownloadManifestResponse{
			Header: models.DownloadManifestHeader{Count: 0, Size: 0},
			Data:   []models.DownloadManifestEntry{},
		}
		return h.buildResponse(resp, http.StatusOK)
	}

	presignClient := s3.NewPresignClient(S3Client)

	var entries []models.DownloadManifestEntry
	var totalSize int64
	bucketNameToOptions := map[string]BucketOptions{}
	presignDuration := 180 * time.Minute

	for _, row := range rows {
		s3Bucket := row.S3Bucket
		getObjectInput := s3.GetObjectInput{
			Bucket:                     aws.String(s3Bucket),
			Key:                        aws.String(row.S3Key),
			VersionId:                  row.PublishedS3VersionId,
			ResponseContentDisposition: aws.String(fmt.Sprintf(`attachment; filename="%s"`, row.PackageName)),
		}
		bucketOptions, found := bucketNameToOptions[s3Bucket]
		if !found {
			bucketOptions = BucketOptions{Region: regions.ForBucket(s3Bucket)}
			if roleARN, isExternal := h.externalBucketConfig[s3Bucket]; isExternal {
				credentialsProvider := aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(STSClient, roleARN, func(options *stscreds.AssumeRoleOptions) {
					options.RoleSessionName = "packages-service-presign-session"
					options.Duration = presignDuration
					options.Policy = aws.String(`{
        				"Version": "2012-10-17",
        				"Statement": [
            				{
                				"Effect": "Allow",
                				"Action": [
                    				"s3:GetObject",
                    				"s3:GetObjectVersion"
                				],
								"Resource": "*"
            				}
        				]
					}`)
				}))
				bucketOptions.CredentialsProvider = credentialsProvider
			}
			bucketNameToOptions[s3Bucket] = bucketOptions
		}
		// it's an external bucket, so set RequestPayer
		if bucketOptions.CredentialsProvider != nil {
			getObjectInput.RequestPayer = types.RequestPayerRequester
		}

		presignResult, err := presignClient.PresignGetObject(ctx, &getObjectInput, s3.WithPresignExpires(presignDuration),
			func(options *s3.PresignOptions) {
				options.ClientOptions = append(options.ClientOptions, func(options *s3.Options) {
					options.Region = bucketOptions.Region
					if bucketOptions.CredentialsProvider != nil {
						options.Credentials = bucketOptions.CredentialsProvider
					}
				})
			})
		if err != nil {
			h.logger.Errorf("failed to generate presigned URL for bucket=%s key=%s: %v", s3Bucket, row.S3Key, err)
			return nil, fmt.Errorf("failed to generate presigned URL: %w", err)
		}

		// Path construction: single-file packages use only parent names,
		// multi-file packages append the package's own name.
		var path []string
		if row.PackageFileCount == 1 {
			path = row.PackageNamePath
		} else {
			path = append(row.PackageNamePath, row.PackageName)
		}
		if path == nil {
			path = []string{}
		}

		entries = append(entries, models.DownloadManifestEntry{
			NodeId:        row.NodeId,
			FileName:      row.FileName,
			PackageName:   row.PackageName,
			Path:          path,
			URL:           presignResult.URL,
			Size:          row.Size,
			FileExtension: getFullExtension(row.S3Key),
		})
		totalSize += row.Size
	}

	resp := models.DownloadManifestResponse{
		Header: models.DownloadManifestHeader{
			Count: len(entries),
			Size:  totalSize,
		},
		Data: entries,
	}

	h.logger.Infof("download manifest: %d files, %d bytes for %d requested packages", len(entries), totalSize, len(request.NodeIds))
	return h.buildResponse(resp, http.StatusOK)
}

// getPackageHierarchy runs the recursive CTE to resolve package node IDs into
// file-level rows with S3 locations, scoped to the given dataset.
func (h *DownloadManifestHandler) getPackageHierarchy(ctx context.Context, orgId int, datasetNodeId string, nodeIds []string) ([]models.PackageHierarchyRow, error) {
	query := fmt.Sprintf(`
		WITH RECURSIVE parents AS (
			SELECT
				id, parent_id, dataset_id, name, type, node_id, size, state,
				ARRAY[]::VARCHAR[] AS node_id_path,
				ARRAY[]::VARCHAR[] AS name_path
			FROM "%[1]d".packages
			WHERE node_id = ANY($1::text[])
			AND dataset_id = (SELECT id FROM "%[1]d".datasets WHERE node_id = $2)

			UNION ALL

			SELECT
				children.id, children.parent_id, children.dataset_id, children.name,
				children.type, children.node_id, children.size, children.state,
				(parents.node_id_path || parents.node_id)::VARCHAR[],
				(parents.name_path || parents.name)::VARCHAR[]
			FROM "%[1]d".packages children
			INNER JOIN parents ON parents.id = children.parent_id
		)
		SELECT
			parents.dataset_id,
			parents.node_id_path,
			parents.id AS package_id,
			parents.node_id,
			parents.type AS package_type,
			parents.state AS package_state,
			parents.name_path AS package_name_path,
			parents.name AS package_name,
			f_count.package_file_count,
			f.id AS file_id,
			f.name AS file_name,
			f.size,
			f.file_type,
			f.s3_bucket,
			f.s3_key,
            f.published_s3_version_id
		FROM parents
		JOIN "%[1]d".files f ON f.package_id = parents.id
		JOIN (
			SELECT package_id, count(*) AS package_file_count
			FROM "%[1]d".files
			WHERE object_type = 'source'
			GROUP BY package_id
		) AS f_count ON f_count.package_id = parents.id
		WHERE parents.type != 'Collection'
		AND parents.state != 'DELETING'
		AND parents.state != 'DELETED'
		AND f.object_type = 'source'`, orgId)

	dbRows, err := PennsieveDB.QueryContext(ctx, query, pq.Array(nodeIds), datasetNodeId)
	if err != nil {
		return nil, fmt.Errorf("package hierarchy query failed: %w", err)
	}
	defer dbRows.Close()

	var results []models.PackageHierarchyRow
	for dbRows.Next() {
		var row models.PackageHierarchyRow
		if err := dbRows.Scan(
			&row.DatasetId,
			pq.Array(&row.NodeIdPath),
			&row.PackageId,
			&row.NodeId,
			&row.PackageType,
			&row.PackageState,
			pq.Array(&row.PackageNamePath),
			&row.PackageName,
			&row.PackageFileCount,
			&row.FileId,
			&row.FileName,
			&row.Size,
			&row.FileType,
			&row.S3Bucket,
			&row.S3Key,
			&row.PublishedS3VersionId,
		); err != nil {
			return nil, fmt.Errorf("failed to scan hierarchy row: %w", err)
		}
		results = append(results, row)
	}
	if err := dbRows.Err(); err != nil {
		return nil, fmt.Errorf("hierarchy row iteration error: %w", err)
	}

	return results, nil
}

// Common multi-dot extensions from the Pennsieve file type map.
var multiDotExtensions = []string{
	".ome.tiff", ".ome.tif", ".ome.tf2", ".ome.tf8", ".ome.btf", ".ome.xml",
	".nii.gz", ".tar.gz", ".brukertiff.gz", ".mefd.gz", ".mgh.gz",
}

// getFullExtension returns the file extension without the leading dot.
// It checks known multi-dot extensions first, then falls back to filepath.Ext.
func getFullExtension(fileName string) string {
	lower := strings.ToLower(fileName)
	best := ""
	for _, ext := range multiDotExtensions {
		if strings.HasSuffix(lower, ext) && len(ext) > len(best) {
			best = ext
		}
	}
	if best != "" {
		return best[1:] // strip leading dot
	}
	ext := filepath.Ext(fileName)
	if ext != "" {
		return ext[1:]
	}
	return ""
}
