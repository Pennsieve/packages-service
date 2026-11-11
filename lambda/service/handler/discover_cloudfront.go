package handler

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/packages-service/api/logging"
)

type DiscoverCloudFrontSignedURLHandler struct {
	RequestHandler
}

func (h *DiscoverCloudFrontSignedURLHandler) handleOptions(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	headers := map[string]string{
		"Access-Control-Allow-Origin":  "*",
		"Access-Control-Allow-Methods": "GET, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type, Origin, Accept",
		"Access-Control-Max-Age":       "3600",
	}
	return &events.APIGatewayV2HTTPResponse{
		StatusCode: http.StatusNoContent,
		Headers:    headers,
	}, nil
}

// handleListAssets lists viewer assets for a published package or dataset (unauthenticated).
// Accepts exactly one of:
//   - package_id: the source package node ID (returns assets linked to that package)
//   - dataset_id: the published-dataset PK from discover.public_datasets.id (returns
//     dataset-scoped assets — those with no package links)
func (h *DiscoverCloudFrontSignedURLHandler) handleListAssets(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	packageNodeID := h.queryParams["package_id"]
	publishedDatasetIDStr := h.queryParams["dataset_id"]

	if (packageNodeID == "") == (publishedDatasetIDStr == "") {
		return h.logAndBuildError("exactly one of 'package_id' or 'dataset_id' is required", http.StatusBadRequest), nil
	}

	var (
		orgID         int64
		datasetIntID  int64
		packageIntID  int64
		datasetNodeID string
		query         string
		queryArg      interface{}
		err           error
	)

	if packageNodeID != "" {
		h.logger.LogInfoWithFields(logging.Fields{logging.KeyPackageNodeID: packageNodeID},
			"handling GET request for discover viewer assets")
		orgID, datasetIntID, packageIntID, err = h.resolveDiscoverPackage(ctx, packageNodeID)
		if err != nil {
			return h.logAndBuildErrorCause("failed to resolve published package", http.StatusNotFound, err), nil
		}
		query = fmt.Sprintf(`
			SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at
			FROM "%d".viewer_assets va
			JOIN "%d".viewer_asset_packages vap ON va.id = vap.viewer_asset_id
			WHERE vap.package_id = $1
			ORDER BY va.created_at DESC
		`, orgID, orgID)
		queryArg = packageIntID
	} else {
		publishedDatasetID, parseErr := strconv.ParseInt(publishedDatasetIDStr, 10, 64)
		if parseErr != nil {
			return h.logAndBuildError("'dataset_id' must be a positive integer (the published-dataset ID)", http.StatusBadRequest), nil
		}
		h.logger.LogInfoWithFields(logging.Fields{logging.KeyPublishedDatasetID: publishedDatasetID},
			"handling GET request for discover viewer assets by dataset")
		orgID, datasetIntID, err = h.resolveDiscoverDataset(ctx, publishedDatasetID)
		if err != nil {
			return h.logAndBuildErrorCause("failed to resolve published dataset", http.StatusNotFound, err), nil
		}
		query = fmt.Sprintf(`
			SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at
			FROM "%d".viewer_assets va
			WHERE va.dataset_id = $1
			  AND NOT EXISTS (
			    SELECT 1 FROM "%d".viewer_asset_packages vap WHERE vap.viewer_asset_id = va.id
			  )
			ORDER BY va.created_at DESC
		`, orgID, orgID)
		queryArg = datasetIntID
	}

	// Resolve dataset node ID for the response
	dsQuery := fmt.Sprintf(`SELECT node_id FROM "%d".datasets WHERE id = $1`, orgID)
	if err := PennsieveDB.QueryRowContext(ctx, dsQuery, datasetIntID).Scan(&datasetNodeID); err != nil {
		return h.logAndBuildErrorCause("failed to resolve dataset node ID", http.StatusInternalServerError, err), nil
	}

	rows, err := PennsieveDB.QueryContext(ctx, query, queryArg)
	if err != nil {
		return h.logAndBuildErrorCause("failed to query assets", http.StatusInternalServerError, err), nil
	}
	defer rows.Close()

	var assets []viewerAssetResponse
	var pathPrefixes []string
	for rows.Next() {
		var a viewerAssetRow
		if err := rows.Scan(&a.ID, &a.DatasetID, &a.Name, &a.AssetType, &a.Properties, &a.S3Bucket, &a.Status, &a.CreatedAt); err != nil {
			return h.logAndBuildErrorCause("failed to scan asset row", http.StatusInternalServerError, err), nil
		}

		// Get linked package node IDs
		pkgQuery := fmt.Sprintf(`
			SELECT p.node_id FROM "%d".packages p
			JOIN "%d".viewer_asset_packages vap ON p.id = vap.package_id
			WHERE vap.viewer_asset_id = $1
		`, orgID, orgID)
		pkgRows, err := PennsieveDB.QueryContext(ctx, pkgQuery, a.ID)
		var pkgIDs []string
		if err == nil {
			for pkgRows.Next() {
				var nodeID string
				if pkgRows.Scan(&nodeID) == nil {
					pkgIDs = append(pkgIDs, nodeID)
				}
			}
			pkgRows.Close()
		}
		if pkgIDs == nil {
			pkgIDs = []string{}
		}
		pathPrefixes = append(pathPrefixes, cloudFrontPathPrefixForBucket(a.S3Bucket))

		assets = append(assets, viewerAssetResponse{
			ID:         a.ID,
			DatasetID:  datasetNodeID,
			Name:       a.Name,
			AssetType:  a.AssetType,
			AssetURL:   viewerAssetURL(orgID, datasetIntID, a.ID, a.S3Bucket),
			Properties: &a.Properties,
			Status:     a.Status,
			PackageIDs: pkgIDs,
			CreatedAt:  a.CreatedAt.Format(time.RFC3339),
		})
	}
	if assets == nil {
		assets = []viewerAssetResponse{}
	}

	resp := listViewerAssetsResponse{Assets: assets}

	// Load CloudFront signing keys if not yet cached
	if cloudfrontDistributionDomain != "" && cloudfrontPrivateKey == nil {
		if secretName, ok := os.LookupEnv("CLOUDFRONT_SIGNING_KEYS_SECRET_NAME"); ok {
			authHandler := CloudFrontSignedURLHandler{RequestHandler: h.RequestHandler}
			if err := authHandler.loadKeysFromSecretsManager(ctx, secretName); err != nil {
				h.logger.LogWarnWithFields(logging.Fields{logging.KeyError: err, logging.KeySecretName: secretName},
					"failed to load CloudFront signing keys")
			}
		}
	}

	// Generate CloudFront signed policy covering every bucket the assets are in
	var cookies []string
	if cloudfrontDistributionDomain != "" && cloudfrontPrivateKey != nil {
		if len(pathPrefixes) == 0 {
			defaultPrefix, err := defaultViewerAssetPathPrefix(ctx, orgID)
			if err != nil {
				h.logger.LogWarnWithFields(logging.Fields{
					logging.KeyError:          err,
					logging.KeyOrganizationID: orgID,
				}, "could not resolve organization storage bucket; signing for the default viewer assets bucket")
			}
			pathPrefixes = []string{defaultPrefix}
		}
		components, err := signViewerAssetsPolicy(orgID, datasetIntID, pathPrefixes)
		if err == nil {
			resp.CloudFront = components
			cookieDomain := "." + os.Getenv("PENNSIEVE_DOMAIN")
			cookies = components.CloudFrontCookies(cookieDomain)
		} else {
			h.logger.LogWarnWithFields(logging.Fields{
				logging.KeyError:          err,
				logging.KeyOrganizationID: orgID,
				logging.KeyDatasetID:      datasetIntID,
			}, "failed to sign CloudFront policy for discover viewer assets")
		}
	}

	apiResp, err := h.buildResponse(resp, http.StatusOK)
	if err != nil {
		return nil, err
	}
	if len(cookies) > 0 {
		apiResp.Cookies = cookies
	}
	return apiResp, nil
}

// resolveDiscoverPackage validates that a package is published via the Discover DB
// and returns the source org ID, dataset integer ID, and package integer ID.
func (h *DiscoverCloudFrontSignedURLHandler) resolveDiscoverPackage(ctx context.Context, packageNodeID string) (orgID, datasetIntID, packageIntID int64, err error) {
	if DiscoverDB == nil {
		return 0, 0, 0, fmt.Errorf("discover database not configured")
	}

	discoverQuery := `
		SELECT DISTINCT pd.source_organization_id, pd.source_dataset_id
		FROM discover.public_file_versions fv
		JOIN discover.public_datasets pd ON fv.dataset_id = pd.id
		WHERE fv.source_package_id = $1
		LIMIT 1
	`
	err = DiscoverDB.QueryRowContext(ctx, discoverQuery, packageNodeID).Scan(&orgID, &datasetIntID)
	if err != nil {
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:         err,
			logging.KeyPackageNodeID: packageNodeID,
		}, "failed to look up package in discover database")
		return 0, 0, 0, fmt.Errorf("published package not found")
	}

	pennsieveQuery := fmt.Sprintf(`SELECT id FROM "%d".packages WHERE node_id = $1`, orgID)
	err = PennsieveDB.QueryRowContext(ctx, pennsieveQuery, packageNodeID).Scan(&packageIntID)
	if err != nil {
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:         err,
			logging.KeyPackageNodeID: packageNodeID,
		}, "failed to get package integer ID")
		return 0, 0, 0, fmt.Errorf("package not found in platform")
	}

	return orgID, datasetIntID, packageIntID, nil
}

// resolveDiscoverDataset validates that a dataset is published via the Discover DB
// and returns the source org ID and source dataset integer ID.
// publishedDatasetID is discover.public_datasets.id (the published-dataset PK).
func (h *DiscoverCloudFrontSignedURLHandler) resolveDiscoverDataset(ctx context.Context, publishedDatasetID int64) (orgID, datasetIntID int64, err error) {
	if DiscoverDB == nil {
		return 0, 0, fmt.Errorf("discover database not configured")
	}

	discoverQuery := `
		SELECT source_organization_id, source_dataset_id
		FROM discover.public_datasets
		WHERE id = $1
	`
	err = DiscoverDB.QueryRowContext(ctx, discoverQuery, publishedDatasetID).Scan(&orgID, &datasetIntID)
	if err != nil {
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:              err,
			logging.KeyPublishedDatasetID: publishedDatasetID,
		}, "failed to look up dataset in discover database")
		return 0, 0, fmt.Errorf("published dataset not found")
	}

	return orgID, datasetIntID, nil
}
