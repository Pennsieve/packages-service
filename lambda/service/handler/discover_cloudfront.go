package handler

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/lib/pq"
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

// discoverScopeAll is the `scope` query value that makes dataset mode return
// every asset in the dataset, not only the ones with no package links.
const discoverScopeAll = "all"

// publishSucceededStatus is the discover.public_dataset_versions status of a
// version that finished publishing (pennsieve-api PublishStatus.PublishSucceeded).
const publishSucceededStatus = "PUBLISH_SUCCEEDED"

// linkedPackageNodeIDsColumn selects, for each viewer asset row `va`, the node IDs
// of the packages it is linked to (empty array when none), so a list query needs
// no follow-up query per asset. %[1]d is the organization ID.
const linkedPackageNodeIDsColumn = `COALESCE((
				SELECT array_agg(p.node_id ORDER BY p.node_id)
				FROM "%[1]d".packages p
				JOIN "%[1]d".viewer_asset_packages vap2 ON p.id = vap2.package_id
				WHERE vap2.viewer_asset_id = va.id
			), '{}') AS package_ids`

// handleListAssets lists viewer assets for a published package or dataset (unauthenticated).
// Accepts exactly one of:
//   - package_id: the source package node ID (returns assets linked to that package)
//   - dataset_id: the published-dataset PK from discover.public_datasets.id. By default
//     it returns dataset-scoped assets (those with no package links). With scope=all it
//     returns every asset in the dataset: those with no package links plus those linked
//     to a package that is in a successfully published version of the dataset. In both
//     cases chat-scoped assets are excluded.
func (h *DiscoverCloudFrontSignedURLHandler) handleListAssets(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	packageNodeID := h.queryParams["package_id"]
	publishedDatasetIDStr := h.queryParams["dataset_id"]
	scope := h.queryParams["scope"]

	if (packageNodeID == "") == (publishedDatasetIDStr == "") {
		return h.logAndBuildError("exactly one of 'package_id' or 'dataset_id' is required", http.StatusBadRequest), nil
	}
	if scope != "" && scope != discoverScopeAll {
		return h.logAndBuildError("'scope' must be 'all' when supplied", http.StatusBadRequest), nil
	}
	if scope == discoverScopeAll && packageNodeID != "" {
		return h.logAndBuildError("'scope' is only supported with 'dataset_id'", http.StatusBadRequest), nil
	}

	var (
		orgID              int64
		datasetIntID       int64
		packageIntID       int64
		publishedDatasetID int64
		datasetNodeID      string
		query              string
		queryArg           interface{}
		err                error
	)

	if packageNodeID != "" {
		h.logger.LogInfoWithFields(logging.Fields{logging.KeyPackageNodeID: packageNodeID},
			"handling GET request for discover viewer assets")
		orgID, datasetIntID, packageIntID, err = h.resolveDiscoverPackage(ctx, packageNodeID)
		if err != nil {
			return h.logAndBuildErrorCause("failed to resolve published package", http.StatusNotFound, err), nil
		}
		query = fmt.Sprintf(`
			SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at,
			`+linkedPackageNodeIDsColumn+`
			FROM "%[1]d".viewer_assets va
			JOIN "%[1]d".viewer_asset_packages vap ON va.id = vap.viewer_asset_id
			WHERE vap.package_id = $1
			ORDER BY va.created_at DESC
		`, orgID)
		queryArg = packageIntID
	} else {
		var parseErr error
		publishedDatasetID, parseErr = strconv.ParseInt(publishedDatasetIDStr, 10, 64)
		if parseErr != nil {
			return h.logAndBuildError("'dataset_id' must be a positive integer (the published-dataset ID)", http.StatusBadRequest), nil
		}
		h.logger.LogInfoWithFields(logging.Fields{logging.KeyPublishedDatasetID: publishedDatasetID},
			"handling GET request for discover viewer assets by dataset")
		orgID, datasetIntID, err = h.resolveDiscoverDataset(ctx, publishedDatasetID)
		if err != nil {
			return h.logAndBuildErrorCause("failed to resolve published dataset", http.StatusNotFound, err), nil
		}
		// Default: only assets with no package links. scope=all: every asset; the
		// ones linked to unpublished packages are dropped below.
		packageFilter := `
			  AND NOT EXISTS (
			    SELECT 1 FROM "%[1]d".viewer_asset_packages vap WHERE vap.viewer_asset_id = va.id
			  )`
		if scope == discoverScopeAll {
			packageFilter = ""
		}
		query = fmt.Sprintf(`
			SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at,
			`+linkedPackageNodeIDsColumn+`
			FROM "%[1]d".viewer_assets va
			WHERE va.dataset_id = $1
			  AND va.chat_session_id IS NULL`+packageFilter+`
			ORDER BY va.created_at DESC
		`, orgID)
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

	type assetWithPackages struct {
		row    viewerAssetRow
		pkgIDs []string
	}
	var found []assetWithPackages
	for rows.Next() {
		var a assetWithPackages
		if err := rows.Scan(&a.row.ID, &a.row.DatasetID, &a.row.Name, &a.row.AssetType, &a.row.Properties,
			&a.row.S3Bucket, &a.row.Status, &a.row.CreatedAt, pq.Array(&a.pkgIDs)); err != nil {
			return h.logAndBuildErrorCause("failed to scan asset row", http.StatusInternalServerError, err), nil
		}
		found = append(found, a)
	}
	if err := rows.Err(); err != nil {
		return h.logAndBuildErrorCause("failed to read asset rows", http.StatusInternalServerError, err), nil
	}

	// scope=all includes package-linked assets, which must only be public when the
	// package was published. Keep an asset if it has no package links or at least
	// one published linked package, and report only the published links.
	if scope == discoverScopeAll {
		var linked []string
		for _, a := range found {
			linked = append(linked, a.pkgIDs...)
		}
		published, err := h.publishedPackageNodeIDs(ctx, publishedDatasetID, linked)
		if err != nil {
			return h.logAndBuildErrorCause("failed to look up published packages", http.StatusInternalServerError, err), nil
		}
		kept := found[:0]
		for _, a := range found {
			if len(a.pkgIDs) == 0 {
				kept = append(kept, a)
				continue
			}
			var publishedIDs []string
			for _, id := range a.pkgIDs {
				if published[id] {
					publishedIDs = append(publishedIDs, id)
				}
			}
			if len(publishedIDs) > 0 {
				a.pkgIDs = publishedIDs
				kept = append(kept, a)
			}
		}
		found = kept
	}

	assets := make([]viewerAssetResponse, 0, len(found))
	var pathPrefixes []string
	for _, f := range found {
		a := f.row
		pkgIDs := f.pkgIDs
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

// publishedPackageNodeIDs returns which of the given package node IDs have a file
// in a successfully published version of the published dataset (an embargoed or
// failed version does not count). Older versions stay public after a new one is
// published, so any published version qualifies, not only the latest.
// publishedDatasetID is discover.public_datasets.id.
func (h *DiscoverCloudFrontSignedURLHandler) publishedPackageNodeIDs(ctx context.Context, publishedDatasetID int64, packageNodeIDs []string) (map[string]bool, error) {
	published := map[string]bool{}
	if len(packageNodeIDs) == 0 {
		return published, nil
	}
	if DiscoverDB == nil {
		return nil, fmt.Errorf("discover database not configured")
	}

	query := `
		SELECT DISTINCT fv.source_package_id
		FROM discover.public_file_versions fv
		JOIN discover.public_dataset_version_files vf
		  ON vf.file_id = fv.id AND vf.dataset_id = fv.dataset_id
		JOIN discover.public_dataset_versions v
		  ON v.dataset_id = vf.dataset_id AND v.version = vf.dataset_version
		WHERE fv.dataset_id = $1
		  AND v.status = $2
		  AND fv.source_package_id = ANY($3::text[])
	`
	rows, err := DiscoverDB.QueryContext(ctx, query, publishedDatasetID, publishSucceededStatus, pq.Array(packageNodeIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query published packages: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		published[nodeID] = true
	}
	return published, rows.Err()
}
