package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

// publishSucceededStatus is the discover.public_dataset_versions status of a
// version that finished publishing (pennsieve-api PublishStatus.PublishSucceeded).
const publishSucceededStatus = "PUBLISH_SUCCEEDED"

// handleListAssets lists viewer assets for a published package or dataset (unauthenticated).
// Accepts exactly one of:
//   - package_id: the source package node ID (returns assets linked to that package)
//   - dataset_id: the published-dataset PK from discover.public_datasets.id. By default
//     it returns dataset-scoped assets (those with no package links). With scope=all it
//     returns those plus the assets linked to a published package.
//
// Only the dataset's latest PUBLISH_SUCCEEDED version counts: a package is
// published only if it has a file in that version. Package links are trimmed
// to published packages, and each one gets its file path in that version.
//
// Every mode leaves out chat figures (they belong to a chat, not the dataset) and
// assets that are not ready yet (still uploading), since this list is public.
// Every mode also takes asset_type, limit and offset.
func (h *DiscoverCloudFrontSignedURLHandler) handleListAssets(ctx context.Context) (*events.APIGatewayV2HTTPResponse, error) {
	packageNodeID := h.queryParams["package_id"]
	publishedDatasetIDStr := h.queryParams["dataset_id"]
	scope := h.queryParams["scope"]

	if (packageNodeID == "") == (publishedDatasetIDStr == "") {
		return h.logAndBuildError("exactly one of 'package_id' or 'dataset_id' is required", http.StatusBadRequest), nil
	}
	if err := checkScope(scope, packageNodeID); err != nil {
		return h.logAndBuildError(err.Error(), http.StatusBadRequest), nil
	}
	limit, offset, err := parsePage(h.queryParams, scope)
	if err != nil {
		return h.logAndBuildError(err.Error(), http.StatusBadRequest), nil
	}

	var list discoverList
	var errResp *events.APIGatewayV2HTTPResponse
	if packageNodeID != "" {
		list, errResp = h.packageModeList(ctx, packageNodeID)
	} else {
		list, errResp = h.datasetModeList(ctx, publishedDatasetIDStr, scope)
	}
	if errResp != nil {
		return errResp, nil
	}

	list.q.assetType = h.queryParams["asset_type"]
	list.q.limit, list.q.offset = limit, offset
	return h.runDiscoverList(ctx, list)
}

// publishedFiles is what latestPublishedFiles found: the dataset's latest
// succeeded version, and the published path of each package's file in it.
type publishedFiles struct {
	version   int
	filePaths map[string]string
}

// discoverList is a discover list request once its mode has been resolved.
type discoverList struct {
	q                  listQuery
	publishedDatasetID int64
	// published is set by scope=all, which needs it before the list query.
	// The other modes leave it nil, and runDiscoverList reads it afterwards.
	published *publishedFiles
}

// packageModeList resolves a package_id request. On failure it returns the
// error response to send.
func (h *DiscoverCloudFrontSignedURLHandler) packageModeList(ctx context.Context, packageNodeID string) (discoverList, *events.APIGatewayV2HTTPResponse) {
	h.logger.LogInfoWithFields(logging.Fields{logging.KeyPackageNodeID: packageNodeID},
		"handling GET request for discover viewer assets")

	orgID, datasetIntID, packageIntID, publishedDatasetID, err := h.resolveDiscoverPackage(ctx, packageNodeID)
	if errors.Is(err, errPublishedPackageNotFound) {
		return discoverList{}, h.logAndBuildErrorCause("failed to resolve published package", http.StatusNotFound, err)
	}
	if err != nil {
		return discoverList{}, h.logAndBuildErrorCause("failed to look up published package", http.StatusInternalServerError, err)
	}

	q := listQuery{orgID: orgID, datasetID: datasetIntID, packageID: packageIntID, mode: listByPackage,
		excludeChat: true, status: viewerAssetStatusReady}
	return discoverList{q: q, publishedDatasetID: publishedDatasetID}, nil
}

// datasetModeList resolves a dataset_id request, with or without scope=all.
// On failure it returns the error response to send.
func (h *DiscoverCloudFrontSignedURLHandler) datasetModeList(ctx context.Context, publishedDatasetIDStr, scope string) (discoverList, *events.APIGatewayV2HTTPResponse) {
	publishedDatasetID, err := strconv.ParseInt(publishedDatasetIDStr, 10, 64)
	if err != nil {
		return discoverList{}, h.logAndBuildError("'dataset_id' must be a positive integer (the published-dataset ID)", http.StatusBadRequest)
	}
	h.logger.LogInfoWithFields(logging.Fields{logging.KeyPublishedDatasetID: publishedDatasetID},
		"handling GET request for discover viewer assets by dataset")

	orgID, datasetIntID, err := h.resolveDiscoverDataset(ctx, publishedDatasetID)
	if err != nil {
		return discoverList{}, h.logAndBuildErrorCause("failed to resolve published dataset", http.StatusNotFound, err)
	}

	// Default: only assets with no package links.
	list := discoverList{
		q: listQuery{orgID: orgID, datasetID: datasetIntID, mode: listDatasetUnlinked,
			excludeChat: true, status: viewerAssetStatusReady},
		publishedDatasetID: publishedDatasetID,
	}
	if scope != listScopeAll {
		return list, nil
	}

	// scope=all: the published filter goes into the SQL, so limit/offset count
	// only assets that will really be returned.
	publishedIDs, published, err := h.publishedLinkedPackageIDs(ctx, orgID, datasetIntID, publishedDatasetID)
	if err != nil {
		return discoverList{}, h.logAndBuildErrorCause("failed to look up published packages", http.StatusInternalServerError, err)
	}
	list.q.mode = listDatasetAll
	list.q.publishedLinksOnly = true
	list.q.publishedPackageIDs = publishedIDs
	list.published = published
	return list, nil
}

// runDiscoverList runs a resolved discover list and builds the response:
// assets, their published package links, paging fields and CloudFront signing.
func (h *DiscoverCloudFrontSignedURLHandler) runDiscoverList(ctx context.Context, list discoverList) (*events.APIGatewayV2HTTPResponse, error) {
	orgID, datasetIntID := list.q.orgID, list.q.datasetID

	var datasetNodeID string
	dsQuery := fmt.Sprintf(`SELECT node_id FROM "%d".datasets WHERE id = $1`, orgID)
	if err := PennsieveDB.QueryRowContext(ctx, dsQuery, datasetIntID).Scan(&datasetNodeID); err != nil {
		return h.logAndBuildErrorCause("failed to resolve dataset node ID", http.StatusInternalServerError, err), nil
	}

	found, total, err := queryViewerAssets(ctx, list.q)
	if err != nil {
		return h.logAndBuildErrorCause("failed to query assets", http.StatusInternalServerError, err), nil
	}

	linked, err := discoverLinkedPackages(ctx, list.q, found)
	if err != nil {
		return h.logAndBuildErrorCause("failed to look up linked packages", http.StatusInternalServerError, err), nil
	}

	// scope=all already has the files for every linked package in the dataset.
	// Reusing them means its filter and this trim agree on one version, even if
	// a new version finishes publishing in between.
	published := list.published
	if published == nil {
		version, filePaths, err := h.latestPublishedFiles(ctx, list.publishedDatasetID, linkedNodeIDs(linked))
		if err != nil {
			return h.logAndBuildErrorCause("failed to look up published packages", http.StatusInternalServerError, err), nil
		}
		published = &publishedFiles{version: version, filePaths: filePaths}
	}
	linked = keepPublishedRefs(linked, published.filePaths)

	assets := make([]viewerAssetResponse, 0, len(found))
	var pathPrefixes []string
	for _, a := range found {
		pathPrefixes = append(pathPrefixes, cloudFrontPathPrefixForBucket(a.S3Bucket))
		assets = append(assets, viewerAssetResponse{
			ID:         a.ID,
			DatasetID:  datasetNodeID,
			Name:       a.Name,
			AssetType:  a.AssetType,
			AssetURL:   viewerAssetURL(orgID, datasetIntID, a.ID, a.S3Bucket),
			Properties: &a.Properties,
			Status:     a.Status,
			PackageIDs: packageNodeIDs(linked[a.ID]),
			Packages:   packageResponses(linked[a.ID]),
			CreatedAt:  a.CreatedAt.Format(time.RFC3339),
		})
	}

	resp := listViewerAssetsResponse{Assets: assets, Version: published.version}
	resp.setPage(list.q, total)

	components, cookies := h.signViewerAssetsCloudFront(ctx, orgID, datasetIntID, pathPrefixes)
	if components != nil {
		resp.CloudFront = components
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

// discoverLinkedPackages returns the packages linked to each listed asset, before
// trimming to the published ones.
func discoverLinkedPackages(ctx context.Context, q listQuery, assets []viewerAssetRow) (map[string][]packageRef, error) {
	// The default dataset mode only lists assets with no links, so the lookup
	// could only come back empty. Skip the query.
	if q.mode == listDatasetUnlinked {
		return map[string][]packageRef{}, nil
	}
	// false: discover doesn't care whether the workspace package is in the
	// trash. A file that is still in the latest published version stays public.
	return packageRefsByAsset(ctx, q.orgID, viewerAssetIDs(assets), false)
}

// publishedLinkedPackageIDs returns the integer IDs of the dataset's
// asset-linked packages that are in the latest published version. scope=all
// keeps an asset only if it has no links or a link to one of these. It also
// returns what latestPublishedFiles found, so the caller can reuse it.
func (h *DiscoverCloudFrontSignedURLHandler) publishedLinkedPackageIDs(ctx context.Context, orgID, datasetIntID, publishedDatasetID int64) ([]int64, *publishedFiles, error) {
	linkedIDs, err := datasetLinkedPackageIDs(ctx, orgID, datasetIntID)
	if err != nil {
		return nil, nil, err
	}
	nodeIDs := make([]string, 0, len(linkedIDs))
	for nodeID := range linkedIDs {
		nodeIDs = append(nodeIDs, nodeID)
	}

	version, filePaths, err := h.latestPublishedFiles(ctx, publishedDatasetID, nodeIDs)
	if err != nil {
		return nil, nil, err
	}

	var published []int64
	for nodeID, id := range linkedIDs {
		if _, ok := filePaths[nodeID]; ok {
			published = append(published, id)
		}
	}

	// Usually a sign the dataset was published before version-file rows were
	// kept: its linked assets then quietly drop out. Say so in the logs.
	if len(linkedIDs) > 0 && len(published) == 0 {
		h.logger.LogWarnWithFields(logging.Fields{
			logging.KeyPublishedDatasetID: publishedDatasetID,
			logging.KeyCount:              len(linkedIDs),
		}, "scope=all: dataset has package-linked assets but none of the packages are in the latest published version")
	}
	return published, &publishedFiles{version: version, filePaths: filePaths}, nil
}

// datasetLinkedPackageIDs returns every package that a viewer asset in the
// dataset is linked to, as node ID -> integer ID. One query.
func datasetLinkedPackageIDs(ctx context.Context, orgID, datasetIntID int64) (map[string]int64, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT p.node_id, p.id
		FROM "%[1]d".%[2]s va
		JOIN "%[1]d".%[3]s vap ON vap.viewer_asset_id = va.id
		JOIN "%[1]d".packages p ON p.id = vap.package_id
		WHERE va.dataset_id = $1
	`, orgID, viewerAssetsTable, viewerAssetPackagesTable)

	rows, err := PennsieveDB.QueryContext(ctx, query, datasetIntID)
	if err != nil {
		return nil, fmt.Errorf("failed to query linked packages for dataset: %w", err)
	}
	defer rows.Close()

	ids := map[string]int64{}
	for rows.Next() {
		var nodeID string
		var id int64
		if err := rows.Scan(&nodeID, &id); err != nil {
			return nil, fmt.Errorf("failed to scan linked package: %w", err)
		}
		ids[nodeID] = id
	}
	return ids, rows.Err()
}

// linkedNodeIDs returns the node IDs of every package in linked.
func linkedNodeIDs(linked map[string][]packageRef) []string {
	var nodeIDs []string
	for _, refs := range linked {
		nodeIDs = append(nodeIDs, packageNodeIDs(refs)...)
	}
	return nodeIDs
}

// keepPublishedRefs trims each asset's links down to the packages in filePaths
// and fills in their file paths. An asset left with no links drops out of the map.
func keepPublishedRefs(linked map[string][]packageRef, filePaths map[string]string) map[string][]packageRef {
	kept := map[string][]packageRef{}
	for assetID, refs := range linked {
		for _, ref := range refs {
			path, published := filePaths[ref.NodeID]
			if !published {
				continue
			}
			ref.FilePath = path
			kept[assetID] = append(kept[assetID], ref)
		}
	}
	return kept
}

// uniqueStrings returns values without duplicates, keeping first-seen order.
func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	var unique []string
	for _, v := range values {
		if seen[v] {
			continue
		}
		seen[v] = true
		unique = append(unique, v)
	}
	return unique
}

// errPublishedPackageNotFound means there is no published package to list:
// it was never published, or isn't in its dataset's latest published version.
// Callers answer 404.
var errPublishedPackageNotFound = errors.New("published package not found")

// resolveDiscoverPackage checks that a package has a file in its dataset's
// latest published version, and returns the source org ID, dataset integer ID,
// package integer ID, and the published dataset ID (discover.public_datasets.id).
// A package only in a failed or an older version returns errPublishedPackageNotFound.
// Any other error is a failed lookup, not a missing package.
func (h *DiscoverCloudFrontSignedURLHandler) resolveDiscoverPackage(ctx context.Context, packageNodeID string) (orgID, datasetIntID, packageIntID, publishedDatasetID int64, err error) {
	if DiscoverDB == nil {
		return 0, 0, 0, 0, fmt.Errorf("discover database not configured")
	}

	discoverQuery := `
		SELECT pd.id, pd.source_organization_id, pd.source_dataset_id
		FROM discover.public_file_versions fv
		JOIN discover.public_datasets pd ON fv.dataset_id = pd.id
		WHERE fv.source_package_id = $1
		LIMIT 1
	`
	err = DiscoverDB.QueryRowContext(ctx, discoverQuery, packageNodeID).Scan(&publishedDatasetID, &orgID, &datasetIntID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, 0, 0, fmt.Errorf("%w: never published", errPublishedPackageNotFound)
	}
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to look up package in discover database: %w", err)
	}

	_, filePaths, err := h.latestPublishedFiles(ctx, publishedDatasetID, []string{packageNodeID})
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if _, published := filePaths[packageNodeID]; !published {
		return 0, 0, 0, 0, fmt.Errorf("%w: not in the latest published version", errPublishedPackageNotFound)
	}

	pennsieveQuery := fmt.Sprintf(`SELECT id FROM "%d".packages WHERE node_id = $1`, orgID)
	err = PennsieveDB.QueryRowContext(ctx, pennsieveQuery, packageNodeID).Scan(&packageIntID)
	if errors.Is(err, sql.ErrNoRows) {
		// Published, but the workspace row is gone (e.g. purged): nothing to list.
		return 0, 0, 0, 0, fmt.Errorf("%w: package not found in platform", errPublishedPackageNotFound)
	}
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to get package integer ID: %w", err)
	}

	return orgID, datasetIntID, packageIntID, publishedDatasetID, nil
}

// resolveDiscoverDataset validates that a dataset is published via the Discover DB
// and returns the source org ID and source dataset integer ID. The dataset must
// have at least one PUBLISH_SUCCEEDED version: a public_datasets row alone can
// be an embargoed or failed publish, whose assets must not be public. Package
// mode applies the same rule through latestPublishedFiles.
// publishedDatasetID is discover.public_datasets.id (the published-dataset PK).
func (h *DiscoverCloudFrontSignedURLHandler) resolveDiscoverDataset(ctx context.Context, publishedDatasetID int64) (orgID, datasetIntID int64, err error) {
	if DiscoverDB == nil {
		return 0, 0, fmt.Errorf("discover database not configured")
	}

	discoverQuery := `
		SELECT pd.source_organization_id, pd.source_dataset_id
		FROM discover.public_datasets pd
		WHERE pd.id = $1
		  AND EXISTS (
			SELECT 1 FROM discover.public_dataset_versions v
			WHERE v.dataset_id = pd.id AND v.status = $2
		  )
	`
	err = DiscoverDB.QueryRowContext(ctx, discoverQuery, publishedDatasetID, publishSucceededStatus).Scan(&orgID, &datasetIntID)
	if err != nil {
		h.logger.LogErrorWithFields(logging.Fields{
			logging.KeyError:              err,
			logging.KeyPublishedDatasetID: publishedDatasetID,
		}, "failed to look up dataset in discover database")
		return 0, 0, fmt.Errorf("published dataset not found")
	}

	return orgID, datasetIntID, nil
}

// latestPublishedFiles returns the dataset's latest PUBLISH_SUCCEEDED version and,
// for each given package that has a file in it, that file's published path.
// A package is published if and only if it is in filePaths. A dataset with no
// succeeded version gives version 0 and an empty map; that is not an error.
// publishedDatasetID is discover.public_datasets.id.
func (h *DiscoverCloudFrontSignedURLHandler) latestPublishedFiles(ctx context.Context, publishedDatasetID int64, nodeIDs []string) (version int, filePaths map[string]string, err error) {
	if DiscoverDB == nil {
		return 0, nil, fmt.Errorf("discover database not configured")
	}

	var migrated bool
	versionQuery := `
		SELECT version, migrated
		FROM discover.public_dataset_versions
		WHERE dataset_id = $1 AND status = $2
		ORDER BY version DESC
		LIMIT 1
	`
	err = DiscoverDB.QueryRowContext(ctx, versionQuery, publishedDatasetID, publishSucceededStatus).Scan(&version, &migrated)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, map[string]string{}, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("failed to look up latest published version: %w", err)
	}

	filePaths = map[string]string{}
	if len(nodeIDs) == 0 {
		return version, filePaths, nil
	}
	// A package is often linked to several assets (e.g. a thumb and a zarr), so
	// the same ID can come in more than once. Send each one only once.
	nodeIDs = uniqueStrings(nodeIDs)

	// DISTINCT ON keeps the first s3_key per package, so a package with
	// several files always gets the same path.
	filesQuery := `
		SELECT DISTINCT ON (fv.source_package_id) fv.source_package_id, fv.s3_key
		FROM discover.public_file_versions fv
		JOIN discover.public_dataset_version_files vf
		  ON vf.file_id = fv.id AND vf.dataset_id = fv.dataset_id
		WHERE fv.dataset_id = $1
		  AND vf.dataset_version = $2
		  AND fv.source_package_id = ANY($3::text[])
		ORDER BY fv.source_package_id, fv.s3_key
	`
	rows, err := DiscoverDB.QueryContext(ctx, filesQuery, publishedDatasetID, version, pq.Array(nodeIDs))
	if err != nil {
		return 0, nil, fmt.Errorf("failed to query published files: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var nodeID, s3Key string
		if err := rows.Scan(&nodeID, &s3Key); err != nil {
			return 0, nil, fmt.Errorf("failed to scan published file: %w", err)
		}
		filePaths[nodeID] = publishedFilePath(s3Key, publishedDatasetID, version, migrated)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("failed to read published files: %w", err)
	}
	return version, filePaths, nil
}

// publishedFilePath strips the storage prefix off a published file's s3_key,
// leaving its path inside the dataset (e.g. "files/scan.png"). The prefix is
// "{dataset id}/" for a migrated version and "{dataset id}/{version}/" for an
// older one. Mirrors discover-service S3Key.removeVersionPrefix.
func publishedFilePath(s3Key string, publishedDatasetID int64, version int, migrated bool) string {
	prefix := fmt.Sprintf("%d/%d/", publishedDatasetID, version)
	if migrated {
		prefix = fmt.Sprintf("%d/", publishedDatasetID)
	}
	return strings.TrimPrefix(s3Key, prefix)
}
