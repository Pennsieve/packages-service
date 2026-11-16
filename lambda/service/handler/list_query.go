package handler

import (
	"context"
	"fmt"
	"strings"
)

// listMode says which of a dataset's viewer assets a list query returns.
type listMode int

const (
	// listByPackage returns the assets linked to one package.
	listByPackage listMode = iota
	// listDatasetUnlinked returns a dataset's assets that have no package links.
	listDatasetUnlinked
	// listDatasetAll returns every asset in a dataset, linked or not.
	listDatasetAll
)

// viewerAssetStatusReady is the status of an asset whose upload has finished.
const viewerAssetStatusReady = "ready"

// listQuery describes a viewer-asset list. The authenticated list (GET /assets)
// and the public one (GET /discover/assets) both build their SQL from it, so a
// new filter only has to be added here.
type listQuery struct {
	orgID, datasetID, packageID int64
	mode                        listMode
	excludeChat, readyOnly      bool
}

// build returns the list SQL and its arguments. It selects the columns
// queryViewerAssets scans, newest asset first.
func (q listQuery) build() (string, []any) {
	var join string
	var conditions []string
	var args []any

	// param adds a query argument and returns its placeholder: $1, $2, ...
	param := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}

	switch q.mode {
	case listByPackage:
		join = fmt.Sprintf(`JOIN "%[1]d".viewer_asset_packages vap ON va.id = vap.viewer_asset_id`, q.orgID)
		conditions = append(conditions, "vap.package_id = "+param(q.packageID))
	case listDatasetUnlinked:
		conditions = append(conditions,
			"va.dataset_id = "+param(q.datasetID),
			fmt.Sprintf(`NOT EXISTS (SELECT 1 FROM "%[1]d".viewer_asset_packages vap WHERE vap.viewer_asset_id = va.id)`, q.orgID))
	case listDatasetAll:
		conditions = append(conditions, "va.dataset_id = "+param(q.datasetID))
	}

	if q.excludeChat {
		conditions = append(conditions, "va.chat_session_id IS NULL")
	}
	if q.readyOnly {
		conditions = append(conditions, "va.status = "+param(viewerAssetStatusReady))
	}

	sql := fmt.Sprintf(`
		SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at
		FROM "%[1]d".viewer_assets va
		%[2]s
		WHERE %[3]s
		ORDER BY va.created_at DESC
	`, q.orgID, join, strings.Join(conditions, "\n\t\t  AND "))
	return sql, args
}

// queryViewerAssets runs a list query and returns every row. The rows are
// closed before it returns, so callers can run follow-up queries (like the
// package lookup) without holding a second DB connection open.
func queryViewerAssets(ctx context.Context, q listQuery) ([]viewerAssetRow, error) {
	query, args := q.build()
	rows, err := PennsieveDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assets []viewerAssetRow
	for rows.Next() {
		var a viewerAssetRow
		if err := rows.Scan(&a.ID, &a.DatasetID, &a.Name, &a.AssetType, &a.Properties, &a.S3Bucket, &a.Status, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan asset row: %w", err)
		}
		assets = append(assets, a)
	}
	return assets, rows.Err()
}

func viewerAssetIDs(assets []viewerAssetRow) []string {
	ids := make([]string, len(assets))
	for i, a := range assets {
		ids[i] = a.ID
	}
	return ids
}
