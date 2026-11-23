package handler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// listMode says which of a dataset's viewer assets a list query returns.
type listMode int

// Modes start at 1 so a listQuery with no mode set is an error in build(),
// instead of quietly running as package mode.
const (
	// listByPackage returns the assets linked to one package.
	listByPackage listMode = iota + 1
	// listDatasetUnlinked returns a dataset's assets that have no package links.
	listDatasetUnlinked
	// listDatasetAll returns every asset in a dataset, linked or not.
	listDatasetAll
)

// The two statuses a viewer asset can have.
const (
	// viewerAssetStatusCreated is a new asset whose upload hasn't finished.
	viewerAssetStatusCreated = "created"
	// viewerAssetStatusReady is an asset whose upload has finished.
	viewerAssetStatusReady = "ready"
)

// isValidAssetStatus reports whether s is one of the two asset statuses. The
// match is exact: "Ready" is not valid.
func isValidAssetStatus(s string) bool {
	return s == viewerAssetStatusCreated || s == viewerAssetStatusReady
}

// Query params shared by GET /assets and GET /discover/assets.
const (
	// listScopeAll is the only `scope` value: every asset in the dataset, not
	// only the ones with no package links.
	listScopeAll = "all"
	// scopeAllDefaultLimit is the page size of a scope=all list with no `limit`.
	scopeAllDefaultLimit = 100
	// maxListLimit is the largest `limit` a caller may ask for.
	maxListLimit = 500
)

// checkScope returns an error when the `scope` param is not allowed.
func checkScope(scope, packageNodeID string) error {
	if scope != "" && scope != listScopeAll {
		return errors.New("'scope' must be 'all' when supplied")
	}
	if scope == listScopeAll && packageNodeID != "" {
		return errors.New("'scope' can't be combined with 'package_id'")
	}
	return nil
}

// parsePage reads the `limit` and `offset` params. A limit of 0 means "no
// limit". scope=all always pages (100 by default); the other modes only page
// when the caller asks, so existing callers get the same list as before.
func parsePage(params map[string]string, scope string) (limit, offset int, err error) {
	if scope == listScopeAll {
		limit = scopeAllDefaultLimit
	}
	if s, ok := params["limit"]; ok {
		limit, err = strconv.Atoi(s)
		if err != nil || limit < 1 || limit > maxListLimit {
			return 0, 0, fmt.Errorf("'limit' must be a whole number from 1 to %d", maxListLimit)
		}
	}
	if s, ok := params["offset"]; ok {
		offset, err = strconv.Atoi(s)
		if err != nil || offset < 0 {
			return 0, 0, errors.New("'offset' must be a whole number, 0 or more")
		}
	}
	return limit, offset, nil
}

// livePackageSQL is true for a package that is not "removed": not in the trash
// (DELETING/DELETED) and not replaced by a newer upload. The viewer-asset code
// only defines "removed" here, so the list filter and the package lookups can't drift.
// alias is the SQL alias of the packages table in the query.
func livePackageSQL(alias string) string {
	return fmt.Sprintf(`(%[1]s.state NOT IN ('DELETING', 'DELETED') AND %[1]s.replaced_by_package_id IS NULL)`, alias)
}

// listQuery describes a viewer-asset list. The authenticated list (GET /assets)
// and the public one (GET /discover/assets) both build their SQL from it, so a
// new filter only has to be added here.
type listQuery struct {
	orgID int64
	// datasetID is the filter in the dataset modes. Package mode ignores it: the
	// caller has already checked that the package belongs to the dataset.
	datasetID int64
	// packageID is used by listByPackage only.
	packageID int64
	mode      listMode
	// excludeChat leaves out chat figures.
	excludeChat bool
	// assetType and status keep only assets with exactly this value. Empty means
	// no filter. Discover always sets status to ready (no unfinished uploads).
	assetType, status string
	// liveLinksOnly leaves out assets whose package links are all to removed
	// packages. Assets with no links stay. Used by the authenticated scope=all.
	liveLinksOnly bool
	// publishedLinksOnly keeps assets with no package links, or with at least
	// one link to a package in publishedPackageIDs. Used by discover scope=all,
	// so the filter runs before limit/offset, not after.
	publishedLinksOnly  bool
	publishedPackageIDs []int64
	// limit 0 means no limit.
	limit, offset int
}

// paged reports whether the caller asked for a page rather than the whole list.
// Only a paged list reports total_count, limit and offset.
func (q listQuery) paged() bool {
	return q.limit > 0 || q.offset > 0
}

// build returns the list SQL and its arguments. It selects the columns
// queryViewerAssets scans, newest asset first.
func (q listQuery) build() (string, []any, error) {
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
		join = fmt.Sprintf(`JOIN "%[1]d".%[2]s vap ON va.id = vap.viewer_asset_id`, q.orgID, viewerAssetPackagesTable)
		conditions = append(conditions, "vap.package_id = "+param(q.packageID))
	case listDatasetUnlinked:
		conditions = append(conditions,
			"va.dataset_id = "+param(q.datasetID),
			fmt.Sprintf(`NOT EXISTS (SELECT 1 FROM "%[1]d".%[2]s vap WHERE vap.viewer_asset_id = va.id)`, q.orgID, viewerAssetPackagesTable))
	case listDatasetAll:
		conditions = append(conditions, "va.dataset_id = "+param(q.datasetID))
	default:
		// A bug in the caller, not bad user input: every caller should set a mode.
		return "", nil, fmt.Errorf("listQuery: unknown mode %d", q.mode)
	}

	if q.excludeChat {
		conditions = append(conditions, "va.chat_session_id IS NULL")
	}
	if q.assetType != "" {
		conditions = append(conditions, "va.asset_type = "+param(q.assetType))
	}
	if q.status != "" {
		conditions = append(conditions, "va.status = "+param(q.status))
	}
	if q.liveLinksOnly {
		conditions = append(conditions, q.unlinkedOrLinkedWhere(livePackageSQL("p")))
	}
	if q.publishedLinksOnly {
		published := fmt.Sprintf("p.id = ANY(%s::int[])", param(pq.Array(q.publishedPackageIDs)))
		conditions = append(conditions, q.unlinkedOrLinkedWhere(published))
	}

	page := ""
	if q.limit > 0 {
		page += " LIMIT " + param(q.limit)
	}
	if q.offset > 0 {
		page += " OFFSET " + param(q.offset)
	}

	// COUNT(*) OVER() is counted before LIMIT/OFFSET, so every row carries the
	// total for the whole list. va.id breaks created_at ties so pages are stable.
	sql := fmt.Sprintf(`
		SELECT va.id, va.dataset_id, va.name, va.asset_type, va.properties, va.s3_bucket, va.status, va.created_at,
		       COUNT(*) OVER() AS total_count
		FROM "%[1]d".%[2]s va
		%[3]s
		WHERE %[4]s
		ORDER BY va.created_at DESC, va.id
		%[5]s
	`, q.orgID, viewerAssetsTable, join, strings.Join(conditions, "\n\t\t  AND "), page)
	return sql, args, nil
}

// unlinkedOrLinkedWhere returns a condition that keeps an asset if it has no
// package links, or at least one link to a package matching packageCondition.
// In packageCondition, `p` is the linked package. The link rows are aliased
// `link`, not `vap`, so they never clash with package mode's outer join.
func (q listQuery) unlinkedOrLinkedWhere(packageCondition string) string {
	return fmt.Sprintf(`(
		NOT EXISTS (SELECT 1 FROM "%[1]d".%[2]s link WHERE link.viewer_asset_id = va.id)
		OR EXISTS (
			SELECT 1 FROM "%[1]d".%[2]s link
			JOIN "%[1]d".packages p ON p.id = link.package_id
			WHERE link.viewer_asset_id = va.id AND %[3]s
		)
	)`, q.orgID, viewerAssetPackagesTable, packageCondition)
}

// queryViewerAssets runs a list query. It returns one page of rows and the
// total number of matching assets, ignoring limit and offset. The rows are
// closed before it returns, so callers can run follow-up queries (like the
// package lookup) without holding a second DB connection open.
func queryViewerAssets(ctx context.Context, q listQuery) ([]viewerAssetRow, int, error) {
	assets, total, err := runViewerAssetsQuery(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	// A page past the end has no rows, so no row carries the total. Ask for the
	// first row instead, just to read the total from it.
	if len(assets) == 0 && q.offset > 0 {
		first := q
		first.limit, first.offset = 1, 0
		_, total, err = runViewerAssetsQuery(ctx, first)
		if err != nil {
			return nil, 0, err
		}
	}
	return assets, total, nil
}

// runViewerAssetsQuery runs the built SQL once and scans every row.
func runViewerAssetsQuery(ctx context.Context, q listQuery) ([]viewerAssetRow, int, error) {
	query, args, err := q.build()
	if err != nil {
		return nil, 0, err
	}
	rows, err := PennsieveDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query viewer assets: %w", err)
	}
	defer rows.Close()

	var assets []viewerAssetRow
	total := 0
	for rows.Next() {
		var a viewerAssetRow
		if err := rows.Scan(&a.ID, &a.DatasetID, &a.Name, &a.AssetType, &a.Properties, &a.S3Bucket, &a.Status, &a.CreatedAt, &total); err != nil {
			return nil, 0, fmt.Errorf("failed to scan asset row: %w", err)
		}
		assets = append(assets, a)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to read viewer asset rows: %w", err)
	}
	return assets, total, nil
}

func viewerAssetIDs(assets []viewerAssetRow) []string {
	ids := make([]string, len(assets))
	for i, a := range assets {
		ids[i] = a.ID
	}
	return ids
}
