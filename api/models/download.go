package models

import "database/sql"

// DownloadRequest is the request body for POST /download-manifest.
type DownloadRequest struct {
	NodeIds []string `json:"nodeIds"`
}

// DownloadManifestResponse is the response for POST /download-manifest.
// Blocked is populated when files are excluded from download due to
// their scan status (currently: infected or failed). Clients should
// surface both arrays — Data entries are directly downloadable, and
// Blocked entries explain why the listed files were withheld.
type DownloadManifestResponse struct {
	Header  DownloadManifestHeader         `json:"header"`
	Data    []DownloadManifestEntry        `json:"data"`
	Blocked []DownloadManifestBlockedEntry `json:"blocked,omitempty"`
}

type DownloadManifestHeader struct {
	Count        int   `json:"count"`
	Size         int64 `json:"size"`
	BlockedCount int   `json:"blockedCount,omitempty"`
}

type DownloadManifestEntry struct {
	NodeId        string   `json:"nodeId"`
	FileName      string   `json:"fileName"`
	PackageName   string   `json:"packageName"`
	Path          []string `json:"path"`
	URL           string   `json:"url"`
	Size          int64    `json:"size"`
	FileExtension string   `json:"fileExtension,omitempty"`
	// ScanStatus is the current malware-scan state of the file
	// (see scan-service docs for the value vocabulary). Omitted if
	// the row predates the scan_status migration. "clean",
	// "unscanned", "pending", and similar pass-through values are
	// surfaced here so the UI can render an appropriate indicator;
	// "infected" / "failed" never appear in Data (they're in Blocked).
	ScanStatus string `json:"scanStatus,omitempty"`
}

// DownloadManifestBlockedEntry is returned for files that were
// excluded from Data because of a blocking scan status. It omits the
// presigned URL by design — the client should not attempt download.
type DownloadManifestBlockedEntry struct {
	NodeId      string `json:"nodeId"`
	FileName    string `json:"fileName"`
	PackageName string `json:"packageName"`
	ScanStatus  string `json:"scanStatus"`
}

// PackageHierarchyRow represents a single row from the recursive package hierarchy query.
type PackageHierarchyRow struct {
	DatasetId            int
	NodeIdPath           []string
	PackageId            int64
	NodeId               string
	PackageType          string
	PackageState         string
	PackageNamePath      []string
	PackageName          string
	PackageFileCount     int
	FileId               int64
	FileName             string
	Size                 int64
	FileType             string
	S3Bucket             string
	S3Key                string
	PublishedS3VersionId *string
	ScanStatus           sql.NullString
}
