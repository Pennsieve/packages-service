package models

// DownloadRequest is the request body for POST /download-manifest.
type DownloadRequest struct {
	NodeIds []string `json:"nodeIds"`
}

// DownloadManifestResponse is the response for POST /download-manifest.
type DownloadManifestResponse struct {
	Header DownloadManifestHeader  `json:"header"`
	Data   []DownloadManifestEntry `json:"data"`
}

type DownloadManifestHeader struct {
	Count int   `json:"count"`
	Size  int64 `json:"size"`
}

type DownloadManifestEntry struct {
	NodeId        string   `json:"nodeId"`
	FileName      string   `json:"fileName"`
	PackageName   string   `json:"packageName"`
	Path          []string `json:"path"`
	URL           string   `json:"url"`
	Size          int64    `json:"size"`
	FileExtension string   `json:"fileExtension,omitempty"`
}

// PackageHierarchyRow represents a single row from the recursive package hierarchy query.
type PackageHierarchyRow struct {
	DatasetId        int
	NodeIdPath       []string
	PackageId        int64
	NodeId           string
	PackageType      string
	PackageState     string
	PackageNamePath  []string
	PackageName      string
	PackageFileCount int
	FileId           int64
	FileName         string
	Size             int64
	FileType         string
	S3Bucket         string
	S3Key            string
}