package store

import "github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"

var filesColumns = []string{"id", "package_id", "size", "object_type", "published_s3_version_id", "scan_status"}

type File struct {
	ID                   int
	PackageId            int64
	Size                 int64
	ObjectType           objectType.ObjectType
	PublishedS3VersionID *string
	// ScanStatus is files.scan_status as scan-service writes it
	// (pending, clean, format_validated, unscanned, infected, failed, ...).
	ScanStatus string
}

func (f File) IsPublished() bool {
	return f.PublishedS3VersionID != nil
}

type FilesScanner struct {
	*ModelScanner
}

func (f FilesScanner) Scan(scanner RowScanner, file *File) error {
	var objectTypeString string
	err := scanner.Scan(
		&file.ID,
		&file.PackageId,
		&file.Size,
		&objectTypeString,
		&file.PublishedS3VersionID,
		&file.ScanStatus)
	objType, ok := objectType.Dict[objectTypeString]
	if !ok {
		// this is the default for an unknown type in objectType.String()
		objType = objectType.File
	}
	file.ObjectType = objType
	return err
}

func (f FilesScanner) JoinScan(scanner RowScanner, packageNodeId *string, file *File) error {
	var objectTypeString string
	err := scanner.Scan(
		packageNodeId,
		&file.ID,
		&file.PackageId,
		&file.Size,
		&objectTypeString,
		&file.PublishedS3VersionID,
		&file.ScanStatus)
	objType, ok := objectType.Dict[objectTypeString]
	if !ok {
		// this is the default for an unknown type in objectType.String()
		objType = objectType.File
	}
	file.ObjectType = objType
	return err
}

var filesScanner = FilesScanner{NewModelScanner("files", filesColumns)}
