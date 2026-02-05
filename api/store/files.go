package store

import "github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"

var filesColumns = []string{"id", "package_id", "size", "object_type", "published"}

type File struct {
	ID         int
	PackageId  int64
	Size       int64
	ObjectType objectType.ObjectType
	Published  bool
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
		&file.Published)
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
		&file.Published)
	objType, ok := objectType.Dict[objectTypeString]
	if !ok {
		// this is the default for an unknown type in objectType.String()
		objType = objectType.File
	}
	file.ObjectType = objType
	return err
}

var filesScanner = FilesScanner{NewModelScanner("files", filesColumns)}
