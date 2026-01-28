package store

var filesColumns = []string{"id", "package_id", "size", "published"}

type File struct {
	ID        int
	PackageId int64
	Size      int64
	Published bool
}

type FilesScanner struct {
	*ModelScanner
}

func (f FilesScanner) Scan(scanner RowScanner, file *File) error {
	return scanner.Scan(
		&file.ID,
		&file.PackageId,
		&file.Size,
		&file.Published)
}

var filesScanner = FilesScanner{NewModelScanner("files", filesColumns)}
