package regions

import "strings"

func ForBucket(bucketName string) string {
	if strings.HasSuffix(bucketName, "-use1") {
		return "us-east-1"
	}
	if strings.HasSuffix(bucketName, "-afs1") {
		return "af-south-1"
	}

	return "us-east-1"

}
