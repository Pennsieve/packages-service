package main

import (
	"fmt"
	"net/url"
	"strings"
)

// contains is a simple string contains helper
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// extractBucketName extracts the bucket name from an S3 URL
func extractBucketName(parsedURL *url.URL) string {
	host := parsedURL.Host
	path := parsedURL.Path
	
	// Virtual-hosted-style URLs: bucket-name.s3.amazonaws.com
	// or bucket-name.s3.region.amazonaws.com
	if contains(host, ".s3.") || contains(host, ".s3-") {
		// The bucket name is the first part of the host
		parts := strings.Split(host, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	
	// Path-style URLs: s3.amazonaws.com/bucket-name/key
	// or s3.region.amazonaws.com/bucket-name/key
	if strings.HasPrefix(host, "s3.") || strings.HasPrefix(host, "s3-") {
		// The bucket name is the first part of the path
		if path != "" && path != "/" {
			// Remove leading slash
			if strings.HasPrefix(path, "/") {
				path = path[1:]
			}
			// Get the first path segment
			parts := strings.Split(path, "/")
			if len(parts) > 0 && parts[0] != "" {
				return parts[0]
			}
		}
	}
	
	return ""
}

func main() {
	testURL := "https://pennsieve-dev-storage-use1.s3.amazonaws.com/14b49597-25da-4f83-8705-a0cb56313817/2d901a56-de34-46ef-8b32-4aa72f4f75d2?response-content-disposition=attachment%3B%20filename%3D%22Seurat_final_toshare.Rds%22&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEIX%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJGMEQCICqTlZy%2Fmxy4psdo0qfDljFwfmaPTrNg%2BmEo1hCIAYgsAiAXSlvKFpF7oB20YWhOGgHFRDsoR6wwZx%2BiyIybNJ5Dayr%2FAwhOEAMaDDk0MTE2NTI0MDAxMSIMnraPLUFjdDTLdMCDKtwDNx7gAVEAHXBcNTakaoiPBFYmvrq5iNdXVmjgacUZIVZkUd6Y5o1OtQcSaKdXofUhc7DCUbQjj7Jna788q2mHz0fXu%2FZEiK%2Fa2uUozX8pFTglqtn4mDhLK2tMzH9j0j2FDAU9%2FxVvKTI6F74XpCUSLkNEale9gCUwuIbqNrI9gBor8JuExoJ1synqhfqCb4UBVItdb%2FRj1p0qGnwtKb0wGb%2FoLOjMw08IpC2lWO%2FfYKOBsltsJTzMQfkXJ%2BeILcbFbw4UfQAFLmnI2Z5Z0Vp42%2FxlGqLicqLIlz%2FX3kdeGdpmU%2Fy0WIMKCj7DgxrISSU9r%2BVArD3qiNCipPXorC5t0jJ2CqptVqbme4DXyGHWWglwk0EmSGgr0HTyHfsW2a6S59F1iqH4eRXQBa4xKOj3T8EE2yM0kX2jjFxg9%2Flhd2uYRxW9HKV1XYBwvhaZwqX2COpYVyI143zNt5TOPj4IGEdDMaJ5BTitmlOCZip9ZeOp%2BajK62aWRvRPAncZBtVdME8FLUnqn8qDqUjmt0PxHUYbAogMqSuNQaAEVHNjKlHt0v6g%2F7aNkX6FO8LOkeVSC5lfIhPQ0GfAl2V81sEJGI%2BCi4es5oGFmmsICh0n3dc4FEgmJmkuXtKiyocw%2BYefyAY6pgFwg1DmVubLKrOA6gS6fGB1M02czn0Xsc%2FxWOYixv90%2FaoKcp3FAw35c50v615fqjcs%2F0656Ngvfb8yGh1AKSvdBl286J8qvbNT96iu8YnLE%2FSVqxit5xl2rHEAzsR%2BfiaWVSbsnXyKaATgUDLxquqoqkF6VVZWr2ZAnyPpUNGvMWe5mw2pi7DtCqcnY680D5Flr5kUSaWA88F7DpOCQoY4LOwF0MIZ&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251102T215123Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1800&X-Amz-Credential=ASIA5WIOR33FY6HRU7ZI%2F20251102%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=37a707ce849a0391d3b6f57e45c97ecfbb7388abd9bad49ed053de8ab4f1ac88"
	
	parsedURL, err := url.Parse(testURL)
	if err != nil {
		fmt.Printf("Error parsing URL: %v\n", err)
		return
	}
	
	bucket := extractBucketName(parsedURL)
	fmt.Printf("Host: %s\n", parsedURL.Host)
	fmt.Printf("Extracted bucket: '%s'\n", bucket)
	
	// Test if it matches expected bucket
	expectedBucket := "pennsieve-dev-storage-use1"
	if bucket == expectedBucket {
		fmt.Printf("✅ Bucket extraction SUCCESS: %s\n", bucket)
	} else {
		fmt.Printf("❌ Bucket extraction FAILED: got '%s', expected '%s'\n", bucket, expectedBucket)
	}
}