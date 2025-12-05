package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3API defines the S3 operations used by this tool
type S3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	GetObjectRetention(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error)
	PutObjectRetention(ctx context.Context, params *s3.PutObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// ManifestEntry represents a keyspace/table entry in the manifest
type ManifestEntry struct {
	Keyspace     string           `json:"keyspace"`
	ColumnFamily string           `json:"columnfamily"`
	Objects      []ManifestObject `json:"objects"`
}

// ManifestObject represents an object entry in the manifest
type ManifestObject struct {
	Path string `json:"path"`
	MD5  string `json:"MD5"`
	Size int64  `json:"size"`
}

// Manifest represents the parsed manifest - a flat list of all object paths
type Manifest struct {
	Objects []ManifestObject
}

// extractHostnamePath extracts [cluster]/[hostname]/ from a manifest key
func extractHostnamePath(manifestKey string) (string, error) {
	parts := strings.Split(manifestKey, "/")
	if len(parts) < 4 {
		return "", fmt.Errorf("invalid manifest path: %s", manifestKey)
	}
	return parts[0] + "/" + parts[1] + "/", nil
}

// parseManifest parses manifest JSON data
// Medusa manifests are arrays of keyspace entries, each containing objects
func parseManifest(data []byte) (*Manifest, error) {
	var entries []ManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	// Flatten all objects from all keyspace entries
	var allObjects []ManifestObject
	for _, entry := range entries {
		allObjects = append(allObjects, entry.Objects...)
	}

	return &Manifest{Objects: allObjects}, nil
}

// needsRetentionUpdate determines if retention should be updated based on current and required dates
func needsRetentionUpdate(currentRetention *time.Time, requiredUntil time.Time) bool {
	if currentRetention == nil {
		return true
	}
	return currentRetention.Before(requiredUntil)
}

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name")
	cluster := flag.String("cluster", "", "Cluster name")
	retentionDays := flag.Int("retention", 0, "Retention interval in days")
	dryRun := flag.Bool("dry-run", false, "Dry run mode - don't actually update retention")
	flag.Parse()

	if *bucket == "" || *cluster == "" || *retentionDays <= 0 {
		log.Fatal("Usage: go run main.go -bucket <bucket> -cluster <cluster> -retention <days> [-dry-run]")
	}

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	// Find all manifests matching the pattern: [cluster]/[hostname]/[last-backup]/meta/manifest.json
	manifests, err := findManifests(ctx, client, *bucket, *cluster)
	if err != nil {
		log.Fatalf("Failed to find manifests: %v", err)
	}

	log.Printf("Found %d manifests", len(manifests))

	retentionUntil := time.Now().AddDate(0, 0, *retentionDays)

	for _, manifestKey := range manifests {
		log.Printf("Processing manifest: %s", manifestKey)

		manifest, err := downloadManifest(ctx, client, *bucket, manifestKey)
		if err != nil {
			log.Printf("Error downloading manifest %s: %v", manifestKey, err)
			continue
		}

		// Extract hostname path from manifest key: [cluster]/[hostname]/
		// Data files are stored in a shared directory: [cluster]/[hostname]/data/
		hostnamePath, err := extractHostnamePath(manifestKey)
		if err != nil {
			log.Printf("Invalid manifest path: %s", manifestKey)
			continue
		}

		for _, obj := range manifest.Objects {
			// Check if path already includes the hostname prefix (new manifest format)
			// or if it's a relative path that needs the prefix (old format)
			var objectKey string
			if strings.HasPrefix(obj.Path, hostnamePath) {
				objectKey = obj.Path
			} else {
				objectKey = hostnamePath + obj.Path
			}

			needsUpdate, err := checkRetention(ctx, client, *bucket, objectKey, retentionUntil)
			if err != nil {
				log.Printf("Error checking retention for %s: %v", objectKey, err)
				continue
			}

			if needsUpdate {
				if *dryRun {
					log.Printf("[DRY-RUN] Would update retention for: %s", objectKey)
				} else {
					err = updateRetention(ctx, client, *bucket, objectKey, retentionUntil)
					if err != nil {
						log.Printf("Error updating retention for %s: %v", objectKey, err)
					} else {
						log.Printf("Updated retention for: %s (until %s)", objectKey, retentionUntil.Format(time.RFC3339))
					}
				}
			}
		}
	}

	log.Println("Done")
}

// findManifests finds all manifest.json files matching the pattern
func findManifests(ctx context.Context, client S3API, bucket, cluster string) ([]string, error) {
	var manifests []string

	// List all objects under cluster prefix to find hostnames
	prefix := cluster + "/"

	// Track unique hostname/backup combinations
	backupPaths := make(map[string]bool)

	var continuationToken *string
	for {
		resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range resp.Contents {
			key := *obj.Key
			// Look for manifest.json files
			if strings.HasSuffix(key, "/meta/manifest.json") {
				backupPaths[key] = true
			}
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	for path := range backupPaths {
		manifests = append(manifests, path)
	}

	return manifests, nil
}

// downloadManifest downloads and parses a manifest.json file
func downloadManifest(ctx context.Context, client S3API, bucket, key string) (*Manifest, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	return parseManifest(body)
}

// checkRetention checks if an object's retention needs to be updated
func checkRetention(ctx context.Context, client S3API, bucket, key string, requiredUntil time.Time) (bool, error) {
	resp, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// If there's no retention set or object doesn't exist, we need to set it
		if strings.Contains(err.Error(), "NoSuchObjectLockConfiguration") ||
			strings.Contains(err.Error(), "ObjectLockConfigurationNotFoundError") ||
			strings.Contains(err.Error(), "NoSuchKey") {
			return true, nil
		}
		return false, err
	}

	var currentRetention *time.Time
	if resp.Retention != nil && resp.Retention.RetainUntilDate != nil {
		currentRetention = resp.Retention.RetainUntilDate
	}

	return needsRetentionUpdate(currentRetention, requiredUntil), nil
}

// updateRetention sets the retention for an object
func updateRetention(ctx context.Context, client S3API, bucket, key string, retainUntil time.Time) error {
	_, err := client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retainUntil),
		},
	})
	return err
}
