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

// Manifest represents the structure of manifest.json
type Manifest struct {
	Objects []ManifestObject `json:"objects"`
}

// ManifestObject represents an object entry in the manifest
type ManifestObject struct {
	Path string `json:"path"`
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

		// Extract base path from manifest key (remove /meta/manifest.json)
		basePath := strings.TrimSuffix(manifestKey, "/meta/manifest.json")

		for _, obj := range manifest.Objects {
			objectKey := basePath + "/" + obj.Path

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
func findManifests(ctx context.Context, client *s3.Client, bucket, cluster string) ([]string, error) {
	var manifests []string

	// List all objects under cluster prefix to find hostnames
	prefix := cluster + "/"

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	// Track unique hostname/backup combinations
	backupPaths := make(map[string]bool)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			// Look for manifest.json files
			if strings.HasSuffix(key, "/meta/manifest.json") {
				backupPaths[key] = true
			}
		}
	}

	for path := range backupPaths {
		manifests = append(manifests, path)
	}

	return manifests, nil
}

// downloadManifest downloads and parses a manifest.json file
func downloadManifest(ctx context.Context, client *s3.Client, bucket, key string) (*Manifest, error) {
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

	var manifest Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	return &manifest, nil
}

// checkRetention checks if an object's retention needs to be updated
func checkRetention(ctx context.Context, client *s3.Client, bucket, key string, requiredUntil time.Time) (bool, error) {
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

	if resp.Retention == nil || resp.Retention.RetainUntilDate == nil {
		return true, nil
	}

	// Check if current retention is less than required
	return resp.Retention.RetainUntilDate.Before(requiredUntil), nil
}

// updateRetention sets the retention for an object
func updateRetention(ctx context.Context, client *s3.Client, bucket, key string, retainUntil time.Time) error {
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
