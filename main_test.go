package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MockS3Client implements S3API for testing
type MockS3Client struct {
	ListObjectsV2Func     func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObjectFunc         func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	GetObjectRetentionFunc func(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error)
	PutObjectRetentionFunc func(ctx context.Context, params *s3.PutObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error)
}

func (m *MockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.ListObjectsV2Func != nil {
		return m.ListObjectsV2Func(ctx, params, optFns...)
	}
	return nil, errors.New("ListObjectsV2 not implemented")
}

func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.GetObjectFunc != nil {
		return m.GetObjectFunc(ctx, params, optFns...)
	}
	return nil, errors.New("GetObject not implemented")
}

func (m *MockS3Client) GetObjectRetention(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error) {
	if m.GetObjectRetentionFunc != nil {
		return m.GetObjectRetentionFunc(ctx, params, optFns...)
	}
	return nil, errors.New("GetObjectRetention not implemented")
}

func (m *MockS3Client) PutObjectRetention(ctx context.Context, params *s3.PutObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error) {
	if m.PutObjectRetentionFunc != nil {
		return m.PutObjectRetentionFunc(ctx, params, optFns...)
	}
	return nil, errors.New("PutObjectRetention not implemented")
}

// Unit Tests for extractHostnamePath

func TestExtractHostnamePath(t *testing.T) {
	tests := []struct {
		name        string
		manifestKey string
		want        string
		wantErr     bool
	}{
		{
			name:        "valid manifest path",
			manifestKey: "links/links-us-default-sts-8/medusa-backup-schedule-1764858600/meta/manifest.json",
			want:        "links/links-us-default-sts-8/",
			wantErr:     false,
		},
		{
			name:        "another valid path",
			manifestKey: "cluster1/host1/backup-001/meta/manifest.json",
			want:        "cluster1/host1/",
			wantErr:     false,
		},
		{
			name:        "path with many segments",
			manifestKey: "a/b/c/d/e/meta/manifest.json",
			want:        "a/b/",
			wantErr:     false,
		},
		{
			name:        "too few segments",
			manifestKey: "a/b/c",
			wantErr:     true,
		},
		{
			name:        "empty path",
			manifestKey: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractHostnamePath(tt.manifestKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractHostnamePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("extractHostnamePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Unit Tests for parseManifest

func TestParseManifest(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    *Manifest
		wantErr bool
	}{
		{
			name: "valid manifest with objects",
			data: []byte(`{"objects":[{"path":"data/keyspace/table/file1.db"},{"path":"data/keyspace/table/file2.db"}]}`),
			want: &Manifest{
				Objects: []ManifestObject{
					{Path: "data/keyspace/table/file1.db"},
					{Path: "data/keyspace/table/file2.db"},
				},
			},
			wantErr: false,
		},
		{
			name: "empty objects array",
			data: []byte(`{"objects":[]}`),
			want: &Manifest{
				Objects: []ManifestObject{},
			},
			wantErr: false,
		},
		{
			name:    "empty JSON",
			data:    []byte(`{}`),
			want:    &Manifest{},
			wantErr: false,
		},
		{
			name:    "invalid JSON",
			data:    []byte(`{invalid}`),
			wantErr: true,
		},
		{
			name:    "empty input",
			data:    []byte(``),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseManifest(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseManifest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if len(got.Objects) != len(tt.want.Objects) {
				t.Errorf("parseManifest() got %d objects, want %d", len(got.Objects), len(tt.want.Objects))
				return
			}
			for i, obj := range got.Objects {
				if obj.Path != tt.want.Objects[i].Path {
					t.Errorf("parseManifest() object[%d].Path = %v, want %v", i, obj.Path, tt.want.Objects[i].Path)
				}
			}
		})
	}
}

// Unit Tests for needsRetentionUpdate

func TestNeedsRetentionUpdate(t *testing.T) {
	now := time.Now()
	past := now.Add(-24 * time.Hour)
	future := now.Add(24 * time.Hour)
	farFuture := now.Add(48 * time.Hour)

	tests := []struct {
		name             string
		currentRetention *time.Time
		requiredUntil    time.Time
		want             bool
	}{
		{
			name:             "nil retention needs update",
			currentRetention: nil,
			requiredUntil:    future,
			want:             true,
		},
		{
			name:             "past retention needs update",
			currentRetention: &past,
			requiredUntil:    future,
			want:             true,
		},
		{
			name:             "current retention before required needs update",
			currentRetention: &future,
			requiredUntil:    farFuture,
			want:             true,
		},
		{
			name:             "current retention after required no update needed",
			currentRetention: &farFuture,
			requiredUntil:    future,
			want:             false,
		},
		{
			name:             "current retention equal to required no update needed",
			currentRetention: &future,
			requiredUntil:    future,
			want:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := needsRetentionUpdate(tt.currentRetention, tt.requiredUntil)
			if got != tt.want {
				t.Errorf("needsRetentionUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Integration Tests with Mock S3 Client

func TestFindManifests(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		bucket    string
		cluster   string
		setupMock func() *MockS3Client
		want      []string
		wantErr   bool
	}{
		{
			name:    "finds manifests successfully",
			bucket:  "test-bucket",
			cluster: "links",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
						return &s3.ListObjectsV2Output{
							Contents: []types.Object{
								{Key: aws.String("links/host1/backup1/meta/manifest.json")},
								{Key: aws.String("links/host1/backup2/meta/manifest.json")},
								{Key: aws.String("links/host1/data/file.db")},
								{Key: aws.String("links/host2/backup1/meta/manifest.json")},
							},
							IsTruncated: aws.Bool(false),
						}, nil
					},
				}
			},
			want:    []string{"links/host1/backup1/meta/manifest.json", "links/host1/backup2/meta/manifest.json", "links/host2/backup1/meta/manifest.json"},
			wantErr: false,
		},
		{
			name:    "handles pagination",
			bucket:  "test-bucket",
			cluster: "cluster1",
			setupMock: func() *MockS3Client {
				callCount := 0
				return &MockS3Client{
					ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
						callCount++
						if callCount == 1 {
							return &s3.ListObjectsV2Output{
								Contents: []types.Object{
									{Key: aws.String("cluster1/host1/backup1/meta/manifest.json")},
								},
								IsTruncated:           aws.Bool(true),
								NextContinuationToken: aws.String("token123"),
							}, nil
						}
						return &s3.ListObjectsV2Output{
							Contents: []types.Object{
								{Key: aws.String("cluster1/host2/backup1/meta/manifest.json")},
							},
							IsTruncated: aws.Bool(false),
						}, nil
					},
				}
			},
			want:    []string{"cluster1/host1/backup1/meta/manifest.json", "cluster1/host2/backup1/meta/manifest.json"},
			wantErr: false,
		},
		{
			name:    "no manifests found",
			bucket:  "test-bucket",
			cluster: "empty",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
						return &s3.ListObjectsV2Output{
							Contents:    []types.Object{},
							IsTruncated: aws.Bool(false),
						}, nil
					},
				}
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:    "S3 error",
			bucket:  "test-bucket",
			cluster: "error",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
						return nil, errors.New("access denied")
					},
				}
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := tt.setupMock()
			got, err := findManifests(ctx, mock, tt.bucket, tt.cluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("findManifests() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			// Convert to map for comparison since order is not guaranteed
			gotMap := make(map[string]bool)
			for _, m := range got {
				gotMap[m] = true
			}
			wantMap := make(map[string]bool)
			for _, m := range tt.want {
				wantMap[m] = true
			}
			if len(gotMap) != len(wantMap) {
				t.Errorf("findManifests() got %d manifests, want %d", len(got), len(tt.want))
				return
			}
			for k := range wantMap {
				if !gotMap[k] {
					t.Errorf("findManifests() missing manifest %s", k)
				}
			}
		})
	}
}

func TestDownloadManifest(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		bucket    string
		key       string
		setupMock func() *MockS3Client
		want      *Manifest
		wantErr   bool
	}{
		{
			name:   "downloads and parses manifest successfully",
			bucket: "test-bucket",
			key:    "cluster/host/backup/meta/manifest.json",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						body := `{"objects":[{"path":"data/ks/table/file.db"}]}`
						return &s3.GetObjectOutput{
							Body: io.NopCloser(strings.NewReader(body)),
						}, nil
					},
				}
			},
			want: &Manifest{
				Objects: []ManifestObject{
					{Path: "data/ks/table/file.db"},
				},
			},
			wantErr: false,
		},
		{
			name:   "S3 GetObject error",
			bucket: "test-bucket",
			key:    "missing/manifest.json",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						return nil, errors.New("NoSuchKey")
					},
				}
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "invalid JSON in manifest",
			bucket: "test-bucket",
			key:    "cluster/host/backup/meta/manifest.json",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						return &s3.GetObjectOutput{
							Body: io.NopCloser(strings.NewReader(`{invalid json}`)),
						}, nil
					},
				}
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := tt.setupMock()
			got, err := downloadManifest(ctx, mock, tt.bucket, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("downloadManifest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if len(got.Objects) != len(tt.want.Objects) {
				t.Errorf("downloadManifest() got %d objects, want %d", len(got.Objects), len(tt.want.Objects))
			}
		})
	}
}

func TestCheckRetention(t *testing.T) {
	ctx := context.Background()
	requiredUntil := time.Now().Add(30 * 24 * time.Hour)
	pastRetention := time.Now().Add(-1 * 24 * time.Hour)
	futureRetention := time.Now().Add(60 * 24 * time.Hour)

	tests := []struct {
		name      string
		bucket    string
		key       string
		setupMock func() *MockS3Client
		want      bool
		wantErr   bool
	}{
		{
			name:   "retention needs update - expires before required",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectRetentionFunc: func(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error) {
						return &s3.GetObjectRetentionOutput{
							Retention: &types.ObjectLockRetention{
								Mode:            types.ObjectLockRetentionModeGovernance,
								RetainUntilDate: aws.Time(pastRetention),
							},
						}, nil
					},
				}
			},
			want:    true,
			wantErr: false,
		},
		{
			name:   "no update needed - retention expires after required",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectRetentionFunc: func(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error) {
						return &s3.GetObjectRetentionOutput{
							Retention: &types.ObjectLockRetention{
								Mode:            types.ObjectLockRetentionModeGovernance,
								RetainUntilDate: aws.Time(futureRetention),
							},
						}, nil
					},
				}
			},
			want:    false,
			wantErr: false,
		},
		{
			name:   "no retention configured - needs update",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectRetentionFunc: func(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error) {
						return nil, errors.New("NoSuchObjectLockConfiguration")
					},
				}
			},
			want:    true,
			wantErr: false,
		},
		{
			name:   "object not found - needs update",
			bucket: "test-bucket",
			key:    "cluster/host/data/missing.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectRetentionFunc: func(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error) {
						return nil, errors.New("NoSuchKey")
					},
				}
			},
			want:    true,
			wantErr: false,
		},
		{
			name:   "access denied error",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectRetentionFunc: func(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error) {
						return nil, errors.New("AccessDenied")
					},
				}
			},
			want:    false,
			wantErr: true,
		},
		{
			name:   "nil retention in response - needs update",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					GetObjectRetentionFunc: func(ctx context.Context, params *s3.GetObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error) {
						return &s3.GetObjectRetentionOutput{
							Retention: nil,
						}, nil
					},
				}
			},
			want:    true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := tt.setupMock()
			got, err := checkRetention(ctx, mock, tt.bucket, tt.key, requiredUntil)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkRetention() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("checkRetention() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUpdateRetention(t *testing.T) {
	ctx := context.Background()
	retainUntil := time.Now().Add(30 * 24 * time.Hour)

	tests := []struct {
		name      string
		bucket    string
		key       string
		setupMock func() *MockS3Client
		wantErr   bool
	}{
		{
			name:   "updates retention successfully",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					PutObjectRetentionFunc: func(ctx context.Context, params *s3.PutObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error) {
						// Verify the parameters
						if *params.Bucket != "test-bucket" {
							return nil, errors.New("wrong bucket")
						}
						if *params.Key != "cluster/host/data/file.db" {
							return nil, errors.New("wrong key")
						}
						if params.Retention.Mode != types.ObjectLockRetentionModeGovernance {
							return nil, errors.New("wrong mode")
						}
						return &s3.PutObjectRetentionOutput{}, nil
					},
				}
			},
			wantErr: false,
		},
		{
			name:   "access denied error",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					PutObjectRetentionFunc: func(ctx context.Context, params *s3.PutObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error) {
						return nil, errors.New("AccessDenied")
					},
				}
			},
			wantErr: true,
		},
		{
			name:   "object lock not enabled",
			bucket: "test-bucket",
			key:    "cluster/host/data/file.db",
			setupMock: func() *MockS3Client {
				return &MockS3Client{
					PutObjectRetentionFunc: func(ctx context.Context, params *s3.PutObjectRetentionInput, optFns ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error) {
						return nil, errors.New("InvalidRequest: Bucket is missing Object Lock Configuration")
					},
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := tt.setupMock()
			err := updateRetention(ctx, mock, tt.bucket, tt.key, retainUntil)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateRetention() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test object path construction
func TestObjectPathConstruction(t *testing.T) {
	tests := []struct {
		name         string
		manifestKey  string
		objectPath   string
		expectedPath string
	}{
		{
			name:         "standard medusa backup path",
			manifestKey:  "links/links-us-default-sts-8/medusa-backup-schedule-1764858600/meta/manifest.json",
			objectPath:   "data/keyspace/table/mc-1-big-Data.db",
			expectedPath: "links/links-us-default-sts-8/data/keyspace/table/mc-1-big-Data.db",
		},
		{
			name:         "different cluster and host",
			manifestKey:  "production/node-1/backup-2024/meta/manifest.json",
			objectPath:   "data/system/local/mc-1-big-Data.db",
			expectedPath: "production/node-1/data/system/local/mc-1-big-Data.db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostnamePath, err := extractHostnamePath(tt.manifestKey)
			if err != nil {
				t.Fatalf("extractHostnamePath() error = %v", err)
			}
			objectKey := hostnamePath + tt.objectPath
			if objectKey != tt.expectedPath {
				t.Errorf("object path = %v, want %v", objectKey, tt.expectedPath)
			}
		})
	}
}
