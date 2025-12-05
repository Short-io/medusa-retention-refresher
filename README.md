# Medusa Retention Refresher

A CLI tool to extend S3 Object Lock retention periods for Cassandra backups created by [Medusa](https://github.com/thelastpickle/cassandra-medusa).

## Overview

When using S3 Object Lock to protect Medusa backups, retention periods are typically set at backup creation time. This tool allows you to extend retention periods for existing backups without re-uploading the data.

The tool:
1. Discovers all backup manifests for a given cluster
2. Parses each manifest to identify all backup objects
3. Checks current retention settings
4. Extends retention where needed (uses GOVERNANCE mode)

## Prerequisites

- Go 1.21+
- AWS credentials configured (via environment variables, ~/.aws/credentials, or IAM role)
- S3 bucket with Object Lock enabled

## Installation

```bash
go build -o medusa-retention-refresher .
```

## Usage

```bash
./medusa-retention-refresher -bucket <bucket> -cluster <cluster> -retention <days> [-dry-run]
```

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `-bucket` | Yes | S3 bucket name containing the backups |
| `-cluster` | Yes | Cassandra cluster name (S3 prefix) |
| `-retention` | Yes | Retention period in days from today |
| `-dry-run` | No | Preview changes without applying them |

### Examples

Preview retention updates for a 30-day retention policy:
```bash
./medusa-retention-refresher -bucket my-backups -cluster prod-cassandra -retention 30 -dry-run
```

Apply 90-day retention to all backups:
```bash
./medusa-retention-refresher -bucket my-backups -cluster prod-cassandra -retention 90
```

## Expected S3 Structure

The tool expects Medusa's default backup structure:
```
<cluster>/<hostname>/<backup-name>/meta/manifest.json
<cluster>/<hostname>/<backup-name>/data/...
```

## IAM Permissions

Required S3 permissions:
- `s3:ListBucket`
- `s3:GetObject`
- `s3:GetObjectRetention`
- `s3:PutObjectRetention`
