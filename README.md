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

### From Source

```bash
go build -o medusa-retention-refresher .
```

### Docker

Pre-built images are available from GitHub Container Registry:

```bash
docker pull ghcr.io/short-io/medusa-retention-refresher:latest
```

Run with Docker:
```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  ghcr.io/short-io/medusa-retention-refresher:latest \
  -bucket my-backups -cluster prod-cassandra -min-retention 7 -max-retention 30
```

### Kubernetes

Deploy as a CronJob for scheduled retention updates:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: medusa-retention-refresher
spec:
  schedule: "0 2 * * *"  # Run daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: medusa-retention-refresher
          containers:
          - name: refresher
            image: ghcr.io/short-io/medusa-retention-refresher:latest
            args:
            - -bucket=my-backups
            - -cluster=prod-cassandra
            - -min-retention=7
            - -max-retention=30
            env:
            - name: AWS_REGION
              value: us-east-1
          restartPolicy: OnFailure
```

For AWS authentication in Kubernetes, use one of:
- **IAM Roles for Service Accounts (IRSA)** on EKS - recommended
- **Pod Identity** on EKS
- **Secret with AWS credentials** - create a Secret and mount as environment variables

Example with IRSA (annotate the ServiceAccount):
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: medusa-retention-refresher
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/medusa-retention-refresher
```

## Usage

```bash
./medusa-retention-refresher -bucket <bucket> -cluster <cluster> -min-retention <days> -max-retention <days> [-dry-run]
```

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `-bucket` | Yes | S3 bucket name containing the backups |
| `-cluster` | Yes | Cassandra cluster name (S3 prefix) |
| `-min-retention` | Yes | Minimum retention threshold in days - objects with retention expiring before this many days from now will be updated |
| `-max-retention` | Yes | Target retention in days - new retention period applied when updating objects |
| `-dry-run` | No | Preview changes without applying them |

### Examples

Preview changes: extend retention to 30 days for objects expiring within 7 days:
```bash
./medusa-retention-refresher -bucket my-backups -cluster prod-cassandra -min-retention 7 -max-retention 30 -dry-run
```

Apply 90-day retention to objects expiring within 14 days:
```bash
./medusa-retention-refresher -bucket my-backups -cluster prod-cassandra -min-retention 14 -max-retention 90
```

## Expected S3 Structure

The tool expects Medusa's default backup structure:
```
<cluster>/
  <hostname>/
    <backup-name>/
      meta/
        manifest.json    # Backup manifest listing all objects
    data/
      ...                # Data files (shared across all backups for this host)
```

Note: Data files are stored at the hostname level and shared across backups, while each backup has its own manifest that references the relevant data files.

## IAM Permissions

Required S3 permissions:
- `s3:ListBucket`
- `s3:GetObject`
- `s3:GetObjectRetention`
- `s3:PutObjectRetention`
