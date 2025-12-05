# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands

```bash
# Build the binary
go build -o medusa-retention-refresher .

# Run directly
go run main.go -bucket <bucket> -cluster <cluster> -retention <days> [-dry-run]

# Manage dependencies
go mod tidy
```

## What This Tool Does

A CLI utility that refreshes S3 Object Lock retention periods for Cassandra backups created by Medusa. It:

1. Scans an S3 bucket for Medusa backup manifests (`[cluster]/[hostname]/[backup_name]/meta/manifest.json`)
2. Parses each manifest to get the list of backup objects
3. Checks each object's current retention period
4. Extends retention to the specified number of days from now if the current retention expires sooner

**Backup structure:**
- Manifests: `[cluster]/[hostname]/[backup_name]/meta/manifest.json`
- Data files: `[cluster]/[hostname]/data/...` (shared across all backups)

Uses S3 Object Lock in GOVERNANCE mode.

## Required CLI Flags

- `-bucket`: S3 bucket name containing backups
- `-cluster`: Cassandra cluster name (used as the S3 prefix)
- `-retention`: Number of days to set retention from today
- `-dry-run`: Optional flag to preview changes without applying them
