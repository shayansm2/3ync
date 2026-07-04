# 3ync

**A local-first synchronization tool for S3-compatible object storage.**

3ync synchronizes buckets between S3-compatible object stores while keeping its own synchronization metadata inside the storage itself. It is designed to preserve state across runs, detect conflicts, and make synchronization resilient to interruptions and distributed deployments.

## Features

- Bidirectional synchronization
- Supports S3-compatible object storage (MinIO, AWS S3, etc.)
- Local-first architecture with metadata stored alongside the data
- Conflict detection and safe synchronization
- Incremental sync using persisted metadata
- Designed to recover gracefully from interrupted synchronization


## Getting Started

```bash
git clone https://github.com/shayansm2/3ync.git
cd 3ync

go build .
```

Run:

```bash
export ACCESS_KEY=
export SECRET_KEY=
export BASE_END_POINT=

./3ync <source-bucket> <replica-bucket>
```

## Documentation

The design, architecture, and reasoning behind 3ync are explained in this blog post:

[Building 3ync: A Local-First, S3-Compatible Sync Engine](https://shayansm.substack.com/p/building-3ync-a-local-first-s3-compatible)
