# 3ync

**A local-first replication and synchronization tool for S3-compatible object storage.**

3ync keeps buckets in sync across S3-compatible stores (MinIO, AWS S3, etc.) while storing its own metadata inside the buckets themselves. It uses persisted sync timestamps to handle incremental updates, detect deletions, resolve conflicts with a last-write-wins policy, and recover gracefully from interrupted runs.

## Replication vs Synchronization

**Replication** (`Replicate`) is one-way: only the source bucket changes; the replica is read-only. **Synchronization** (`Synchronize`) is bidirectional: both buckets are updated to converge on the same state.

### Replication

```
FUNCTION REPLICATE(source, replica)
    CASE

        Source missing:
            Replica older than bucket_metadata.lastUpdate?
                YES → Do nothing (File was previously deleted in source)
                NO  → Copy object from replica to source (New file in replica)

        Replica missing:
            Source newer than bucket_metadata.lastUpdate?
                YES → Do nothing (New file in source)
                NO  → Backup source object
                      Delete source object

        Both exist:
            Objects identical (ETags match)?
                YES → Do nothing

            Source newer?
                YES → Do nothing

            Replica newer:
                YES → Backup source object
                      Replace source object with replica object

END FUNCTION
```

### Synchronization

```
FUNCTION SYNCHRONIZE(first, second)
    CASE

        One side missing:
            Object older than missing side's lastUpdate with peer?
                YES → Backup and delete from existing side
                NO  → Copy object to missing side

        Both exist:
            Objects identical (ETags match)?
                YES → Do nothing

            First newer?
                YES → Backup second object
                      Replace second object with first object

            Second newer:
                YES → Backup first object
                      Replace first object with second object

END FUNCTION
```

## Internal metadata

Each bucket stores sync state under `.3ync/metadata.json` (last update per peer) and backups under `.3ync/backups/`.

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

./3ync <bucket-a> <bucket-b>
```

For development, you can use a `.env` file instead (see `.env.example`).

## Testing

Requires Docker.

```bash
go test . -v
```

## Documentation

[Building 3ync: A Local-First, S3-Compatible Sync Engine](https://shayansm.substack.com/p/building-3ync-a-local-first-s3-compatible)
