#!/bin/bash
# =============================================================================
# backup.sh — Backup ArangoDB + Redis from Docker volumes
#
# Usage:
#   ./backup.sh                  # backup to ~/graphRAG/backups/<timestamp>/
#   ./backup.sh /mnt/backups     # backup to custom directory
# =============================================================================

set -euo pipefail

BACKUP_ROOT="${1:-$(dirname "$0")/backups}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$BACKUP_ROOT/$TIMESTAMP"
ARANGO_PASS="${ARANGO_ROOT_PASSWORD:-localdev}"
ARANGO_DB="${ARANGO_DATABASE:-t16o}"

mkdir -p "$BACKUP_DIR"
echo "[$(date +%H:%M:%S)] Backup starting → $BACKUP_DIR"

# ---------------------------------------------------------------------------
# Redis — trigger BGSAVE, wait, copy dump.rdb
# ---------------------------------------------------------------------------
echo "[$(date +%H:%M:%S)] Redis: triggering BGSAVE..."
docker exec t16o-redis redis-cli BGSAVE > /dev/null

# Wait for BGSAVE to complete
for i in $(seq 1 30); do
    sleep 1
    SAVING=$(docker exec t16o-redis redis-cli LASTSAVE 2>/dev/null)
    BG=$(docker exec t16o-redis redis-cli INFO persistence 2>/dev/null | grep rdb_bgsave_in_progress | tr -d '[:space:]')
    if [ "$BG" = "rdb_bgsave_in_progress:0" ]; then
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "[$(date +%H:%M:%S)] WARNING: BGSAVE may not have completed"
    fi
done

docker cp t16o-redis:/data/dump.rdb "$BACKUP_DIR/redis-dump.rdb"
REDIS_SIZE=$(du -sh "$BACKUP_DIR/redis-dump.rdb" | cut -f1)
echo "[$(date +%H:%M:%S)] Redis: done ($REDIS_SIZE)"

# ---------------------------------------------------------------------------
# ArangoDB — arangodump to temp dir, copy out
# ---------------------------------------------------------------------------
# Check if database exists before dumping
DB_EXISTS=$(docker exec t16o-arangodb arangosh \
    --server.password "$ARANGO_PASS" \
    --javascript.execute-string "print(db._databases().indexOf('$ARANGO_DB') >= 0)" 2>/dev/null | tail -1)

if [ "$DB_EXISTS" = "true" ]; then
    echo "[$(date +%H:%M:%S)] ArangoDB: dumping database '$ARANGO_DB'..."
    docker exec t16o-arangodb rm -rf /tmp/arango-backup
    docker exec t16o-arangodb arangodump \
        --server.database "$ARANGO_DB" \
        --server.password "$ARANGO_PASS" \
        --output-directory /tmp/arango-backup \
        --overwrite true \
        --compress-output true \
        --threads 2 \
        2>&1 | tail -3

    docker cp t16o-arangodb:/tmp/arango-backup "$BACKUP_DIR/arangodb/"
    docker exec t16o-arangodb rm -rf /tmp/arango-backup
    ARANGO_SIZE=$(du -sh "$BACKUP_DIR/arangodb" | cut -f1)
    echo "[$(date +%H:%M:%S)] ArangoDB: done ($ARANGO_SIZE)"
else
    echo "[$(date +%H:%M:%S)] ArangoDB: database '$ARANGO_DB' does not exist yet, skipping"
    ARANGO_SIZE="0"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
TOTAL_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
echo ""
echo "=== Backup Complete ==="
echo "  Location: $BACKUP_DIR"
echo "  Redis:    $REDIS_SIZE"
echo "  ArangoDB: $ARANGO_SIZE"
echo "  Total:    $TOTAL_SIZE"
echo ""

# Prune old backups — keep last 5
KEEP=5
BACKUPS=$(ls -1d "$BACKUP_ROOT"/20* 2>/dev/null | sort -r)
COUNT=$(echo "$BACKUPS" | wc -l)
if [ "$COUNT" -gt "$KEEP" ]; then
    echo "$BACKUPS" | tail -n +$((KEEP + 1)) | while read OLD; do
        echo "  Pruning old backup: $OLD"
        rm -rf "$OLD"
    done
fi
