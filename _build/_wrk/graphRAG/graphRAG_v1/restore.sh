#!/bin/bash
# =============================================================================
# restore.sh — Restore ArangoDB + Redis from a backup directory
#
# Usage:
#   ./restore.sh                              # restore latest backup
#   ./restore.sh backups/20260403_143000      # restore specific backup
#   ./restore.sh backups/20260403_143000 redis     # redis only
#   ./restore.sh backups/20260403_143000 arangodb  # arangodb only
# =============================================================================

set -euo pipefail

BACKUP_ROOT="$(dirname "$0")/backups"
ARANGO_PASS="${ARANGO_ROOT_PASSWORD:-localdev}"
ARANGO_DB="${ARANGO_DATABASE:-t16o}"

# Determine backup directory
if [ -n "${1:-}" ] && [ -d "$1" ]; then
    BACKUP_DIR="$1"
    COMPONENT="${2:-all}"
elif [ -n "${1:-}" ] && [ ! -d "$1" ]; then
    echo "ERROR: Directory not found: $1"
    echo ""
    echo "Available backups:"
    ls -1d "$BACKUP_ROOT"/20* 2>/dev/null | sort -r || echo "  (none)"
    exit 1
else
    # Find latest backup
    BACKUP_DIR=$(ls -1d "$BACKUP_ROOT"/20* 2>/dev/null | sort -r | head -1)
    COMPONENT="${1:-all}"
    if [ -z "$BACKUP_DIR" ]; then
        echo "ERROR: No backups found in $BACKUP_ROOT"
        exit 1
    fi
fi

echo "[$(date +%H:%M:%S)] Restoring from: $BACKUP_DIR"
echo "[$(date +%H:%M:%S)] Components: $COMPONENT"
echo ""

# ---------------------------------------------------------------------------
# Confirm
# ---------------------------------------------------------------------------
read -p "This will OVERWRITE current data. Continue? (y/N) " CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    echo "Aborted."
    exit 0
fi

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
if [ "$COMPONENT" = "all" ] || [ "$COMPONENT" = "redis" ]; then
    if [ -f "$BACKUP_DIR/redis-dump.rdb" ]; then
        echo "[$(date +%H:%M:%S)] Redis: stopping container..."
        docker stop t16o-redis

        echo "[$(date +%H:%M:%S)] Redis: copying dump.rdb..."
        docker cp "$BACKUP_DIR/redis-dump.rdb" t16o-redis:/data/dump.rdb

        echo "[$(date +%H:%M:%S)] Redis: starting container..."
        docker start t16o-redis

        # Wait for healthy
        for i in $(seq 1 30); do
            sleep 1
            STATUS=$(docker inspect --format='{{.State.Health.Status}}' t16o-redis 2>/dev/null || echo "unknown")
            if [ "$STATUS" = "healthy" ]; then
                break
            fi
        done
        echo "[$(date +%H:%M:%S)] Redis: restored and healthy"
    else
        echo "[$(date +%H:%M:%S)] Redis: no dump.rdb found in backup, skipping"
    fi
fi

# ---------------------------------------------------------------------------
# ArangoDB
# ---------------------------------------------------------------------------
if [ "$COMPONENT" = "all" ] || [ "$COMPONENT" = "arangodb" ]; then
    if [ -d "$BACKUP_DIR/arangodb" ]; then
        echo "[$(date +%H:%M:%S)] ArangoDB: copying dump to container..."
        docker exec t16o-arangodb rm -rf /tmp/arango-restore
        docker cp "$BACKUP_DIR/arangodb" t16o-arangodb:/tmp/arango-restore

        echo "[$(date +%H:%M:%S)] ArangoDB: restoring database '$ARANGO_DB'..."
        docker exec t16o-arangodb arangorestore \
            --server.database "$ARANGO_DB" \
            --server.password "$ARANGO_PASS" \
            --input-directory /tmp/arango-restore \
            --create-database true \
            --overwrite true \
            --threads 2 \
            2>&1 | tail -5

        docker exec t16o-arangodb rm -rf /tmp/arango-restore
        echo "[$(date +%H:%M:%S)] ArangoDB: restored"
    else
        echo "[$(date +%H:%M:%S)] ArangoDB: no dump found in backup, skipping"
    fi
fi

echo ""
echo "=== Restore Complete ==="
