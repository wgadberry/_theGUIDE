# T16O Admin Guide

Operational runbook for managing the T16O services infrastructure.

---

## 1. Connecting to EC2

### Services EC2 (main — all workers, MySQL, RabbitMQ)
```powershell
ssh -i C:\Users\wgadb\source\repos\T16O\docker\t160_v1_services.pem ec2-user@34.196.15.116
```

### ChromaDB EC2 (search infrastructure)
```powershell
ssh -i C:\Users\wgadb\source\repos\T16O\docker\kp-velintra.pem ubuntu@54.83.147.188
```

---

## 2. Docker Container Management

### View running containers
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

### View resource usage
```bash
docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}'
```

### View logs
```bash
# Last 50 lines
docker logs t16o_v1_services_producer --tail 50

# Follow live
docker logs t16o_v1_services_producer -f

# With timestamps
docker logs t16o_v1_services_producer --tail 50 -t
```

### Restart a single worker
```bash
cd /home/ec2-user/t16o/docker
docker compose -f docker-compose.services.yml restart producer
```

### Rebuild and restart (after code changes)
```bash
cd /home/ec2-user/t16o/docker

# Single service
docker compose -f docker-compose.services.yml build producer && \
docker compose -f docker-compose.services.yml up -d producer

# Force no-cache rebuild
docker compose -f docker-compose.services.yml build --no-cache producer && \
docker compose -f docker-compose.services.yml up -d producer

# All services
docker compose -f docker-compose.services.yml up -d --build
```

### Stop / start individual containers
```bash
docker compose -f docker-compose.services.yml stop detailer
docker compose -f docker-compose.services.yml start detailer
```

### Restart everything
```bash
cd /home/ec2-user/t16o/docker
docker compose -f docker-compose.services.yml down
docker compose -f docker-compose.services.yml up -d
```

### Clean up unused images/volumes (reclaim disk)
```bash
docker system prune -f          # removes stopped containers, dangling images
docker image prune -a -f        # removes all unused images (more aggressive)
```

---

## 3. Running Scripts Inside Containers

**IMPORTANT:** Do NOT use `docker compose run` for utility scripts — it creates temporary containers that can leave zombie shims and crash Docker. Use `docker exec` instead.

### Copy a script into a running container, then execute
```bash
# Copy
docker cp /home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_wrk/backfill-gaps.py \
  t16o_v1_services_producer:/app/_wrk/

# Execute
docker exec t16o_v1_services_producer python3 /app/_wrk/backfill-gaps.py --dry-run
```

### Interactive shell inside a container
```bash
docker exec -it t16o_v1_services_producer bash
cd /app/_wrk
python3 backfill-gaps.py --dry-run
```

### Run a MySQL query from the host
```bash
docker exec t16o_v1_services_mysql mysql -u root -prootpassword t16o_db \
  -e "SELECT COUNT(*) FROM tx"
```

### Run a SQL file
```bash
docker cp /tmp/my-script.sql t16o_v1_services_mysql:/tmp/
docker exec t16o_v1_services_mysql mysql -u root -prootpassword t16o_db \
  -e "source /tmp/my-script.sql"
```

---

## 4. Deploying Code Changes

### From your Windows machine to EC2
```powershell
# Single file
scp -i C:\Users\wgadb\source\repos\T16O\docker\t160_v1_services.pem `
  C:\Users\wgadb\source\repos\T16O\sql\shred\_theGuide\_build_all\_wrk\guide-producer.py `
  ec2-user@34.196.15.116:/home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_wrk/guide-producer.py

# Then rebuild the container on EC2
ssh -i C:\Users\wgadb\source\repos\T16O\docker\t160_v1_services.pem ec2-user@34.196.15.116
cd /home/ec2-user/t16o/docker
docker compose -f docker-compose.services.yml build producer && \
docker compose -f docker-compose.services.yml up -d producer
```

### Deploy a stored procedure
```powershell
# Copy to EC2
scp -i C:\Users\wgadb\source\repos\T16O\docker\t160_v1_services.pem `
  C:\Users\wgadb\source\repos\T16O\sql\shred\_theGuide\_build_all\_db\procedures\sp_tx_get.sql `
  ec2-user@34.196.15.116:/tmp/sp_tx_get.sql

# Execute on EC2
ssh -i C:\Users\wgadb\source\repos\T16O\docker\t160_v1_services.pem ec2-user@34.196.15.116
docker cp /tmp/sp_tx_get.sql t16o_v1_services_mysql:/tmp/
docker exec t16o_v1_services_mysql mysql -u root -prootpassword t16o_db \
  -e "source /tmp/sp_tx_get.sql"
```

### Deploy the bmap viewer
```powershell
scp -i C:\Users\wgadb\source\repos\T16O\docker\t160_v1_services.pem `
  C:\Users\wgadb\source\repos\T16O\sql\shred\ClusterMaps\bmap-viewer\bmap-viewer-v3.html `
  ec2-user@34.196.15.116:/home/ec2-user/t16o/sql/shred/ClusterMaps/bmap-viewer/bmap-viewer-v3.html
```
No container rebuild needed — the viewer is a static HTML file served directly.

---

## 5. Workers & Pipeline

### Pipeline Flow
```
User Request → Gateway (REST API, port 5100)
                  ↓
              Producer → fetches signatures from Solana RPC
                  ↓
         ┌──────────────────┐
         ↓                  ↓
      Decoder            Detailer
    (Solscan actions)  (Solscan detail)
         ↓                  ↓
         └──────────────────┘
                  ↓
             Aggregator → sp_tx_guide_loader (edges)
                  ↓
             Enricher → token/pool metadata (Solscan API)
                  ↓
              Funder → address classification (Solscan API)
```

### Check which workers are busy
```bash
# Quick: check CPU usage
docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' | sort -k2 -rn

# Detailed: check each worker's last log line
for c in producer decoder detailer enricher aggregator funder synchronizer; do
  echo "=== $c ==="
  docker logs t16o_v1_services_$c --tail 3 2>&1
done
```

### Trigger a producer fetch via API
```bash
curl -s -X POST https://svcs.the16oracles.io/api/trigger/producer \
  -H "X-API-Key: admin_master_key" \
  -H "Content-Type: application/json" \
  -d '{
    "action": "process",
    "batch": {
      "filters": {
        "addresses": ["<MINT_ADDRESS>"]
      },
      "address_sig_cnt": 100
    }
  }'
```

### Trigger with time boundaries
```bash
curl -s -X POST https://svcs.the16oracles.io/api/trigger/producer \
  -H "X-API-Key: admin_master_key" \
  -H "Content-Type: application/json" \
  -d '{
    "action": "process",
    "batch": {
      "filters": {
        "addresses": ["<MINT_ADDRESS>"],
        "before": {"block_time": 1774230993}
      },
      "address_sig_cnt": 1000
    }
  }'
```

### Safe thread counts for bulk processing
| Worker | Threads | Notes |
|--------|---------|-------|
| decoder | 2-4 | Solscan API, moderate speed |
| detailer | 6-8 | Solscan API, slower (2-4x), needs more threads |
| enricher | 1-3 | Solscan API, 3-phase |
| aggregator | 1 | MySQL-heavy, deadlock-prone at higher counts |
| funder | 1-3 | Solscan API, address classification |

Thread counts are configurable at runtime via the `config` table:
```sql
UPDATE config SET config_value = '4'
WHERE config_type = 'batch' AND config_key = 'decoder_wrk_cnt_threads';
```
Workers re-read config each iteration.

---

## 6. RabbitMQ

### Management UI
- **URL:** http://34.196.15.116:15692
- **Credentials:** admin / admin123
- **Virtual Host:** `t16o_mq`

### Check queue depths from CLI
```bash
docker exec t16o_v1_services_rabbitmq rabbitmqctl -p t16o_mq list_queues name messages consumers
```

### Purge a stuck queue
```bash
docker exec t16o_v1_services_rabbitmq rabbitmqctl -p t16o_mq purge_queue mq.guide.decoder.request
```

### Check DLQ (dead letter queues)
```bash
docker exec t16o_v1_services_rabbitmq rabbitmqctl -p t16o_mq list_queues name messages \
  | grep dlq
```

### Priority levels
| Priority | Source |
|----------|--------|
| 10 | Realtime |
| 8 | REST API requests |
| 5 | Normal (CLI, cascades) |
| 3 | Backfill |
| 1 | Background prime |

---

## 7. MySQL

### Connect from host
```bash
docker exec -it t16o_v1_services_mysql mysql -u root -prootpassword t16o_db
```

### Connect remotely (from your Windows machine)
```powershell
mysql -h 34.196.15.116 -P 3397 -u root -prootpassword t16o_db
```

### Useful queries

**Pipeline status — what tx_state are transactions in?**
```sql
SELECT
  tx_state,
  COUNT(*) AS cnt,
  MIN(block_time) AS oldest_bt,
  MAX(block_time) AS newest_bt
FROM tx
GROUP BY tx_state
ORDER BY cnt DESC;
```

**Worker activity — recent request log**
```sql
SELECT target_worker, status, COUNT(*) AS cnt, MAX(created_utc) AS last_at
FROM tx_request_log
WHERE created_utc > NOW() - INTERVAL 1 HOUR
GROUP BY target_worker, status;
```

**Table sizes**
```sql
SELECT table_name, ROUND(data_length/1024/1024) AS data_mb,
       ROUND(index_length/1024/1024) AS idx_mb, table_rows
FROM information_schema.tables
WHERE table_schema = 't16o_db'
ORDER BY data_length DESC;
```

**Active connections**
```sql
SHOW PROCESSLIST;
```

**Check for deadlocks**
```sql
SHOW ENGINE INNODB STATUS\G
```

**Runtime config**
```sql
SELECT * FROM config WHERE config_type = 'batch' ORDER BY config_key;
```

---

## 8. Monitoring & Troubleshooting

### System health check
```bash
# Memory
free -h

# Disk
df -h /

# Load
uptime

# All at once
echo "=== MEMORY ===" && free -h && echo "=== DISK ===" && df -h / && echo "=== LOAD ===" && uptime
```

### Check if a worker is stuck
```bash
# If logs stopped updating, the worker may be deadlocked
docker logs t16o_v1_services_decoder --tail 5 -t

# Check if the process is alive
docker top t16o_v1_services_decoder
```

### Common issues

**Workers not processing:**
1. Check RabbitMQ queue depths — are messages piling up?
2. Check worker logs for errors
3. Check MySQL connections (`SHOW PROCESSLIST`) for locks
4. Restart the stuck worker: `docker compose -f docker-compose.services.yml restart decoder`

**MySQL deadlocks:**
1. Reduce `guide_batch_size` to 10 in config table
2. Reduce worker thread counts
3. Check `SHOW ENGINE INNODB STATUS\G` for deadlock details

**Disk filling up:**
1. Check Docker images: `docker images --format 'table {{.Repository}}\t{{.Size}}' | sort -k2 -rh`
2. Prune unused: `docker system prune -f`
3. Check MySQL data: `du -sh /var/lib/docker/volumes/t16o_v1_services_mysql_data/`

**System unresponsive / needs reboot:**
1. Check for zombie Docker shims: `ps aux | grep shim`
2. Check if containerd is healthy: `systemctl status containerd`
3. If Docker daemon is stuck: `sudo systemctl restart docker` (less disruptive than full reboot)
4. Last resort: reboot from AWS console

---

## 9. Boundary & Gap Management

### Run the gap report
```bash
docker cp /home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_wrk/report-gaps.sql \
  t16o_v1_services_mysql:/tmp/
docker exec t16o_v1_services_mysql mysql -u root -prootpassword t16o_db -t \
  -e "source /tmp/report-gaps.sql"
```

### Run gap backfill (use docker exec, NOT docker compose run)
```bash
docker cp /home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_wrk/backfill-gaps.py \
  t16o_v1_services_producer:/app/_wrk/
docker exec t16o_v1_services_producer python3 /app/_wrk/backfill-gaps.py --dry-run
docker exec t16o_v1_services_producer python3 /app/_wrk/backfill-gaps.py
```

### Run boundary validation
```bash
docker cp /home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_wrk/validate-boundaries.sql \
  t16o_v1_services_mysql:/tmp/
docker exec t16o_v1_services_mysql mysql -u root -prootpassword t16o_db -t \
  -e "source /tmp/validate-boundaries.sql"
```

### Backfill boundary peek signatures
```bash
docker cp /home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_wrk/backfill-boundary-peek.py \
  t16o_v1_services_producer:/app/_wrk/
docker exec t16o_v1_services_producer python3 /app/_wrk/backfill-boundary-peek.py --dry-run
docker exec t16o_v1_services_producer python3 /app/_wrk/backfill-boundary-peek.py
```

---

## 10. API Testing

### Query transactions (bubble map data)
```bash
curl -s "https://svcs.the16oracles.io/api/txs?mint=<MINT>&limit=10&result_mask=207" \
  -H "X-API-Key: admin_master_key" | python3 -m json.tool | head -50
```

### Query by signature
```bash
curl -s "https://svcs.the16oracles.io/api/txs?signature=<SIG>&limit=1&result_mask=207" \
  -H "X-API-Key: admin_master_key" | python3 -m json.tool | head -50
```

### Query wallet history
```bash
curl -s "https://svcs.the16oracles.io/api/txs?address=<WALLET>&limit=50&result_mask=113" \
  -H "X-API-Key: admin_master_key" | python3 -m json.tool | head -50
```

### Check API health
```bash
curl -s "https://svcs.the16oracles.io/api/health" | python3 -m json.tool
```

---

## 11. ChromaDB (Search)

### SSH to ChromaDB EC2
```powershell
ssh -i C:\Users\wgadb\source\repos\T16O\docker\kp-velintra.pem ubuntu@54.83.147.188
```

### Check status
```bash
curl -s http://localhost:8000/api/v2/tenants/default_tenant/databases/default_database/collections
```

### Re-ingest (after container restart — data not persisted)
```bash
cd /home/ubuntu/t16o/docker
docker compose -f docker-compose.chromadb.yml run -d --rm ingest
```

### Admin UI
- **URL:** http://34.196.15.116:3001
- **Connection string:** http://54.83.147.188:8000

---

## 12. Key File Locations

| What | Path (on EC2) |
|------|---------------|
| Docker compose (services) | `/home/ec2-user/t16o/docker/docker-compose.services.yml` |
| Docker compose (chromadb) | `/home/ubuntu/t16o/docker/docker-compose.chromadb.yml` |
| Worker Python scripts | `/home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_wrk/` |
| Stored procedures | `/home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_db/procedures/` |
| Table schemas | `/home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_db/tables/` |
| Migrations | `/home/ec2-user/t16o/sql/shred/_theGuide/_build_all/_db/migrations/` |
| Viewer HTML | `/home/ec2-user/t16o/sql/shred/ClusterMaps/bmap-viewer/bmap-viewer-v3.html` |
| Guide config | `/home/ec2-user/t16o/docker/guide-config.docker.json` |
| SSH keys | `C:\Users\wgadb\source\repos\T16O\docker\*.pem` (local only) |

---

## 13. Connection Reference

| Service | Host | Port | Credentials |
|---------|------|------|-------------|
| Services EC2 SSH | 34.196.15.116 | 22 | key: `t160_v1_services.pem` |
| ChromaDB EC2 SSH | 54.83.147.188 | 22 | key: `kp-velintra.pem` |
| MySQL (services) | 34.196.15.116 | 3397 | root / rootpassword |
| MySQL (local) | 127.0.0.1 | 3396 | root / rootpassword |
| RabbitMQ AMQP | 34.196.15.116 | 5692 | admin / admin123 (vhost: t16o_mq) |
| RabbitMQ UI | 34.196.15.116 | 15692 | admin / admin123 |
| Gateway API | 34.196.15.116 | 5100 | X-API-Key header |
| Gateway (public) | svcs.the16oracles.io | 443 | X-API-Key header |
| .NET API | 34.196.15.116 | 5200 | — |
| Web UI | 34.196.15.116 | 80 | — |
| ChromaDB HTTP | 54.83.147.188 | 8000 | — |
| ChromaDB Search | 54.83.147.188 | 5300 | — |
| ChromaDB Admin | 34.196.15.116 | 3001 | — |
| API test key | — | — | `admin_master_key` |
