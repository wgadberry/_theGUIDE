# Backlog Plan - T16O Guide Pipeline

## 1. Docker Disk Space Issue (INVESTIGATED)

**Status:** Investigated - orphaned volumes identified, user chose to back up before pruning

**Problem:** Docker is consuming excessive disk space

**Investigation needed:**
- [ ] Check Docker disk usage: `docker system df`
- [ ] Identify large containers/images/volumes
- [ ] Check for runaway logs in containers
- [ ] Check MySQL/RabbitMQ data volumes
- [ ] Look for orphaned volumes/images

**Potential causes:**
- MySQL binlog/redo logs accumulating
- RabbitMQ message persistence
- Container logs not rotating
- Orphaned Docker volumes
- Build cache accumulation

**Commands to investigate:**
```bash
docker system df -v
docker volume ls
docker images -a
docker ps -a --size
```

---

## 2. Completed Items

- [x] Fix smart sync to fetch newer transactions (guide-producer.py)
- [x] Add WHERE NOT EXISTS to prevent ID gaps (sp_tx_prepopulate_lookups.sql)
- [x] Fix mq-publish.py to publish directly to queue (exchange binding issue)

---

*Last updated: 2026-01-16*
