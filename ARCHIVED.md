# ARCHIVED — bifrost-trade-socket

**Status:** RETIRED / reference-only (Wave 14G-F)  
**Date:** 2026-08-31  
**Authority IB edge:** Platform IB Gateway Plugin → `redis-ib` (`bifrost-platform-plugin`)

This repository is **not** part of the day-to-day Trade runtime or Inner Loop.

- Do **not** start `run_ib_ingestor` / `run_ib_account_agent` / `run_ib_operator` by default.
- Optional escape hatch: `docker compose -f docker-compose.dev.yml --profile legacy-ib up` (must not dual-write with Plugin).
- Design / phases: `bifrost-trade-infra/docs/WAVE_14G_F_SOCKET_RETIREMENT.md`
- D10 remains BLOCKED — retirement does not unlock live `place_order`.

**Reference retention:** keep this repo (or GitHub archive) ≥90 days for Redis contract archaeology, then Owner may remove it from the multi-root workspace.
