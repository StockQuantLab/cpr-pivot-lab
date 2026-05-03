# Multi-Machine Setup — CPR Pivot Lab

How to run this project on 2–3 machines (yours + a friend or two), each with its own
Kite account, Telegram chat, and local data — without sharing brokerage credentials or
DuckDB files.

Designed for a small trusted group (≤5 people). For anything larger, switch to a
hosted Postgres + S3 setup; this doc deliberately stays at the "git + Doppler + local
DuckDB" tier.

## Trust Boundary — What Each Machine Owns

| Layer | Shared? | Mechanism |
|-------|---------|-----------|
| Source code | Yes | `git pull` from the same remote |
| Secret names / schema | Yes | Same env var names documented here |
| Doppler project/account | **No** | Each person can create their own `cpr-pivot-lab` Doppler project |
| Per-user secrets (Kite, Telegram, SMTP) | **No** | Stored only in that person's Doppler config |
| Parquet OHLCV data (`data/parquet/`) | Optional | Re-ingest locally **or** seed once via cloud drive |
| DuckDB files (`data/*.duckdb`) | **Never** | Exclusive file locking — must stay machine-local |
| `data/*_replica/` directories | **Never** | Generated locally from the canonical DB |
| `.tmp_logs/`, `*.writelock` | **Never** | Runtime PIDs/locks specific to the host |
| `AGENTS.md`, `CLAUDE.md`, `.claude/`, `.codex/` | **Never** | Local agent/operator notes and settings; gitignored in this repo |
| `.env*`, `.doppler.yaml` | **Never** | Gitignored (`.doppler.yaml` added to `.gitignore`) |

## One-Time Setup (per machine)

```bash
# 1. Clone
git clone <repo-url> cpr-pivot-lab
cd cpr-pivot-lab

# 2. Python toolchain
uv sync

# 3. Doppler — install + login (one-time)
# Windows: scoop install doppler   |   macOS: brew install dopplerhq/cli/doppler
# First create your own Doppler project/config in the web UI, then bind this checkout.
doppler login
doppler setup --project cpr-pivot-lab --config dev --no-interactive

# 4. Postgres (OPTIONAL — only if using the LLM agent or signal storage; skip for paper-only)
# docker-compose.yml reads ${POSTGRES_PASSWORD} from Doppler — must run inside doppler run
doppler run -- docker compose up -d
doppler run -- uv run pivot-db-init         # create Postgres schema (once)

# 5. NSE equity allowlist (not in git — copy from sibling repo or download from NSE)
# Place at data/NSE_EQUITY_SYMBOLS.csv (SERIES=EQ rows only)

# 6. Sanity check
doppler run -- uv run python -c "from config.settings import Settings; s = Settings(); print('Doppler OK; Kite key set:', bool(s.kite_api_key))"
```

## Doppler — Independent Project Per Person

Default model: each teammate creates their **own Doppler account/project/config**. You do
not need to invite them to your Doppler project or share any of your credentials. The only
thing that must match across machines is the env var names used by this repo.

```
Your Doppler account
└── Project: cpr-pivot-lab
    └── Config: dev              (your Kite + Telegram + local settings)

Friend's Doppler account
└── Project: cpr-pivot-lab
    └── Config: dev              (their Kite + Telegram + local settings)
```

### Step 1 — Each person creates their own Doppler project

In the Doppler web UI: **Projects → Create** → name it `cpr-pivot-lab`. Use the default
`dev` config unless you intentionally want another config name.

### Step 2 — Set repo-required keys in that config

| Key | Example value |
|-----|---------------|
| `OLLAMA_MODEL` | `llama3.2` |
| `OLLAMA_BASE_URL` | `http://localhost:11434` (or shared Ollama Cloud endpoint) |
| `OLLAMA_API_KEY` | Only if using Ollama Cloud |
| `POSTGRES_PASSWORD` | Any local Docker Postgres password |
| `KITE_API_KEY` | Each user's own Kite Connect app |
| `KITE_API_SECRET` | Same Kite app |
| `KITE_ACCESS_TOKEN` | Generated daily via `pivot-kite-token` (per user) |
| `TELEGRAM_BOT_TOKEN` | Each user's own bot (or one shared bot is fine) |
| `TELEGRAM_CHAT_IDS` | The user's own chat ID — alerts route to them |
| `SMTP_USER`, `SMTP_PASSWORD`, `ALERT_TO_EMAIL` | Each user's email setup |

Never copy `KITE_ACCESS_TOKEN` between people or machines.

### Step 3 — Bind the repo checkout to their config

```bash
cd cpr-pivot-lab
doppler login
doppler setup --project cpr-pivot-lab --config dev --no-interactive
```

This writes `.doppler.yaml` to the repo root (already gitignored). Every `doppler run`
from that directory now uses the right config automatically.

The local CLI setup can be automated once the user has created the Doppler project/config
and entered secrets in the web UI. Creating the Doppler account/project and entering real
Kite/Telegram/SMTP values remains a one-time manual step.

### Optional: shared Doppler project

For a tightly trusted team, you can instead use one shared Doppler project with a child
config per person, such as `dev_kannan` and `dev_friend`. That reduces setup duplication
but expands the trust boundary, so do it only when everyone is comfortable sharing access
to the same Doppler project.

## Kite Token — Daily Per-User Step

Kite Connect access tokens are per app/user and are valid only until the next
06:00 IST token reset; a token cannot be shared across machines. Each user runs
this once per trading day before market open:

```bash
doppler run -- uv run pivot-kite-token --apply-doppler
```

Without `--apply-doppler`, the command prints a manual `doppler secrets set` command
instead of persisting it. If you forget this step, EOD ingestion and `daily-live` will
fail with an auth error.

## Data — Pick One Path

### Option A (recommended): Each machine ingests its own

Zero infra, full reproducibility. The first run is long but unattended.

```bash
# Fetch Kite instrument master (required before --universe current-master)
doppler run -- uv run pivot-kite-ingest --refresh-instruments --exchange NSE

# First-time backfill — daily bars (~3–5 hours, foreground)
doppler run -- uv run pivot-kite-ingest \
  --universe current-master --from 2015-01-01 --to <today> \
  --resume --skip-existing

# First-time backfill — 5-minute bars (required for ATR, day-pack, replay, live)
doppler run -- uv run pivot-kite-ingest --5min \
  --universe current-master --from 2015-01-01 --to <today> \
  --resume --skip-existing

# Build runtime tables (~30–60 min for full history)
# See docs/RUNTIME_REBUILD.md for full details and resumable staged rebuilds
# --duckdb-threads / --duckdb-max-memory are accepted by pivot-build in this repo.
doppler run -- uv run pivot-build --force --full-history --staged-full-rebuild \
  --allow-full-history-rebuild \
  --duckdb-threads 4 --duckdb-max-memory 24GB --batch-size 128

# Post-build state refresh (required for virgin_cpr_flags → prev_is_virgin).
# Staged full rebuild builds market_day_state before virgin_cpr_flags, so rerun state
# after virgin exists. If you rebuild tables manually, run --table virgin before state.
doppler run -- uv run pivot-build --table state --refresh-since 2015-01-01

# Verify
doppler run -- uv run pivot-build --status
doppler run -- uv run pivot-data-quality --refresh --full
doppler run -- uv run pivot-data-validate

# Smoke test before trusting the machine for live paper trading
# Intentionally scoped to two liquid symbols; real replay/live runs should use
# the dated saved universe or --all-symbols as documented in the operator runbook.
doppler run -- uv run pivot-paper-trading daily-replay \
  --multi --strategy CPR_LEVELS --trade-date <recent_known_good_date> \
  --symbols SBIN,RELIANCE --no-alerts
```

After the initial backfill, the daily routine is:

```bash
doppler run -- uv run pivot-refresh --eod-ingest --date <today> --trade-date <next>
```

Both teammates run this independently. Each machine ends up with the same data because
the source (Kite) and code (git) are the same.

### Option B: Seed once via cloud drive

Saves the second teammate's first-run ingestion. Only `data/parquet/` is safe to share —
**never** sync `*.duckdb` or `*_replica/` directories.

1. Person A finishes their backfill.
2. Person A zips and uploads `data/parquet/` to Google Drive / OneDrive / Dropbox.
   Size depends on symbol count and history depth; expect roughly 10–50+ GB for
   multi-year daily + 5-minute full-universe data.
3. Person B downloads and extracts to their own `data/parquet/`.
4. Person B runs `pivot-build --force --full-history --staged-full-rebuild
   --allow-full-history-rebuild ...` to construct their local DuckDB tables from the
   shared parquet. See `docs/RUNTIME_REBUILD.md` for the full command and the required
   post-build state refresh (`--table state --refresh-since`).

After seeding, both machines run their own daily EOD ingestion as in Option A. Do **not**
keep syncing parquet — divergence is fine because each machine re-ingests from the same
upstream Kite.

## Daily Workflow — Both Machines

```bash
# Pull the latest code
git pull

# If schema or runtime tables changed (check commit messages for migrations / new tables):
doppler run -- uv run pivot-build --status
# For targeted rebuilds, follow docs/RUNTIME_REBUILD.md — table-specific flags vary
# (e.g. --table pack also needs --allow-full-pack-rebuild for full-universe rebuilds)

# Refresh Kite token for the day
doppler run -- uv run pivot-kite-token --apply-doppler

# EOD ingest after market close (~3:45 PM IST onward)
doppler run -- uv run pivot-refresh --eod-ingest --date <today> --trade-date <next>

# Pre-market readiness check next morning
doppler run -- uv run pivot-paper-trading daily-prepare --trade-date today --all-symbols
doppler run -- uv run pivot-data-quality --date <today>    # must print "Ready YES"

# Start paper trading (preferred: supervisor for logs, heartbeat, exit diagnostics)
# Start at/after 09:16 IST — live needs the 9:15 candle to resolve direction
doppler run -- uv run pivot-paper-supervisor -- --multi --strategy CPR_LEVELS --trade-date today
# Or directly: pivot-paper-trading daily-live --multi --strategy CPR_LEVELS --trade-date today
```

Backtests run independently on each machine; results are not designed to be merged
across machines.

## What MUST Stay Local (Never Sync)

- `data/market.duckdb`, `data/backtest.duckdb`, `data/paper.duckdb` and all `*.wal`
  / `*.writelock` siblings — DuckDB uses exclusive file locks; a copy from another
  machine can corrupt the file.
- `data/market_replica/`, `data/backtest_replica/`, `data/paper_replica/` — generated
  locally by `ReplicaSync`.
- `.tmp_logs/` — runtime PIDs and lock files for the live process.
- `.env*`, `.doppler.yaml`, `.mcp.json`, `.claude/`, `.codex/`, `AGENTS.md`,
  `CLAUDE.md` — local-only and gitignored in this repo.

## Sharing the Dashboard With Each Other (Optional)

If one teammate wants to view the other's `pivot-dashboard`, use **Tailscale Serve**
following the existing recipe in [`docs/SETUP.md`](SETUP.md#remote-dashboard-access-tailscale).
That gives a tailnet-only HTTPS URL with no public exposure. **Security note**: there is
no app-level auth on the dashboard — Tailscale tailnet membership and ACLs are the
sole access control. Ensure only trusted tailnet members can reach the Serve endpoint.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `ModuleNotFoundError` after `git pull` | `uv sync` — new dependency was added |
| `pivot-build --status` shows missing tables after pull | Identify the affected table and follow `docs/RUNTIME_REBUILD.md` — table-specific flags vary |
| Doppler run says "no config selected" | Run `doppler setup` in the repo root |
| Kite auth error on EOD ingest | Refresh token: `doppler run -- uv run pivot-kite-token --apply-doppler` |
| Friend's alerts arriving on your Telegram | Check that their `TELEGRAM_CHAT_IDS` is set in their own Doppler config, not inherited from yours |
| `IOException: file is already open` on DuckDB | Stop all local processes touching that DB (`pivot-dashboard`, `pivot-backtest`, `pivot-paper-trading`, `pivot-build`). Also never copy `*.duckdb` between machines. |
| Dashboard shows zero rows after seeding parquet from a friend | DuckDB tables are not in parquet — run `pivot-build --force --full-history --staged-full-rebuild --allow-full-history-rebuild` then follow `docs/RUNTIME_REBUILD.md` for the required post-build state refresh. |

## When to Outgrow This Setup

Move to hosted infra when any of the following becomes true:

- More than ~5 active users.
- A need for shared, real-time backtest result history (rather than per-machine).
- Compliance / audit trail on secret access.
- Running paper-trading on a server rather than personal laptops.

At that point: hosted Postgres for `agent_sessions` / `signals`, S3 + Iceberg or DuckDB
on object storage for parquet, and a single Doppler `prd` config injected into the
deployed service.
