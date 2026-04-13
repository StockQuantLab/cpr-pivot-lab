# Setup Guide — CPR Pivot Lab

## Baseline Runbook

1. Prepare environment (`uv sync`, docker + Doppler secrets).
2. Convert raw CSVs with `pivot-convert`.
3. Build runtime tables with the staged full-history rebuild in `docs/RUNTIME_REBUILD.md`.
4. Refresh the issue registry with `pivot-data-quality --refresh`, then gate the target trade date with `pivot-data-quality --date <trade-date>`.
5. Run baseline sweeps with `pivot-backtest` or `pivot-campaign`.
6. Review results in `pivot-dashboard`.
7. For post-March 9, 2026 market data, refresh this repo directly from Kite using the step-by-step flow in `docs/KITE_INGESTION.md`.
8. For daily paper trading, run 4 independent sessions per trading day (CPR_LEVELS LONG/SHORT, FBR LONG/SHORT). Paper trading IS the validation — see `docs/PAPER_TRADING_RUNBOOK.md` for the operator steps. `daily-prepare` runs the same DQ readiness gate automatically before it returns.

## Canonical Commands

```bash
doppler run -- uv run pivot-convert
doppler run -- uv run pivot-build --force --full-history --staged-full-rebuild --duckdb-threads 4 --duckdb-max-memory 24GB --batch-size 64
doppler run -- uv run pivot-data-quality --refresh
doppler run -- uv run pivot-data-quality --date 2026-03-27
doppler run -- uv run pivot-backtest --strategy CPR_LEVELS --universe-name gold_51 --start 2015-01-01 --end 2025-12-31 --save
doppler run -- uv run pivot-campaign --full-universe --start 2015-01-01 --end 2025-03-31
doppler run -- uv run pivot-dashboard
doppler run -- uv run pivot-kite-ingest --refresh-instruments --exchange NSE
doppler run -- uv run pivot-kite-ingest --from 2026-03-10 --to 2026-03-20 --symbols SBIN,RELIANCE
doppler run -- uv run pivot-kite-ingest --from 2026-03-10 --to 2026-03-20 --symbols SBIN,RELIANCE --5min --resume
doppler run -- uv run pivot-build --table pack --refresh-since 2026-03-10 --batch-size 64
doppler run -- uv run pivot-paper-trading walk-forward --start-date 2026-03-10 --end-date 2026-03-20 --symbols SBIN,RELIANCE --strategy CPR_LEVELS
doppler run -- uv run pivot-paper-trading walk-forward-replay --start-date 2026-03-10 --end-date 2026-03-20 --symbols SBIN,RELIANCE --strategy CPR_LEVELS
doppler run -- uv run pivot-paper-trading daily-live --trade-date 2026-03-23 --symbols SBIN,RELIANCE --strategy CPR_LEVELS
```

## Remote Dashboard Access (Tailscale)

Expose the dashboard (`pivot-dashboard` on `127.0.0.1:9999`) to your devices and collaborators via Tailscale Serve — tailnet-only, no public internet exposure.

### Prerequisites

1. **Tailscale installed and connected** on this machine ([tailscale.com/download](https://tailscale.com/download)).
2. **Tailscale account** — sign up at [login.tailscale.com](https://login.tailscale.com).
3. **Dashboard running** — `doppler run -- uv run pivot-dashboard` must be up on port 9999.

### Step 1: Enable HTTPS Certificates (one-time)

1. Go to [Tailscale Admin → DNS](https://login.tailscale.com/admin/dns).
2. Enable **HTTPS Certificates**.
3. **Do NOT enable Funnel** — Funnel exposes services to the public internet, which is the opposite of what we want.

### Step 2: Set a Custom Hostname

Choose a recognizable machine name — this becomes the URL subdomain:

```bash
tailscale up --hostname cpr-pivot-lab-dashboard --accept-routes --accept-dns
```

Verify the change:

```bash
tailscale status --self
# Should show: 100.x.x.x  cpr-pivot-lab-dashboard  ...
```

### Step 3: Enable Tailscale Serve

Serve proxies the local dashboard through a tailnet-only HTTPS URL:

```bash
tailscale serve --bg 9999
```

This outputs the access URL:

```
https://cpr-pivot-lab-dashboard.<tailnet-id>.ts.net
|-- proxy http://127.0.0.1:9999
```

The `--bg` flag makes it persistent (survives terminal close and reboots).

Verify it's running:

```bash
tailscale serve status
```

### Step 4: Access from Your Devices

| Device | Setup | Access |
|--------|-------|--------|
| **This PC** | Already connected | `https://cpr-pivot-lab-dashboard.<tailnet-id>.ts.net` or `http://localhost:9999` |
| **Other laptops** | Install Tailscale → log in with same account | Open the HTTPS URL in browser |
| **Phone/tablet** | Install Tailscale app → log in with same account | Open the HTTPS URL in mobile browser |

All devices must be logged into the same Tailscale account. No additional configuration needed.

### Step 5: Share with a Friend (External Access)

To grant access to someone outside your tailnet **without** giving them full network access:

1. Go to [Tailscale Admin → Machines](https://login.tailscale.com/admin/machines).
2. Click the `⋯` menu on `cpr-pivot-lab-dashboard`.
3. Select **Share**.
4. Enter your friend's email address.

Your friend will:
1. Receive an invite link via email.
2. Install Tailscale on their device.
3. Accept the invite.
4. Open the dashboard URL in their browser.

**Security properties of device sharing:**
- Shared devices are **quarantined** — the recipient can only reach the shared machine's serve endpoint, not other devices on your tailnet.
- The share is **inbound-only** — the recipient cannot initiate outbound connections from the shared device.
- You can **revoke** the share at any time from the admin console.

### Managing Serve

```bash
tailscale serve status              # show current config
tailscale serve --bg 9999           # start (if stopped)
tailscale serve --https=443 off     # stop serving
```

### Security Summary

| Layer | Protection |
|-------|-----------|
| Network | Tailnet-only (WireGuard encrypted, no public route) |
| TLS | Auto HTTPS via Tailscale CA |
| Device access | Only authenticated tailnet members or explicitly shared users |
| Dashboard binding | Remains on `127.0.0.1:9999` — never exposed to LAN or internet |
| Lateral movement | Quarantined shares — collaborators cannot reach other tailnet devices |

### Troubleshooting

| Symptom | Fix |
|---------|-----|
| `tailscale status` shows "logged out" but system tray shows connected | CLI and GUI may be out of sync. Run `tailscale ip` to verify connectivity — if it returns a `100.x.x.x` IP, Tailscale is working. |
| `tailscale serve` says "Serve is not enabled" | Visit the URL shown in the error message to enable HTTPS certificates in the admin console. |
| `listener already exists for port 443` | Run `tailscale serve reset` then retry. |
| URL not loading from another device | Ensure Tailscale is running and logged in on that device. Check `tailscale status` shows the machine as connected. |
| `tailscale serve status` shows nothing after `--bg` | The command may have printed the URL but not persisted. Re-run `tailscale serve --bg 9999`. |

## Paper Trading Notes

- `walk-forward` is the fast validator. Use `walk-forward-replay` only when you need the full paper-session replay path.
- Walk-forward is launched from the CLI and reviewed on the dedicated `/walk_forward` dashboard page.
- Active paper sessions and archived paper-session history are shown under `Paper Sessions` at `/paper_ledger`.
- `/backtest` and `Strategy Analysis` remain saved-backtest views, not paper-session views.
- Coding tools such as Copilot, Codex, and Claude Code should reference the CLI commands above when guiding operators.
- `docs/KITE_INGESTION.md` is the command reference for token refresh, instrument refresh, daily ingestion, and 5-minute ingestion.
- `docs/RUNTIME_REBUILD.md` is the command reference for safe staged full-history runtime rebuilds and resume points.
- `pivot-paper-trading daily-live --feed-source local` runs the live websocket code path against historical `intraday_day_pack` data. Add `--no-alerts` only when you want to suppress Telegram/email dispatch for testing.
- `pivot-paper-trading cleanup` is date-scoped: `--trade-date YYYY-MM-DD --apply`. Run it once per date you want to clear.

## Data Policy

- Keep `data/` out of source control.
- Keep `raw/` and `parquet/` canonical for local operations only.
- Use `pivot-clean` after major run cycles to clear logs/progress artifacts.

## Strategy Conventions

- Use `--strategy CPR_LEVELS` and `--strategy FBR` for baseline decisions.
- Use `--direction LONG` or `--direction SHORT` only for diagnostic splits.
- Keep non-baseline strategies off by default. Enable research-only strategies explicitly.
