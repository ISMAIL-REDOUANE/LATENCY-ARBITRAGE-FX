# Deployment - Lead-Lag HFT Bot with OANDA v2 Execution

This repository is a fresh and complete production packaging for the Lead-Lag
HFT bot: Binance live order-book feed detects lead-lag arbitrage, and the
engine executes on OANDA over a single persistent REST/TLS connection (no
per-order handshake). This guide deploys it to a fresh Ubuntu 22.04/24.04 VPS
as a hardened, monitored systemd service -- with ONE script.

---

## 1. Prerequisites

| Requirement | What / why |
|-------------|------------|
| Ubuntu 22.04 or 24.04 VPS | LTS, systemd, apt |
| root (or sudo) | installs deps, creates user/service |
| OANDA **practice** account + API token | execution broker (v20 REST) |
| Telegram bot token + chat id (optional) | for monitor_oanda.sh alerts |
| Egress to Binance + OANDA endpoints | WSS/HTTPS outbound |

> A 1 vCPU / 1-2 GB RAM VPS is enough. Choose a region close to both
> `stream.binance.com` and your OANDA practice endpoint (e.g. eu-west / us-east).
> Every millisecond of distance reduces the lead-lag edge you capture.

---

## 2. One-command deployment

```bash
sudo -i
git clone https://github.com/ISMAIL-REDOUANE/LATENCY-ARBITRAGE-FX.git
cd LATENCY-ARBITRAGE-FX/latency_arb
sudo bash scripts/deploy_oanda.sh
```

`deploy_oanda.sh` performs the entire pipeline automatically:

1. Detects package manager (apt/dnf) and installs all deps:
   `build-essential cmake ninja-build libboost-all-dev libssl-dev
   nlohmann-json3-dev libzmq3-dev libsodium-dev git python3`.
2. Clones the repo if not present (`/opt/latency-arb`).
3. Builds Release binaries `lead_lag` + `oanda_probe`
   (`-O3 -march=native -DNDEBUG`).
4. Creates the dedicated `leadlag` system user.
5. Creates `/opt/lead_lag` (binaries) and `/var/log/lead_lag` (logs).
6. Generates a secured `/etc/lead_lag/lead_lag.env` ( chmod 600).
7. Installs + enables the hardened `lead_lag_oanda.service`.
8. Runs an `oanda_probe` smoke test (places one tiny demo order).
9. Starts the service and verifies it is active.
10. Prints a summary (status, log locations, monitoring, stop/restart).

Optional flags:

```bash
sudo bash scripts/deploy_oanda.sh --no-prompt          # read creds from env vars
sudo bash scripts/deploy_oanda.sh --no-probe           # skip demo order
sudo bash scripts/deploy_oanda.sh --no-build           # reuse existing binaries
sudo bash scripts/deploy_oanda.sh --no-start               # install only
sudo bash scripts/deploy_oanda.sh --dry-run            # preview, change nothing
sudo bash scripts/deploy_oanda.sh --env-file=my.env    # install a pre-written file
```

---

## 3. Getting an OANDA API token (step-by-step)

OANDA uses a **bearer token** for the REST v20 API.

1. Create a **practice** account at oanda.com.
2. Log into the dashboard (`app.oanda.com`) and open **Account Management**.
3. Click **Manage API Access** (under your profile).
4. Click **Generate new API key** / **Generate Token**.
5. Copy the token immediately (it is shown only once). This is
   `OANDA_TOKEN`.
6. Note your **numeric account ID** (shown near your account name,
   e.g. `101-004-XXXX-001`). This is `OANDA_ACCOUNT_ID`.

When prompted by `deploy_oanda.sh`:

- `OANDA API token` -> your token
- `OANDA account ID` -> your numeric id
- `OANDA host` -> `api-fxpractice.oanda.com` (demo), or `api-fxtrade.oanda.com` (live)
- `OANDA instrument` -> e.g. `XAU_USD`
- `DRY_RUN` -> `1` (simulated) or `0` (live orders). Start with `1`.

> Security: the token lives only in the `0600` root-owned env file
> `/etc/lead_lag/lead_lag.env`. Never commit tokens to the repository.

---

## 4. Telegram monitoring setup (optional)

1. Open Telegram, search **@BotFather**, use `/newbot`, pick a name and copy
   the returned `BOT_TOKEN`.
2. Start a chat with the new bot and send it one message.
3. Get the chat id:
   `curl "https://api.telegram.org/bot<BOT_TOKEN>/getUpdates"` and read
   `result[0].message.chat.id`.
4. Add the monitor to cron (every minute):

```bash
sudo cp /opt/lead_lag/monitor_oanda.sh /opt/lead_lag/monitor.sh
sudo chmod 700 /opt/lead_lag/monitor.sh
sudo crontab -e
```

```cron
*/1 * * * * TELEGRAM_BOT_TOKEN=<token> TELEGRAM_CHAT_ID=<id> /opt/lead_lag/monitor.sh
```

The monitor checks every minute and throttles alerts (critical: 15 min,
warning: 60 min):

- Service down -> tries one `systemctl restart` (if `ENABLE_AUTORESTART=1`);
  if it still fails it alerts.
- No trades in `STALE_HOURS` (default 2h) -> warning.
- `CRITICAL` / `FAILED` / `OANDA...FAIL` in the daily log -> warning.

Set `ENABLE_AUTORESTART=0` to disable unattended restart.

---

## 5. Verifying the service

```bash
systemctl status lead_lag_oanda                 # active (running)
journalctl -u lead_lag_oanda -f                 # live logs
tail -f /var/log/lead_lag/trades_telemetry.csv  # fill rows
ls -l /var/log/lead_lag                         # logs dir
```

Healthy sample in the journal:

```
[binance] connected
[oanda ] persistent connection established 3.6.235.12:443
[oanda ] id=1 ord=BUY units=1 status=1 latency=12.3 ms
```

If not healthy, see Section 8.

---

## 6. Demo to Live switch

1. Get a real OANDA token and account id (Section 3) for the live account.
2. Edit `/etc/lead_lag/lead_lag.env`:

```
OANDA_HOST=api-fxtrade.oanda.com
OANDA_ACCOUNT_ID=<live id>
OANDA_TOKEN=<live token>
DRY_RUN=0        # leave 0 so the engine executes live
```

3. Restart:

```bash
sudo systemctl restart lead_lag_oanda
sudo systemctl status lead_lag_oanda
```

> Warning: `DRY_RUN=0` means the engine places REAL orders on OANDA. Run the
> demo for at least one full session first.

---

## 6. Architecture (production)

```
Binance WS (bookTicker feeds)     OANDA v20 REST (HTTPS / persistent TLS)
      |                                   ^
      v                                   | one stream reused, reconnect w/ retry
  aggregator / strategy                  |
      | (threshold + margin)             |
      v                                   |
  execution (broker_sub -> oanda_executor) POST /v3/accounts/{id}/orders
                                                 |
        telemetry -> /var/log/lead_lag/trades_telemetry.csv
                                                 |
  systemd lead_lag_oanda (hardened)          cron monitor_oanda.sh -> Telegram
```

---

## 7. The systemd unit at a glance

`systemd/lead_lag_oanda.service`:

- Runs as `leadlag`, `WorkingDirectory=/opt/lead_lag`.
- Loads secrets from `/etc/lead_lag/lead_lag.env` (`0600`, root-owned).
- `Environment=OANDA_ENABLED=1`.
- `Restart=always`, `RestartSec=5`, crash-loop guard
  (`StartLimitIntervalSec=300`, `StartLimitBurst=10`).
- `After=network-online.target` / `Wants=network-online.target`.
- Hardening: `ProtectSystem=full`, `PrivateTmp=true`, `NoNewPrivileges=true`,
  `ReadWritePaths=/var/log/lead_lag`, real-time scheduling.

---

## 8. Troubleshooting

| Symptom                            | Cause / fix                                       |
|------------------------------------|---------------------------------------------------|
| `Failed to start . process...`      | binary location; restore from the build or rerun deploy |
| `status=203/EXEC`                   | executable path mismatch in the unit / INSTALL_DIR     |
| HTTP `401` in journal               | token invalid/expired -> regenerate (section 3)        |
| HTTP `400`                          | wrong account id or unknown instrument                 |
| TLS / connect refused               | egress/firewall to api-fxpractice.oanda.com            |
| OANDA probe exit non-zero           | probe binary vs env not set; check OANDA_*            |
| No fills yet                         | threshold never met; lower THRESHOLD_PIPS/ LAG buffer |
| telemetry permission denied         | `sudo chown leadlag:leadlag /var/log/lead_lag`         |
| service `failed` (start-limit)      | check `journalctl -u lead_lag_oanda -n 50` (crash loop)|

First port of call: `journalctl -u lead_lag_oanda -e` (replay recent log; it
prints binance connect, oanda connect, id/status/latency per trade).

---

## 9. Stop / disable

```bash
sudo systemctl stop lead_lag_oanda                 # stop (restarts at boot)
sudo systemctl disable lead_lag_oanda            # do not start at boot
sudo systemctl disable --now lead_lag_oanda     # stop + disable

# full removal
sudo rm /etc/systemd/system/lead_lag_oanda.service
sudo systemctl daemon-reload
sudo rm -rf /opt/lead_lag /var/log/lead_lag /etc/lead_lag
```

---

## Disclaimer

Automated trading carries substantial financial risk. The engine places real
orders when `DRY_RUN=0`. Test thoroughly on a practice account before enabling
live (`api-fxtrade.oanda.com`). The authors are not liable for any losses.