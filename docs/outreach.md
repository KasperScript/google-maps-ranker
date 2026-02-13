# Outreach Stage

The outreach stage turns the top clinics from `radius_scan_merged_results.csv` into a manual-review outreach queue with evidence.

Safety guarantees:
- It never sends emails automatically.
- It never submits forms automatically.
- Playwright assist mode stops before submit and records screenshots.

## Prerequisites
- Run a radius scan that produces a merged CSV.
- Create `prompts/email_template_no_pricing_pl.txt` manually before running outreach.
- Set environment variables in your terminal (recommended; no `.env` required).
- Optionally set `GEMINI_API_KEY` to enable Gemini analysis.

## Environment Variables (No .env Required)
Export keys in your current shell session (macOS zsh/bash):

```bash
export GEMINI_API_KEY="your-gemini-key"
export GOOGLE_MAPS_API_KEY="your-google-maps-key"
```

Optional Gmail OAuth paths:

```bash
export GMAIL_OAUTH_CLIENT_JSON="$HOME/Downloads/client_secret_xxx.json"
export GMAIL_OAUTH_TOKEN_JSON="$HOME/Downloads/token.json"
```

Quick preflight (prints only lengths):

```bash
python3 scripts/preflight_env.py
```

Run outreach after exporting:

```bash
python3 run.py --outreach --outreach-input out/results.json --outreach-top-n 5 --outreach-max-pages 10 --outreach-out out/outreach_runs
```

Reminder: never commit secrets. Keep `.env` and OAuth JSON files out of git.

## Step 1. Run Radius Scan (Extreme Multi-Hub)
Example:

```bash
python run.py --radius-scan --extreme --centers alk,centralny,galeria_polnocna --out out/radius_scan_extreme_all
```

This should produce `out/radius_scan_extreme_all/radius_scan_merged_results.csv`.

## Step 2. Run Outreach Stage On The Merged CSV
Explicit input path:

```bash
python run.py \
  --outreach \
  --outreach-input out/radius_scan_extreme_all/radius_scan_merged_results.csv \
  --outreach-top-n 30 \
  --outreach-max-pages 30 \
  --outreach-out out/outreach
```

Auto-detect the latest merged CSV under `out/`:

```bash
python run.py --outreach --outreach-top-n 30
```

Outreach outputs are now written into a run-specific folder under `--outreach-out`, for example `out/outreach_runs/20260127_153000/`. The CLI prints the exact run directory.

Useful flags:
- `--outreach-refresh-web` bypasses the fetched-page cache.
- `--outreach-refresh-places` is accepted for parity with earlier refresh behavior.

## Step 3. Review The Queue And Use Assist Mode
Review:
- `<run_dir>/outreach_queue.jsonl`
- `<run_dir>/outreach_results.json`
- `<run_dir>/outreach_summary.txt`
- `<run_dir>/evidence/<clinic_slug>/`

Evidence per clinic includes:
- `pages/` HTML snapshots.
- `pdf/` pricing PDFs.
- `extracted_pricing.txt` when pricing text was found in HTML.
- `gemini/` prompt text, prompt hashes, contexts, and raw responses.
- `screenshots/` when Playwright assist mode is enabled.

Playwright assist mode (autofill only, no submit):

```bash
python run.py \
  --outreach \
  --outreach-input out/radius_scan_extreme_all/radius_scan_merged_results.csv \
  --outreach-playwright-assist
```

If a captcha is detected, the clinic is marked as blocked for manual handling.

## Gmail Drafts And Sending (Optional)
Gmail integration is explicit opt-in. Drafts are the primary safe path. Sending is a second step behind hard safety rails.

Setup:
- Install dependencies: `python -m pip install -r requirements.txt`
- Google Cloud step 1: Create or select a project.
- Google Cloud step 2: Enable the Gmail API.
- Google Cloud step 3: Create OAuth client credentials for a Desktop App.
- Google Cloud step 4: Download the OAuth client JSON.
- Place the OAuth client JSON at `credentials.json` in the repo root, or set `GMAIL_OAUTH_CLIENT_JSON`.
- The OAuth token is stored at `token.json` in the repo root, or set `GMAIL_OAUTH_TOKEN_JSON`.

Draft mode (recommended first, does not send):

```bash
python run.py \
  --outreach \
  --outreach-input out/radius_scan_extreme_all/radius_scan_merged_results.csv \
  --outreach-out out/outreach_runs \
  --gmail-drafts \
  --gmail-max-drafts 5
```

Auto-send mode (requires acknowledgement and disabling dry-run):

```bash
python run.py \
  --outreach \
  --outreach-input out/radius_scan_extreme_all/radius_scan_merged_results.csv \
  --outreach-out out/outreach_runs \
  --gmail-send \
  --i-understand-this-will-send-email \
  --gmail-send-no-dry-run \
  --gmail-daily-limit 10 \
  --allow-domains "clinic.pl"
```

Safety rails for sending:
- Sending requires both `--gmail-send` and `--i-understand-this-will-send-email`.
- Sending is dry-run by default until `--gmail-send-no-dry-run` is set.
- Do-not-contact clinics are always skipped.
- A daily limit is enforced using a local send log.
- The optional allowlist blocks sends to unexpected domains.

Evidence and reporting per run:
- QA report: `<run_dir>/QA_REPORT.md`
- Gmail report: `<run_dir>/outreach_gmail_report.txt`
- Per clinic: `<run_dir>/evidence/<clinic_slug>/gmail/attempt_YYYYMMDD_HHMMSS/`
- Each attempt includes `to.txt`, `subject.txt`, `body.txt`, and `status.json`

## Gmail Reply Sync (Incremental)
Sync replies without a fixed 30-day lookback:

```bash
python3 run.py --gmail-sync --gmail-sync-lookback-hours 72
```

How it works:
- The sync window starts at the last successful sync minus a grace overlap (default 30 minutes).
- If no prior sync state exists, it falls back to `now - lookback_hours` (default 72 hours).
- This is designed for laptop-offline gaps; the next run catches up automatically.

Outputs:
- State file: `out/gmail_sync_state.json`
- Replies log: `out/outreach_replies.jsonl`
- Human report: `out/outreach_gmail_sync_report.txt`

Optional scoping:
- Drafts and sends attempt to apply the Gmail label `OrthoRanker`.
- You can scope sync to a label: `--gmail-sync-label OrthoRanker`

## Gemini Doctor (Connectivity Check)
Quick end-to-end probe for the Gemini API:

```bash
python3 scripts/gemini_doctor.py
```

Notes:
- Default model: `gemini-3-pro-preview`
- Fallback order used in the client:
  - `gemini-3-pro-preview`
  - `gemini-3-flash-preview`
  - `gemini-2.5-pro`
  - `gemini-2.5-flash`
