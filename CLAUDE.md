# WIOM Recharge Dashboard

## Project Overview
This is the WIOM Recharge Lifecycle Analytics Dashboard. It fetches live data from Metabase (Snowflake) and generates a self-contained HTML dashboard deployed on GitHub Pages.

- **Live URL**: https://vikaswiom.github.io/wiom-recharge-dashboard/
- **GitHub**: https://github.com/Vikaswiom/wiom-recharge-dashboard
- **Auto-updates**: Every 15 minutes via GitHub Actions

## Key Files
- `dashboard_builder.py` — Main script. Queries Metabase API, processes analytics, generates HTML with Plotly.js charts
- `app.py` — Flask server (optional Render deployment)
- `.github/workflows/build-dashboard.yml` — GitHub Actions CI/CD
- `requirements.txt` — Dependencies (flask, gunicorn, numpy)

## Architecture
- **Data source**: Metabase API → Snowflake DB (ID 113)
- **API key**: `METABASE_API_KEY` env var (GitHub Secrets) or `C:\credentials\.env` (local)
- **Output**: `output_recharge/index.html` (GitHub Pages) + `recharge_dashboard.html`
- **Timezone**: `datetime.utcnow() + timedelta(hours=5, minutes=30)` for IST
- **Plan durations**: ALL included (1, 2, 7, 14, 28, 112, 360, etc.)

## Dashboard Tabs (7 tabs)
1. **Conversion** — Rate, time buckets chart, month-wise table (excludes trial users)
2. **First Plan** — Bar chart (all durations), month-wise grouped bar + table
3. **Segments** — Segment table, churned users distribution charts + table
4. **Daily Metrics** — Tables: installs/active, expired/recharged (no charts)
5. **Plan Cohort** — Upgrade/downgrade charts, transition matrix, journeys, Sankey flow
6. **Non-Converted** — Segments table, purchase frequency chart + table
7. **R-Day Report** — Risk buckets (R0, R1-R7, R8-R15, R15+), detail table (R0-R15 + R15+ combined)

## Design Principles
- Each tab has a "What does this mean?" section at the TOP for non-data-savvy team
- Month-wise tables sorted newest first
- Conversion rate excludes still-in-trial users
- R-Day detail table capped at R15 (R15+ combined into one row)

## Common Tasks
- **Add/modify a tab**: Edit `dashboard_builder.py` — data processing section, chart section, HTML template section
- **Change SQL query**: Edit the `SQL_QUERY` variable at the top of `dashboard_builder.py`
- **Run locally**: `python dashboard_builder.py` (needs API key in `C:\credentials\.env`)
- **Deploy**: Push to master branch — GitHub Actions auto-builds and deploys

## Environment Notes
- Windows 11, Git Bash — use `powershell -NoProfile -Command` instead of `ls`, `cat`, `cp`
- HEREDOC with `cat` doesn't work in git commit — use inline commit messages
- Metabase API key stored at `C:\credentials\.env`
