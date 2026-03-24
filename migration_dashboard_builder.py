"""PAYG Migration Dashboard — nPayG→PayG migration tracking (CI version)"""
import json, urllib.request, ssl, os
from datetime import datetime, timedelta

ctx = ssl.create_default_context()

# API key: env var (CI) or local file
api_key = os.environ.get('METABASE_API_KEY')
if not api_key:
    with open(r"C:\credentials\.env", 'r') as f:
        for line in f:
            if line.strip().startswith('METABASE_API_KEY='):
                api_key = line.strip().split('=', 1)[1].strip().strip('"')

METABASE_URL = "https://metabase.wiom.in/api/dataset"
DB_ID = 113

SQL_QUERY = """
WITH
partner_migrations AS (
    SELECT
        partner_id,
        MIN(created_time + INTERVAL '330 minute') AS migration_ts_ist,
        CASE
            WHEN TO_DATE(MIN(created_time + INTERVAL '330 minute')) < '2026-03-15'::DATE
            THEN TO_DATE(MIN(created_time + INTERVAL '330 minute')) + 1
            ELSE TO_DATE(MIN(created_time + INTERVAL '330 minute'))
        END AS migration_dt
    FROM prod_db.master_db_read_dbo.payg_migration
    GROUP BY partner_id
),

audit_ranked AS (
    SELECT
        a.nas_id,
        a.partner_account_id,
        a.mobile,
        a.speed_limit_mbps,
        a.plan_expiry_time,
        a.has_mandate,
        a.is_picked_up,
        pm.migration_ts_ist,
        pm.migration_dt,
        ROW_NUMBER() OVER (
            PARTITION BY a.nas_id
            ORDER BY
                CASE WHEN a.record_ingest_date <= pm.migration_dt THEN 0 ELSE 1 END,
                a.record_ingest_date DESC,
                a.plan_expiry_time DESC
        ) AS migration_rn
    FROM prod_db.dbt.payg_migration_audit a
    JOIN partner_migrations pm ON a.partner_account_id = pm.partner_id
),

recharge_seq AS (
    SELECT
        trum.router_nas_id AS nas_id,
        DATEADD('minute', 330, trum.otp_issued_time) AS plan_start_ist,
        tpc.time_limit / 86400 AS plan_days,
        ROW_NUMBER() OVER (PARTITION BY trum.router_nas_id ORDER BY trum.otp_issued_time) AS recharge_num
    FROM t_router_user_mapping trum
    JOIN t_plan_configuration tpc ON tpc.id = trum.selected_plan_id
    WHERE trum.otp = 'DONE' AND trum.store_group_id = 0 AND trum.device_limit > 1
      AND trum.mobile > '5999999999'
),

new_payg_installs AS (
    SELECT nas_id FROM recharge_seq
    WHERE recharge_num = 1 AND plan_days = 2 AND TO_DATE(plan_start_ist) >= '2026-01-26'
),

qualified_customers AS (
    SELECT
        ar.nas_id,
        ar.partner_account_id,
        ar.speed_limit_mbps,
        ar.plan_expiry_time,
        ar.migration_ts_ist,
        ar.migration_dt,
        CASE
            WHEN ar.plan_expiry_time >= ar.migration_ts_ist THEN 'ACTIVE'
            WHEN DATEDIFF('day', TO_DATE(ar.plan_expiry_time), TO_DATE(ar.migration_ts_ist)) = 0 THEN 'R0'
            ELSE 'R1_R30'
        END AS migration_status,
        CASE
            WHEN ar.plan_expiry_time >= ar.migration_ts_ist THEN TO_DATE(ar.plan_expiry_time)
            ELSE TO_DATE(ar.migration_ts_ist)
        END AS due_date
    FROM audit_ranked ar
    LEFT JOIN new_payg_installs npi ON ar.nas_id = npi.nas_id
    WHERE ar.migration_rn = 1
      AND ar.speed_limit_mbps IN (50, 100)
      AND ar.has_mandate = 'No'
      AND COALESCE(ar.is_picked_up, 0) = 0
      AND ar.mobile > '5999999999'
      AND (ar.plan_expiry_time >= ar.migration_ts_ist
           OR DATEDIFF('day', TO_DATE(ar.plan_expiry_time), TO_DATE(ar.migration_ts_ist)) BETWEEN 0 AND 30)
      AND npi.nas_id IS NULL
),

all_recharges AS (
    SELECT
        trum.router_nas_id AS nas_id,
        DATEADD('minute', 330, trum.otp_issued_time) AS recharge_time_ist,
        tpc.combined_setting_id AS plan_cs_id,
        tpc.time_limit / 86400 AS plan_days,
        tpc.price AS plan_price
    FROM t_router_user_mapping trum
    JOIN t_plan_configuration tpc ON tpc.id = trum.selected_plan_id
    WHERE trum.otp = 'DONE' AND trum.store_group_id = 0 AND trum.device_limit > 1
      AND trum.mobile > '5999999999'
      AND DATEADD('minute', 330, trum.otp_issued_time) >= '2026-02-11'
),

first_any AS (
    SELECT nas_id, recharge_time_ist, r_day FROM (
        SELECT qc.nas_id, ar.recharge_time_ist,
            DATEDIFF('day', qc.due_date, TO_DATE(ar.recharge_time_ist)) AS r_day,
            ROW_NUMBER() OVER (PARTITION BY qc.nas_id ORDER BY ar.recharge_time_ist) AS rn
        FROM qualified_customers qc
        JOIN all_recharges ar ON qc.nas_id = ar.nas_id AND TO_DATE(ar.recharge_time_ist) >= qc.due_date
    ) WHERE rn = 1
),

first_payg AS (
    SELECT nas_id, migrated_time, migrated_r_day, migrated_plan_days, migrated_plan_price FROM (
        SELECT qc.nas_id, ar.recharge_time_ist AS migrated_time,
            DATEDIFF('day', qc.due_date, TO_DATE(ar.recharge_time_ist)) AS migrated_r_day,
            ar.plan_days AS migrated_plan_days,
            ar.plan_price AS migrated_plan_price,
            ROW_NUMBER() OVER (PARTITION BY qc.nas_id ORDER BY ar.recharge_time_ist) AS rn
        FROM qualified_customers qc
        JOIN all_recharges ar ON qc.nas_id = ar.nas_id
            AND TO_DATE(ar.recharge_time_ist) >= qc.due_date
            AND ar.plan_cs_id = 22
    ) WHERE rn = 1
),

nas_settings AS (
    SELECT nas_id, combined_setting_id AS nas_cs_id
    FROM PROD_DB.MASTER_DB_DBO.T_COMBINED_SETTING_NAS_MAPPING
    WHERE _fivetran_active = true
),

education_events AS (
    SELECT
        TRY_TO_NUMBER(NULLIF(nasid_long, '')) AS nas_id,
        MIN(TO_DATE(timestamp)) AS education_date
    FROM prod_db.public.ct_customer_payg_migration_events_mv
    WHERE event_name = 'migration_50mbps_education_complete'
      AND TRY_TO_NUMBER(NULLIF(nasid_long, '')) IS NOT NULL
    GROUP BY 1
),

supply AS (
    SELECT partner_account_id, city FROM prod_db.public.supply_model
)

SELECT
    qc.nas_id,
    qc.speed_limit_mbps,
    qc.migration_status,
    TO_CHAR(qc.due_date, 'YYYY-MM-DD') AS due_date,
    CASE WHEN qc.due_date <= CURRENT_DATE THEN 1 ELSE 0 END AS is_due,
    COALESCE(s.city, 'Unknown') AS city,
    CASE WHEN fa.nas_id IS NOT NULL THEN 1 ELSE 0 END AS has_recharged,
    CASE WHEN fp.nas_id IS NOT NULL THEN 1 ELSE 0 END AS has_migrated,
    fa.r_day AS first_recharge_r_day,
    fp.migrated_r_day,
    fp.migrated_plan_days,
    fp.migrated_plan_price,
    COALESCE(ns.nas_cs_id, -1) AS nas_setting_id,
    CASE WHEN ee.nas_id IS NOT NULL THEN 1 ELSE 0 END AS education_completed,
    TO_CHAR(ee.education_date, 'YYYY-MM-DD') AS education_date
FROM qualified_customers qc
LEFT JOIN first_any fa ON qc.nas_id = fa.nas_id
LEFT JOIN first_payg fp ON qc.nas_id = fp.nas_id
LEFT JOIN nas_settings ns ON qc.nas_id = ns.nas_id
LEFT JOIN education_events ee ON qc.nas_id = ee.nas_id
LEFT JOIN supply s ON qc.partner_account_id = s.partner_account_id
ORDER BY qc.due_date, qc.nas_id
"""

print("Fetching PAYG Migration data from Metabase...")
payload = json.dumps({
    'database': DB_ID, 'type': 'native', 'native': {'query': SQL_QUERY},
    'constraints': {'max-results': 200000, 'max-results-bare-rows': 200000}
}).encode()
req = urllib.request.Request(METABASE_URL, data=payload, headers={
    'x-api-key': api_key, 'Content-Type': 'application/json'
})
resp = urllib.request.urlopen(req, context=ctx, timeout=300)
data = json.loads(resp.read())
rows = data['data']['rows']
cols = [c['name'] for c in data['data']['cols']]
print(f"Fetched {len(rows)} rows, columns: {cols}")

# Parse into list of dicts (lowercase keys for consistency)
records = []
for r in rows:
    rec = {}
    for i, c in enumerate(cols):
        rec[c.lower()] = r[i]
    records.append(rec)

# Safe percentage helper
def pct(part, whole):
    return round(part * 100 / whole, 1) if whole else 0.0

# ── 1. Overall KPIs ──
total_eligible = len(records)
total_due = sum(1 for r in records if r['is_due'] == 1)
total_recharged = sum(1 for r in records if r['has_recharged'] == 1)
total_migrated = sum(1 for r in records if r['has_migrated'] == 1)
total_non_migrated = total_due - total_migrated
migration_rate = pct(total_migrated, total_due)

# ── 2. Speed tier breakdown ──
def tier_kpis(speed):
    subset = [r for r in records if r['speed_limit_mbps'] == speed]
    eligible = len(subset)
    due = sum(1 for r in subset if r['is_due'] == 1)
    recharged = sum(1 for r in subset if r['has_recharged'] == 1)
    migrated = sum(1 for r in subset if r['has_migrated'] == 1)
    return {
        'eligible': eligible, 'due': due, 'recharged': recharged,
        'migrated': migrated, 'non_migrated': due - migrated,
        'recharge_rate': pct(recharged, due), 'migration_rate': pct(migrated, due)
    }

tier_50 = tier_kpis(50)
tier_100 = tier_kpis(100)

# ── 3. R-day migration curve ──
R_DAYS = [0, 1, 2, 3, 5]

def rday_curve(subset_records):
    due_records = [r for r in subset_records if r['is_due'] == 1]
    due_count = len(due_records)
    results = []
    for rd in R_DAYS:
        cum_recharged = sum(1 for r in due_records if r['has_recharged'] == 1 and r['first_recharge_r_day'] is not None and r['first_recharge_r_day'] <= rd)
        cum_migrated = sum(1 for r in due_records if r['has_migrated'] == 1 and r['migrated_r_day'] is not None and r['migrated_r_day'] <= rd)
        results.append({
            'r_day': f'R{rd}',
            'recharged_pct': pct(cum_recharged, due_count),
            'migrated_pct': pct(cum_migrated, due_count)
        })
    # Total row
    results.append({
        'r_day': 'Total',
        'recharged_pct': pct(sum(1 for r in due_records if r['has_recharged'] == 1), due_count),
        'migrated_pct': pct(sum(1 for r in due_records if r['has_migrated'] == 1), due_count)
    })
    return results

rday_all = rday_curve(records)
rday_50 = rday_curve([r for r in records if r['speed_limit_mbps'] == 50])
rday_100 = rday_curve([r for r in records if r['speed_limit_mbps'] == 100])

# ── 4. Plan distribution ──
plan_dist = {}
for r in records:
    if r['has_migrated'] == 1 and r['migrated_plan_days'] is not None:
        days = int(r['migrated_plan_days'])
        plan_dist[days] = plan_dist.get(days, 0) + 1

plan_labels_order = [1, 2, 7, 14, 28]
plan_labels = []
plan_counts = []
for d in plan_labels_order:
    if d in plan_dist:
        plan_labels.append(f'{d}d')
        plan_counts.append(plan_dist[d])
# Add any remaining plan days not in the standard list
for d in sorted(plan_dist.keys()):
    if d not in plan_labels_order:
        plan_labels.append(f'{d}d')
        plan_counts.append(plan_dist[d])

# ── 5. City breakdown ──
def city_bucket(city_name):
    if city_name is None:
        return 'Bharat'
    c = str(city_name).strip().lower()
    if 'delhi' in c or 'new delhi' in c or 'noida' in c or 'gurgaon' in c or 'gurugram' in c or 'faridabad' in c or 'ghaziabad' in c:
        return 'Delhi'
    if 'mumbai' in c or 'thane' in c or 'navi mumbai' in c:
        return 'Mumbai'
    return 'Bharat'

city_data = {}
for r in records:
    bucket = city_bucket(r['city'])
    if bucket not in city_data:
        city_data[bucket] = {'eligible': 0, 'due': 0, 'recharged': 0, 'migrated': 0}
    city_data[bucket]['eligible'] += 1
    if r['is_due'] == 1:
        city_data[bucket]['due'] += 1
    if r['has_recharged'] == 1:
        city_data[bucket]['recharged'] += 1
    if r['has_migrated'] == 1:
        city_data[bucket]['migrated'] += 1

city_order = ['Delhi', 'Mumbai', 'Bharat']
city_rows_html = ""
for c in city_order:
    if c in city_data:
        cd = city_data[c]
        city_rows_html += f"<tr><td style='text-align:left;font-weight:600;'>{c}</td><td>{cd['eligible']}</td><td>{cd['due']}</td><td>{cd['recharged']}</td><td>{cd['migrated']}</td><td>{pct(cd['migrated'], cd['due'])}%</td></tr>\n"

# ── 6. Tech hygiene — NAS Setting (100 Mbps only) ──
nas_100 = [r for r in records if r['speed_limit_mbps'] == 100]
nas_total = len(nas_100)
nas_correct = sum(1 for r in nas_100 if r['nas_setting_id'] == 22)
nas_bugs = nas_total - nas_correct

# ── 6b. Tech hygiene — Education (50 Mbps, due only) ──
edu_50 = [r for r in records if r['speed_limit_mbps'] == 50 and r['is_due'] == 1]
edu_total = len(edu_50)
edu_completed = sum(1 for r in edu_50 if r['education_completed'] == 1)
edu_pct = pct(edu_completed, edu_total)

# ── 6c. Education Funnel — 50 Mbps due customers breakdown ──
edu_done = [r for r in edu_50 if r['education_completed'] == 1]
edu_not_done = [r for r in edu_50 if r['education_completed'] != 1]

edu_done_payg = sum(1 for r in edu_done if r['has_migrated'] == 1)
edu_done_non_payg = sum(1 for r in edu_done if r['has_recharged'] == 1 and r['has_migrated'] != 1)
edu_done_no_recharge = len(edu_done) - edu_done_payg - edu_done_non_payg

edu_not_done_payg = sum(1 for r in edu_not_done if r['has_migrated'] == 1)
edu_not_done_non_payg = sum(1 for r in edu_not_done if r['has_recharged'] == 1 and r['has_migrated'] != 1)
edu_not_done_no_recharge = len(edu_not_done) - edu_not_done_payg - edu_not_done_non_payg

# Education funnel chart data
edu_funnel_labels = ['PAYG Recharge', 'Non-PAYG Recharge', 'No Recharge']
edu_funnel_done_vals = [edu_done_payg, edu_done_non_payg, edu_done_no_recharge]
edu_funnel_not_done_vals = [edu_not_done_payg, edu_not_done_non_payg, edu_not_done_no_recharge]

# ── 6d. Day-wise Education Funnel Table (like Metabase format) ──
now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
today = now_ist.date()

# Get last 7 days
last_7 = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7, 0, -1)]
# Week boundaries
week_start = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d')  # Monday

# Group edu-completed 50 Mbps due customers by education_date
def edu_day_metrics(customers_list, date_filter=None):
    """Compute funnel metrics for a filtered set of edu-completed customers."""
    if date_filter:
        subset = [r for r in customers_list if r.get('education_date') and r['education_date'][:10] == date_filter]
    else:
        subset = customers_list  # all
    edu_count = len(subset)
    payg = sum(1 for r in subset if r['has_migrated'] == 1)
    non_payg = sum(1 for r in subset if r['has_recharged'] == 1 and r['has_migrated'] != 1)
    no_rech = edu_count - payg - non_payg
    return {
        'edu_count': edu_count,
        'payg': payg,
        'payg_pct': pct(payg, edu_count),               # % of edu-complete who selected PayG
        'non_payg': non_payg,
        'non_payg_pct': pct(non_payg, edu_count),        # % of edu-complete who selected NonPayG
        'no_recharge': no_rech,
        'no_recharge_pct': pct(no_rech, edu_count),      # % of edu-complete with no recharge
        'payg_payment_pct': pct(payg, payg) if payg else 0.0,           # of PayG selected, % payment done (100% with recharge data)
        'non_payg_payment_pct': pct(non_payg, non_payg) if non_payg else 0.0,  # same
    }

# WTD = education_date >= week_start
edu_wtd_list = [r for r in edu_done if r.get('education_date') and r['education_date'][:10] >= week_start]
edu_wtd = edu_day_metrics(edu_wtd_list)

# Overall (all edu-completed due 50 Mbps)
edu_overall = edu_day_metrics(edu_done)

# Per-day for last 7 days
edu_days = {}
for d in last_7:
    edu_days[d] = edu_day_metrics(edu_done, d)

# Build day-wise table columns: Overall | WTD | D1..D7
edu_table_cols = [('Overall', edu_overall), ('WTD', edu_wtd)]
for d in last_7:
    short_label = datetime.strptime(d, '%Y-%m-%d').strftime('%b %d')
    edu_table_cols.append((short_label, edu_days[d]))

# Build HTML rows for the funnel table (matching Metabase screenshot format)
def edu_funnel_table_html():
    header = '<tr><th style="text-align:left;min-width:220px;">EDUCATION COMPLETE</th>'
    for label, _ in edu_table_cols:
        header += f'<th>{label}</th>'
    header += '</tr>'

    def val_row(label, key, style=''):
        r = f'<tr><td style="text-align:left;{style}">{label}</td>'
        for _, m in edu_table_cols:
            r += f'<td>{m[key]}</td>'
        r += '</tr>'
        return r

    def pct_val_row(label, key, style=''):
        r = f'<tr><td style="text-align:left;{style}">{label}</td>'
        for _, m in edu_table_cols:
            r += f'<td>{m[key]}</td>'
        r += '</tr>'
        return r

    rows = ''
    # Row 0: Education Complete (count)
    rows += val_row('0. Education Complete', 'edu_count', 'font-weight:700;')
    # Row 1a: Education Complete (%) = 100%
    rows += '<tr><td style="text-align:left;padding-left:16px;">1a. Education Complete (%)</td>'
    for _, m in edu_table_cols:
        rows += f'<td>100</td>' if m['edu_count'] > 0 else '<td>0</td>'
    rows += '</tr>'
    # Row 1b: PayG Selected % (of edu-complete)
    rows += pct_val_row('1b. PayG Selected %', 'payg_pct', 'color:#22d3ee;padding-left:16px;')
    # Row 1c: NonPayG Selected % (of edu-complete)
    rows += pct_val_row('1c. NonPayG Selected %', 'non_payg_pct', 'color:#fb923c;padding-left:16px;')
    # Row 1d: No Recharge % (of edu-complete)
    rows += pct_val_row('1d. No Recharge %', 'no_recharge_pct', 'color:#f87171;padding-left:16px;')
    # Row 2a: PayG Selected (%) = 100% base
    rows += '<tr><td style="text-align:left;color:#22d3ee;font-weight:600;">2a. PayG Selected (%)</td>'
    for _, m in edu_table_cols:
        rows += f'<td>100</td>' if m['payg'] > 0 else '<td>0</td>'
    rows += '</tr>'
    # Row 2b: PayG Count
    rows += val_row('2b. PayG Recharge Count', 'payg', 'color:#22d3ee;padding-left:16px;')
    # Row 3a: NonPayG Selected (%) = 100% base
    rows += '<tr><td style="text-align:left;color:#fb923c;font-weight:600;">3a. NonPayG Selected (%)</td>'
    for _, m in edu_table_cols:
        rows += f'<td>100</td>' if m['non_payg'] > 0 else '<td>0</td>'
    rows += '</tr>'
    # Row 3b: NonPayG Count
    rows += val_row('3b. NonPayG Recharge Count', 'non_payg', 'color:#fb923c;padding-left:16px;')

    return header, rows

edu_table_header, edu_table_rows = edu_funnel_table_html()

# ── 7. Daily cohort trend ──
daily = {}
for r in records:
    dd = r['due_date'][:10] if r['due_date'] else 'Unknown'
    if dd not in daily:
        daily[dd] = {'due': 0, 'recharged': 0, 'migrated': 0, 'total': 0}
    daily[dd]['total'] += 1
    if r['is_due'] == 1:
        daily[dd]['due'] += 1
    if r['has_recharged'] == 1:
        daily[dd]['recharged'] += 1
    if r['has_migrated'] == 1:
        daily[dd]['migrated'] += 1

daily_dates_sorted = sorted(daily.keys())
daily_dates = daily_dates_sorted
daily_due = [daily[d]['due'] for d in daily_dates]
daily_recharged = [daily[d]['recharged'] for d in daily_dates]
daily_migrated = [daily[d]['migrated'] for d in daily_dates]
daily_migration_rate = [pct(daily[d]['migrated'], daily[d]['due']) for d in daily_dates]

# ── 8. Data table (date + speed breakdown) ──
table_key = {}
for r in records:
    dd = r['due_date'][:10] if r['due_date'] else 'Unknown'
    speed = r['speed_limit_mbps']
    key = (dd, speed)
    if key not in table_key:
        table_key[key] = {'total': 0, 'due': 0, 'recharged': 0, 'migrated': 0}
    table_key[key]['total'] += 1
    if r['is_due'] == 1:
        table_key[key]['due'] += 1
    if r['has_recharged'] == 1:
        table_key[key]['recharged'] += 1
    if r['has_migrated'] == 1:
        table_key[key]['migrated'] += 1

# Sort by date descending for table
table_rows_sorted = sorted(table_key.keys(), key=lambda x: x[0], reverse=True)

# R-day data for chart
rday_labels = [r['r_day'] for r in rday_all]
rday_all_rech = [r['recharged_pct'] for r in rday_all]
rday_all_mig = [r['migrated_pct'] for r in rday_all]
rday_50_rech = [r['recharged_pct'] for r in rday_50]
rday_50_mig = [r['migrated_pct'] for r in rday_50]
rday_100_rech = [r['recharged_pct'] for r in rday_100]
rday_100_mig = [r['migrated_pct'] for r in rday_100]

data_freshness = daily_dates_sorted[-1] if daily_dates_sorted else 'N/A'
generated_at = datetime.now().strftime('%Y-%m-%d %H:%M')

print(f"Total eligible: {total_eligible}, Due: {total_due}, Migrated: {total_migrated}, Rate: {migration_rate}%")

# ── Build HTML ──
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PAYG Migration Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 20px; }}
  h1 {{ text-align: center; font-size: 1.6rem; margin-bottom: 6px; color: #f1f5f9; }}
  .subtitle {{ text-align: center; color: #94a3b8; font-size: 0.85rem; margin-bottom: 20px; }}
  .section-title {{ font-size: 1.1rem; color: #f1f5f9; margin: 20px 0 12px 0; padding-left: 4px; border-left: 3px solid #38bdf8; padding-left: 10px; }}
  .kpi-row {{ display: flex; gap: 14px; justify-content: center; flex-wrap: wrap; margin-bottom: 24px; }}
  .kpi {{ background: #1e293b; border-radius: 12px; padding: 16px 24px; min-width: 155px; text-align: center; border: 1px solid #334155; }}
  .kpi .val {{ font-size: 1.8rem; font-weight: 700; color: #38bdf8; }}
  .kpi .lbl {{ font-size: 0.75rem; color: #94a3b8; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .kpi.green .val {{ color: #4ade80; }}
  .kpi.red .val {{ color: #f87171; }}
  .kpi.amber .val {{ color: #fbbf24; }}
  .kpi.purple .val {{ color: #a78bfa; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }}
  .chart-box {{ background: #1e293b; border-radius: 12px; padding: 16px; border: 1px solid #334155; min-height: 420px; }}
  .chart-full {{ grid-column: 1 / -1; min-height: 450px; }}
  .table-box {{ background: #1e293b; border-radius: 12px; padding: 16px; border: 1px solid #334155; overflow-x: auto; margin-bottom: 20px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  th {{ background: #334155; color: #e2e8f0; padding: 8px 10px; text-align: center; position: sticky; top: 0; }}
  td {{ padding: 6px 10px; text-align: center; border-bottom: 1px solid #334155; }}
  tr:hover td {{ background: #334155; }}
  .tier-container {{ display: flex; gap: 20px; margin-bottom: 24px; flex-wrap: wrap; }}
  .tier-group {{ flex: 1; min-width: 300px; background: #1e293b; border-radius: 12px; padding: 16px; border: 1px solid #334155; }}
  .tier-group h3 {{ text-align: center; color: #f1f5f9; margin-bottom: 12px; font-size: 1rem; }}
  .tier-kpis {{ display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; }}
  .tier-kpi {{ text-align: center; min-width: 90px; padding: 8px 12px; background: #0f172a; border-radius: 8px; }}
  .tier-kpi .tval {{ font-size: 1.3rem; font-weight: 700; color: #38bdf8; }}
  .tier-kpi .tlbl {{ font-size: 0.65rem; color: #94a3b8; margin-top: 2px; text-transform: uppercase; }}
  .hygiene-row {{ display: flex; gap: 20px; margin-bottom: 24px; flex-wrap: wrap; }}
  .hygiene-card {{ flex: 1; min-width: 280px; background: #1e293b; border-radius: 12px; padding: 16px; border: 1px solid #334155; }}
  .hygiene-card h3 {{ text-align: center; color: #f1f5f9; margin-bottom: 12px; font-size: 0.95rem; }}
  .hygiene-kpis {{ display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; }}
  .hygiene-kpi {{ text-align: center; min-width: 80px; padding: 8px 12px; background: #0f172a; border-radius: 8px; }}
  .hygiene-kpi .hval {{ font-size: 1.2rem; font-weight: 700; }}
  .hygiene-kpi .hlbl {{ font-size: 0.65rem; color: #94a3b8; margin-top: 2px; text-transform: uppercase; }}
  .footer {{ text-align: center; color: #64748b; font-size: 0.7rem; margin-top: 16px; }}
  @media (max-width: 768px) {{ .chart-grid {{ grid-template-columns: 1fr; }} .tier-container {{ flex-direction: column; }} .hygiene-row {{ flex-direction: column; }} }}
</style>
</head>
<body>

<h1>PAYG Migration Dashboard</h1>
<p class="subtitle">nPayG &rarr; PayG migration tracking &mdash; Data as of {data_freshness} &mdash; Generated {generated_at}</p>

<!-- ── Overall KPI Cards ── -->
<div class="kpi-row">
  <div class="kpi"><div class="val">{total_eligible}</div><div class="lbl">Total Eligible</div></div>
  <div class="kpi"><div class="val">{total_due}</div><div class="lbl">Due</div></div>
  <div class="kpi green"><div class="val">{total_recharged}</div><div class="lbl">Recharged ({pct(total_recharged, total_due)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #22d3ee;"><div class="val" style="color:#22d3ee;">{total_migrated}</div><div class="lbl">Migrated ({pct(total_migrated, total_due)}%)</div></div>
  <div class="kpi red"><div class="val">{total_non_migrated}</div><div class="lbl">Non-Migrated</div></div>
  <div class="kpi amber"><div class="val">{migration_rate}%</div><div class="lbl">Migration Rate</div></div>
</div>

<!-- ── Speed Tier Cards ── -->
<h2 class="section-title">Speed Tier Breakdown</h2>
<div class="tier-container">
  <div class="tier-group" style="border-top: 3px solid #a78bfa;">
    <h3 style="color:#a78bfa;">50 Mbps</h3>
    <div class="tier-kpis">
      <div class="tier-kpi"><div class="tval">{tier_50['eligible']}</div><div class="tlbl">Eligible</div></div>
      <div class="tier-kpi"><div class="tval">{tier_50['due']}</div><div class="tlbl">Due</div></div>
      <div class="tier-kpi"><div class="tval" style="color:#4ade80;">{tier_50['recharged']}</div><div class="tlbl">Recharged ({tier_50['recharge_rate']}%)</div></div>
      <div class="tier-kpi"><div class="tval" style="color:#22d3ee;">{tier_50['migrated']}</div><div class="tlbl">Migrated ({tier_50['migration_rate']}%)</div></div>
      <div class="tier-kpi"><div class="tval" style="color:#fbbf24;">{tier_50['migration_rate']}%</div><div class="tlbl">Migration Rate</div></div>
    </div>
  </div>
  <div class="tier-group" style="border-top: 3px solid #fb923c;">
    <h3 style="color:#fb923c;">100 Mbps</h3>
    <div class="tier-kpis">
      <div class="tier-kpi"><div class="tval">{tier_100['eligible']}</div><div class="tlbl">Eligible</div></div>
      <div class="tier-kpi"><div class="tval">{tier_100['due']}</div><div class="tlbl">Due</div></div>
      <div class="tier-kpi"><div class="tval" style="color:#4ade80;">{tier_100['recharged']}</div><div class="tlbl">Recharged ({tier_100['recharge_rate']}%)</div></div>
      <div class="tier-kpi"><div class="tval" style="color:#22d3ee;">{tier_100['migrated']}</div><div class="tlbl">Migrated ({tier_100['migration_rate']}%)</div></div>
      <div class="tier-kpi"><div class="tval" style="color:#fbbf24;">{tier_100['migration_rate']}%</div><div class="tlbl">Migration Rate</div></div>
    </div>
  </div>
</div>

<!-- ── R-Day Migration Curve (full width chart) ── -->
<div class="chart-grid">
  <div class="chart-box chart-full" id="rdayChart"></div>
</div>

<!-- ── Plan Distribution + City Breakdown (side by side) ── -->
<h2 class="section-title">Plan Distribution &amp; City Breakdown</h2>
<div class="chart-grid">
  <div class="chart-box" id="planChart"></div>
  <div class="table-box" style="min-height:420px;">
    <h3 style="margin-bottom:10px; color:#f1f5f9; font-size:1rem;">City-Level KPIs</h3>
    <table>
      <thead><tr><th style="text-align:left;">City</th><th>Eligible</th><th>Due</th><th>Recharged</th><th>Migrated</th><th>Migration %</th></tr></thead>
      <tbody>
{city_rows_html}      </tbody>
    </table>
  </div>
</div>

<!-- ── Tech Hygiene Section ── -->
<h2 class="section-title">Tech Hygiene</h2>
<div class="hygiene-row">
  <div class="hygiene-card" style="border-top: 3px solid #fb923c;">
    <h3>NAS Setting (100 Mbps)</h3>
    <div class="hygiene-kpis">
      <div class="hygiene-kpi"><div class="hval" style="color:#38bdf8;">{nas_total}</div><div class="hlbl">Total 100 Mbps</div></div>
      <div class="hygiene-kpi"><div class="hval" style="color:#4ade80;">{nas_correct}</div><div class="hlbl">Correctly Set (=22)</div></div>
      <div class="hygiene-kpi"><div class="hval" style="color:#f87171;">{nas_bugs}</div><div class="hlbl">Bugs (&ne;22) ({pct(nas_bugs, nas_total)}%)</div></div>
    </div>
  </div>
  <div class="hygiene-card" style="border-top: 3px solid #a78bfa;">
    <h3>Education (50 Mbps, Due Only)</h3>
    <div class="hygiene-kpis">
      <div class="hygiene-kpi"><div class="hval" style="color:#38bdf8;">{edu_total}</div><div class="hlbl">Total Due</div></div>
      <div class="hygiene-kpi"><div class="hval" style="color:#4ade80;">{edu_completed}</div><div class="hlbl">Completed</div></div>
      <div class="hygiene-kpi"><div class="hval" style="color:#fbbf24;">{edu_pct}%</div><div class="hlbl">Completion %</div></div>
    </div>
  </div>
</div>

<!-- ── Education Funnel (50 Mbps Due) ── -->
<h2 class="section-title">Education &amp; Recharge Funnel (50 Mbps Due Customers)</h2>
<div class="kpi-row" style="margin-bottom:12px;">
  <div class="kpi"><div class="val">{edu_total}</div><div class="lbl">Total 50 Mbps Due</div></div>
  <div class="kpi" style="border-left:3px solid #4ade80;"><div class="val" style="color:#4ade80;">{edu_completed}</div><div class="lbl">Education Completed ({pct(edu_completed, edu_total)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #f87171;"><div class="val" style="color:#f87171;">{edu_total - edu_completed}</div><div class="lbl">Education Not Completed ({pct(edu_total - edu_completed, edu_total)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #22d3ee;"><div class="val" style="color:#22d3ee;">{edu_done_payg + edu_not_done_payg}</div><div class="lbl">PAYG Selected ({pct(edu_done_payg + edu_not_done_payg, edu_total)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #fb923c;"><div class="val" style="color:#fb923c;">{edu_done_non_payg + edu_not_done_non_payg}</div><div class="lbl">Non-PAYG Selected ({pct(edu_done_non_payg + edu_not_done_non_payg, edu_total)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #94a3b8;"><div class="val" style="color:#94a3b8;">{edu_done_no_recharge + edu_not_done_no_recharge}</div><div class="lbl">No Recharge ({pct(edu_done_no_recharge + edu_not_done_no_recharge, edu_total)}%)</div></div>
</div>
<div class="chart-grid">
  <div class="chart-box" id="eduFunnelChart"></div>
  <div class="table-box" style="min-height:420px;">
    <h3 style="margin-bottom:10px; color:#f1f5f9; font-size:1rem;">Education &rarr; Recharge Breakdown</h3>
    <table>
      <thead><tr><th style="text-align:left;">Education Status</th><th>Total</th><th style="color:#22d3ee;">PAYG Recharge</th><th style="color:#fb923c;">Non-PAYG Recharge</th><th style="color:#f87171;">No Recharge</th></tr></thead>
      <tbody>
        <tr>
          <td style="text-align:left;font-weight:600;color:#4ade80;">Completed</td>
          <td>{len(edu_done)}</td>
          <td style="color:#22d3ee;">{edu_done_payg} ({pct(edu_done_payg, len(edu_done))}%)</td>
          <td style="color:#fb923c;">{edu_done_non_payg} ({pct(edu_done_non_payg, len(edu_done))}%)</td>
          <td style="color:#f87171;">{edu_done_no_recharge} ({pct(edu_done_no_recharge, len(edu_done))}%)</td>
        </tr>
        <tr>
          <td style="text-align:left;font-weight:600;color:#f87171;">Not Completed</td>
          <td>{len(edu_not_done)}</td>
          <td style="color:#22d3ee;">{edu_not_done_payg} ({pct(edu_not_done_payg, len(edu_not_done))}%)</td>
          <td style="color:#fb923c;">{edu_not_done_non_payg} ({pct(edu_not_done_non_payg, len(edu_not_done))}%)</td>
          <td style="color:#f87171;">{edu_not_done_no_recharge} ({pct(edu_not_done_no_recharge, len(edu_not_done))}%)</td>
        </tr>
        <tr style="border-top:2px solid #94a3b8;">
          <td style="text-align:left;font-weight:700;">Total (50 Mbps Due)</td>
          <td style="font-weight:700;">{edu_total}</td>
          <td style="font-weight:700;color:#22d3ee;">{edu_done_payg + edu_not_done_payg}</td>
          <td style="font-weight:700;color:#fb923c;">{edu_done_non_payg + edu_not_done_non_payg}</td>
          <td style="font-weight:700;color:#f87171;">{edu_done_no_recharge + edu_not_done_no_recharge}</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

<!-- ── Day-wise Education Funnel Table ── -->
<h2 class="section-title">Day-wise Education Funnel (50 Mbps — by Education Completion Date)</h2>
<div class="table-box">
  <div style="overflow-x:auto;">
  <table style="font-size:0.78rem;">
    <thead>{edu_table_header}</thead>
    <tbody>{edu_table_rows}</tbody>
  </table>
  </div>
</div>

<!-- ── Daily Cohort Trend (full width chart) ── -->
<div class="chart-grid">
  <div class="chart-box chart-full" id="dailyChart"></div>
</div>

<!-- ── Data Table ── -->
<div class="table-box">
  <h3 style="margin-bottom:10px; color:#f1f5f9; font-size:1rem;">Day-wise Data Table</h3>
  <div style="max-height:400px; overflow-y:auto;">
  <table>
    <thead><tr><th>Due Date</th><th>Speed</th><th>Total</th><th>Due</th><th>Recharged</th><th>Migrated</th><th>Migration %</th></tr></thead>
    <tbody>
"""

for key in table_rows_sorted:
    dd, speed = key
    td = table_key[key]
    mig_pct = pct(td['migrated'], td['due'])
    html += f"<tr><td>{dd}</td><td>{speed} Mbps</td><td>{td['total']}</td><td>{td['due']}</td><td>{td['recharged']}</td><td>{td['migrated']}</td><td>{mig_pct}%</td></tr>\n"

html += """
    </tbody>
  </table>
  </div>
</div>

<p class="footer">Source: payg_migration + payg_migration_audit + T_ROUTER_USER_MAPPING + T_PLAN_CONFIGURATION (combined_setting_id=22) + T_COMBINED_SETTING_NAS_MAPPING + ct_customer_payg_migration_events_mv + supply_model</p>

<script>
const darkLayout = {
  paper_bgcolor: '#1e293b', plot_bgcolor: '#1e293b',
  font: { color: '#e2e8f0', size: 11 },
  margin: { l: 50, r: 30, t: 45, b: 80 },
  xaxis: { gridcolor: '#334155', tickangle: -45, tickfont: { size: 9 } },
  yaxis: { gridcolor: '#334155' },
  legend: { orientation: 'h', y: -0.35, x: 0.5, xanchor: 'center', font: { size: 10 } },
  height: 380
};
const cfg = { responsive: true, displayModeBar: false };
"""

html += f"""
// ── R-Day Migration Curve ──
const rdayLabels = {json.dumps(rday_labels)};
const rdayAllRech = {json.dumps(rday_all_rech)};
const rdayAllMig = {json.dumps(rday_all_mig)};
const rday50Rech = {json.dumps(rday_50_rech)};
const rday50Mig = {json.dumps(rday_50_mig)};
const rday100Rech = {json.dumps(rday_100_rech)};
const rday100Mig = {json.dumps(rday_100_mig)};

Plotly.newPlot('rdayChart', [
  {{ x: rdayLabels, y: rdayAllRech, type: 'scatter', mode: 'lines+markers', name: 'All Recharged %',
     line: {{ color: '#4ade80', width: 3 }}, marker: {{ size: 7 }} }},
  {{ x: rdayLabels, y: rdayAllMig, type: 'scatter', mode: 'lines+markers', name: 'All Migrated %',
     line: {{ color: '#22d3ee', width: 3 }}, marker: {{ size: 7 }} }},
  {{ x: rdayLabels, y: rday50Rech, type: 'scatter', mode: 'lines+markers', name: '50 Recharged %',
     line: {{ color: '#a78bfa', width: 2, dash: 'dash' }}, marker: {{ size: 5 }} }},
  {{ x: rdayLabels, y: rday50Mig, type: 'scatter', mode: 'lines+markers', name: '50 Migrated %',
     line: {{ color: '#e879f9', width: 2, dash: 'dash' }}, marker: {{ size: 5 }} }},
  {{ x: rdayLabels, y: rday100Rech, type: 'scatter', mode: 'lines+markers', name: '100 Recharged %',
     line: {{ color: '#fb923c', width: 2, dash: 'dot' }}, marker: {{ size: 5 }} }},
  {{ x: rdayLabels, y: rday100Mig, type: 'scatter', mode: 'lines+markers', name: '100 Migrated %',
     line: {{ color: '#f87171', width: 2, dash: 'dot' }}, marker: {{ size: 5 }} }}
], {{
  ...darkLayout,
  title: {{ text: 'R-Day Migration Curve (Cumulative % of Due Customers)', font: {{ size: 14 }} }},
  yaxis: {{ ...darkLayout.yaxis, title: 'Cumulative %', range: [0, 105] }},
  xaxis: {{ ...darkLayout.xaxis, tickangle: 0, title: 'R-Day' }},
  height: 420
}}, cfg);

// ── Plan Distribution ──
const planLabels = {json.dumps(plan_labels)};
const planCounts = {json.dumps(plan_counts)};

Plotly.newPlot('planChart', [
  {{ x: planLabels, y: planCounts, type: 'bar', name: 'Migrated Customers',
     marker: {{ color: ['#38bdf8', '#22d3ee', '#4ade80', '#a78bfa', '#fbbf24', '#fb923c', '#f87171', '#e879f9'].slice(0, planLabels.length) }},
     text: planCounts.map(String), textposition: 'auto', textfont: {{ size: 11, color: '#fff' }} }}
], {{
  ...darkLayout,
  title: {{ text: 'Plan Distribution (Migrated Customers)', font: {{ size: 13 }} }},
  yaxis: {{ ...darkLayout.yaxis, title: 'Count' }},
  xaxis: {{ ...darkLayout.xaxis, tickangle: 0, title: 'Plan Duration' }}
}}, cfg);

// ── Daily Cohort Trend ──
const dailyDates = {json.dumps(daily_dates)};
const dailyDue = {json.dumps(daily_due)};
const dailyRecharged = {json.dumps(daily_recharged)};
const dailyMigrated = {json.dumps(daily_migrated)};
const dailyMigRate = {json.dumps(daily_migration_rate)};

Plotly.newPlot('dailyChart', [
  {{ x: dailyDates, y: dailyDue, type: 'bar', name: 'Due', marker: {{ color: '#38bdf8' }},
     text: dailyDue.map(String), textposition: 'auto', textfont: {{ size: 9, color: '#fff' }} }},
  {{ x: dailyDates, y: dailyRecharged, type: 'bar', name: 'Recharged', marker: {{ color: '#4ade80' }},
     text: dailyRecharged.map(String), textposition: 'auto', textfont: {{ size: 9, color: '#fff' }} }},
  {{ x: dailyDates, y: dailyMigrated, type: 'bar', name: 'Migrated', marker: {{ color: '#22d3ee' }},
     text: dailyMigrated.map(String), textposition: 'auto', textfont: {{ size: 9, color: '#fff' }} }},
  {{ x: dailyDates, y: dailyMigRate, type: 'scatter', mode: 'lines+markers', name: 'Migration Rate %',
     yaxis: 'y2', line: {{ color: '#fbbf24', width: 2 }}, marker: {{ size: 5 }},
     text: dailyMigRate.map(v => v + '%'), textposition: 'top center' }}
], {{
  ...darkLayout,
  title: {{ text: 'Daily Cohort Trend (Due Date)', font: {{ size: 14 }} }},
  yaxis: {{ ...darkLayout.yaxis, title: 'Count' }},
  yaxis2: {{ overlaying: 'y', side: 'right', gridcolor: 'transparent', title: 'Migration Rate %', titlefont: {{ color: '#fbbf24' }}, tickfont: {{ color: '#fbbf24' }}, range: [0, 105] }},
  xaxis: {{ ...darkLayout.xaxis, tickangle: -45 }},
  barmode: 'group',
  height: 450
}}, cfg);

// ── Education Funnel Chart (50 Mbps) ──
const eduLabels = {json.dumps(edu_funnel_labels)};
const eduDoneVals = {json.dumps(edu_funnel_done_vals)};
const eduNotDoneVals = {json.dumps(edu_funnel_not_done_vals)};

Plotly.newPlot('eduFunnelChart', [
  {{ x: eduLabels, y: eduDoneVals, type: 'bar', name: 'Education Completed',
     marker: {{ color: '#4ade80' }},
     text: eduDoneVals.map(String), textposition: 'auto', textfont: {{ size: 11, color: '#fff' }} }},
  {{ x: eduLabels, y: eduNotDoneVals, type: 'bar', name: 'Education Not Completed',
     marker: {{ color: '#f87171' }},
     text: eduNotDoneVals.map(String), textposition: 'auto', textfont: {{ size: 11, color: '#fff' }} }}
], {{
  ...darkLayout,
  title: {{ text: 'Education vs Recharge Outcome (50 Mbps Due)', font: {{ size: 13 }} }},
  yaxis: {{ ...darkLayout.yaxis, title: 'Customers' }},
  xaxis: {{ ...darkLayout.xaxis, tickangle: 0 }},
  barmode: 'group'
}}, cfg);
"""

html += """
</script>

</body>
</html>
"""

# Output
os.makedirs('output_recharge', exist_ok=True)
out_path = os.path.join('output_recharge', 'migration.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"Migration Dashboard saved to: {out_path}")
