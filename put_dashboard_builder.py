"""PayG Router Pickup (PUT) Dashboard — Day-wise ticket data (CI version)"""
import json, urllib.request, ssl, os

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
WITH customer_type AS (
    SELECT
        mobile,
        MIN(DATEADD(MINUTE, 330, otp_issued_time)) AS install_time_ist,
        CASE WHEN CAST(MIN(DATEADD(MINUTE, 330, otp_issued_time)) AS DATE) >= '2026-01-26'
             THEN 'PayG' ELSE 'nPayG' END AS cx_type
    FROM T_ROUTER_USER_MAPPING
    WHERE otp = 'DONE'
      AND store_group_id = 0
      AND device_limit > 1
      AND mobile > '5999999999'
      AND mobile NOT IN ('6900099267','7679376747')
    GROUP BY mobile
),
last_recharge_event AS (
    SELECT * FROM (
        SELECT
            mobile,
            DATEADD(MINUTE, 330, added_time) AS recharged_time,
            PARSE_JSON(data):paymentMode::STRING AS paymentMode,
            ROW_NUMBER() OVER (PARTITION BY mobile ORDER BY added_time DESC) AS rn
        FROM customer_logs
        WHERE event_name = 'renewal_fee_captured'
    ) WHERE rn = 1
),
trump_recharge AS (
    SELECT
        DATEADD(MINUTE, 330, OTP_ISSUED_TIME) AS recharge_time,
        DATEADD(MINUTE, 330, OTP_EXPIRY_TIME) AS recharge_expire_time,
        idmaker(shard, 0, router_nas_id) AS nas_id,
        mobile
    FROM T_ROUTER_USER_MAPPING
    WHERE otp = 'DONE'
      AND store_group_id = 0
      AND device_limit > 1
      AND DATEADD(MINUTE, 330, OTP_EXPIRY_TIME) >= '2025-06-01'::TIMESTAMP_NTZ
),
base_tickets AS (
    SELECT
        t.CREATED,
        t.STATUS,
        t.MOBILE,
        CASE
            WHEN t.STATUS = 2 AND lr.paymentMode IS NOT NULL
                 AND PARSE_JSON(t.EXTRA_DATA):resolutionType::STRING IS NULL
                THEN 'customer_recharged'
            WHEN t.STATUS = 2
                 AND PARSE_JSON(t.EXTRA_DATA):resolutionType::STRING = 'CUSTOMER_RECOVERED'
                THEN 'customer_recharged'
            WHEN t.STATUS = 2
                 AND PARSE_JSON(t.EXTRA_DATA):resolutionType::STRING = 'ROUTER_RECOVERED'
                THEN 'ROUTER_RECOVERED'
            WHEN t.STATUS = 2
                 AND lr.paymentMode IS NULL
                 AND PARSE_JSON(t.EXTRA_DATA):resolutionType::STRING IS NULL
                THEN 'bug'
        END AS resolved_status_final,
        CASE WHEN DATEDIFF('minute', tr.recharge_expire_time, t.CREATED) > 20160 THEN 1 ELSE 0 END AS ticket_created_at_R15
    FROM prod_db.dynamodb_read.tasks t
    JOIN customer_type ct ON t.MOBILE = ct.mobile
    LEFT JOIN last_recharge_event lr
        ON t.MOBILE = lr.mobile
        AND t.CREATED <= lr.recharged_time
        AND t.DUE_DATE >= lr.recharged_time
    LEFT JOIN trump_recharge tr
        ON PARSE_JSON(t.EXTRA_DATA):nas_id::NUMBER = tr.nas_id
        AND t.CREATED > tr.recharge_time
    WHERE t.type = 'ROUTER_PICKUP'
      AND ct.cx_type = 'PayG'
      AND CAST(t.CREATED AS DATE) >= '2026-01-26'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.ID ORDER BY tr.recharge_time DESC) = 1
)
SELECT
    CAST(CREATED AS DATE) AS ticket_date,
    COUNT(*) AS total_tickets,
    COUNT(DISTINCT MOBILE) AS unique_customers,
    SUM(CASE WHEN STATUS = 0 THEN 1 ELSE 0 END) AS unassigned,
    SUM(CASE WHEN STATUS = 1 THEN 1 ELSE 0 END) AS assigned,
    SUM(CASE WHEN STATUS = 2 THEN 1 ELSE 0 END) AS resolved,
    SUM(CASE WHEN STATUS = 3 THEN 1 ELSE 0 END) AS unresolved,
    SUM(CASE WHEN resolved_status_final = 'customer_recharged' THEN 1 ELSE 0 END) AS resolved_recharged,
    SUM(CASE WHEN resolved_status_final = 'ROUTER_RECOVERED' THEN 1 ELSE 0 END) AS resolved_recovered,
    SUM(CASE WHEN resolved_status_final = 'bug' THEN 1 ELSE 0 END) AS resolved_bug,
    SUM(CASE WHEN ticket_created_at_R15 = 0 THEN 1 ELSE 0 END) AS self_created
FROM base_tickets
GROUP BY 1
ORDER BY 1
"""

print("Fetching PayG PUT data from Metabase...")
payload = json.dumps({
    'database': DB_ID, 'type': 'native', 'native': {'query': SQL_QUERY},
    'constraints': {'max-results': 50000, 'max-results-bare-rows': 50000}
}).encode()
req = urllib.request.Request(METABASE_URL, data=payload, headers={
    'x-api-key': api_key, 'Content-Type': 'application/json'
})
resp = urllib.request.urlopen(req, context=ctx, timeout=300)
data = json.loads(resp.read())
rows = data['data']['rows']
cols = [c['name'] for c in data['data']['cols']]
print(f"Fetched {len(rows)} rows")

# Parse data
dates = [r[0][:10] for r in rows]
total = [r[1] for r in rows]
unique_cx = [r[2] for r in rows]
unassigned = [r[3] for r in rows]
assigned = [r[4] for r in rows]
resolved = [r[5] for r in rows]
unresolved = [r[6] for r in rows]
resolved_recharged = [r[7] for r in rows]
resolved_recovered = [r[8] for r in rows]
resolved_bug = [r[9] for r in rows]
self_created = [r[10] for r in rows]

# Cumulative total
cum_total = []
s = 0
for t in total:
    s += t
    cum_total.append(s)

# Summary stats
total_tickets = sum(total)
total_unique = sum(unique_cx)
avg_daily = total_tickets / len(dates) if dates else 0
max_day = max(total) if total else 0
max_day_date = dates[total.index(max_day)] if total else ''
total_resolved = sum(resolved)
total_unresolved = sum(unresolved)
total_assigned = sum(assigned)
total_unassigned = sum(unassigned)
total_recharged = sum(resolved_recharged)
total_recovered = sum(resolved_recovered)
total_bug = sum(resolved_bug)
total_self_created = sum(self_created)
total_system_created = total_tickets - total_self_created

# Build HTML
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PayG Router Pickup (PUT) Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 20px; }}
  h1 {{ text-align: center; font-size: 1.6rem; margin-bottom: 6px; color: #f1f5f9; }}
  .subtitle {{ text-align: center; color: #94a3b8; font-size: 0.85rem; margin-bottom: 20px; }}
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
  .table-box {{ background: #1e293b; border-radius: 12px; padding: 16px; border: 1px solid #334155; overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  th {{ background: #334155; color: #e2e8f0; padding: 8px 10px; text-align: center; position: sticky; top: 0; }}
  td {{ padding: 6px 10px; text-align: center; border-bottom: 1px solid #334155; }}
  tr:hover td {{ background: #334155; }}
  .footer {{ text-align: center; color: #64748b; font-size: 0.7rem; margin-top: 16px; }}
  @media (max-width: 768px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>

<h1>PayG Router Pickup (PUT) Dashboard</h1>
<p class="subtitle">Day-wise pickup tickets for PayG customers (since 26 Jan 2026) &mdash; Data as of {dates[-1] if dates else 'N/A'}</p>

<div class="kpi-row">
  <div class="kpi"><div class="val">{total_tickets}</div><div class="lbl">Total Tickets</div></div>
  <div class="kpi" style="border-left:3px solid #e879f9;"><div class="val" style="color:#e879f9;">{total_self_created}</div><div class="lbl">Self Created ({round(total_self_created*100/total_tickets,1)}%)</div></div>
  <div class="kpi"><div class="val">{total_system_created}</div><div class="lbl">System Created ({round(total_system_created*100/total_tickets,1)}%)</div></div>
  <div class="kpi green"><div class="val">{total_resolved}</div><div class="lbl">Resolved ({round(total_resolved*100/total_tickets,1)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #22d3ee;"><div class="val" style="color:#22d3ee;">{total_recharged}</div><div class="lbl">Cx Recharged ({round(total_recharged*100/total_tickets,1)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #34d399;"><div class="val" style="color:#34d399;">{total_recovered}</div><div class="lbl">Router Recovered ({round(total_recovered*100/total_tickets,1)}%)</div></div>
  <div class="kpi" style="border-left:3px solid #fb923c;"><div class="val" style="color:#fb923c;">{total_bug}</div><div class="lbl">Bug ({round(total_bug*100/total_tickets,1)}%)</div></div>
  <div class="kpi red"><div class="val">{total_unresolved}</div><div class="lbl">Unresolved ({round(total_unresolved*100/total_tickets,1)}%)</div></div>
  <div class="kpi amber"><div class="val">{total_assigned}</div><div class="lbl">Assigned ({round(total_assigned*100/total_tickets,1)}%)</div></div>
  <div class="kpi purple"><div class="val">{total_unassigned}</div><div class="lbl">Unassigned ({round(total_unassigned*100/total_tickets,1)}%)</div></div>
  <div class="kpi"><div class="val">{round(avg_daily,1)}</div><div class="lbl">Avg Daily Tickets</div></div>
</div>

<div class="chart-grid">
  <div class="chart-box chart-full" id="chart1"></div>
  <div class="chart-box" id="chart2"></div>
  <div class="chart-box" id="chart3"></div>
  <div class="chart-box chart-full" id="chart5"></div>
  <div class="chart-box chart-full" id="chart4"></div>
</div>

<div class="table-box">
  <h3 style="margin-bottom:10px; color:#f1f5f9; font-size:1rem;">Day-wise Data Table</h3>
  <div style="max-height:400px; overflow-y:auto;">
  <table>
    <thead><tr><th>Date</th><th>Total</th><th style="color:#e879f9">Self Created</th><th>Unique Cx</th><th>Unassigned</th><th>Assigned</th><th>Resolved</th><th style="color:#22d3ee">Cx Recharged</th><th style="color:#34d399">Router Recovered</th><th style="color:#fb923c">Bug</th><th>Unresolved</th><th>Resolution %</th></tr></thead>
    <tbody>
"""

for i in range(len(dates)-1, -1, -1):
    res_pct = round(resolved[i]*100/total[i], 1) if total[i] > 0 else 0
    html += f"<tr><td>{dates[i]}</td><td>{total[i]}</td><td>{self_created[i]}</td><td>{unique_cx[i]}</td><td>{unassigned[i]}</td><td>{assigned[i]}</td><td>{resolved[i]}</td><td>{resolved_recharged[i]}</td><td>{resolved_recovered[i]}</td><td>{resolved_bug[i]}</td><td>{unresolved[i]}</td><td>{res_pct}%</td></tr>\n"

html += """
    </tbody>
  </table>
  </div>
</div>

<p class="footer">Source: prod_db.dynamodb_read.tasks (TYPE='ROUTER_PICKUP') + T_ROUTER_USER_MAPPING (cx_type='PayG') + customer_logs (renewal_fee_captured) | Resolution: EXTRA_DATA:resolutionType</p>

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
const dates = {json.dumps(dates)};
const total = {json.dumps(total)};
const cum = {json.dumps(cum_total)};
const unassigned = {json.dumps(unassigned)};
const assigned = {json.dumps(assigned)};
const resolved = {json.dumps(resolved)};
const unresolved = {json.dumps(unresolved)};
const resRecharged = {json.dumps(resolved_recharged)};
const resRecovered = {json.dumps(resolved_recovered)};
const resBug = {json.dumps(resolved_bug)};
const selfCreated = {json.dumps(self_created)};
const systemCreated = total.map((t, i) => t - selfCreated[i]);
"""

html += """
// Chart 1: Daily tickets (self vs system) + cumulative line
Plotly.newPlot('chart1', [
  { x: dates, y: selfCreated, type: 'bar', name: 'Self Created', marker: { color: '#e879f9' },
    text: selfCreated.map(String), textposition: 'auto', textfont: { size: 9, color: '#fff' } },
  { x: dates, y: systemCreated, type: 'bar', name: 'System Created', marker: { color: '#38bdf8' },
    text: systemCreated.map(String), textposition: 'auto', textfont: { size: 9, color: '#fff' } },
  { x: dates, y: cum, type: 'scatter', mode: 'lines+markers', name: 'Cumulative',
    yaxis: 'y2', line: { color: '#fbbf24', width: 2 }, marker: { size: 4 } }
], {
  ...darkLayout,
  title: { text: 'Daily PayG PUT Tickets (Self vs System) + Cumulative', font: { size: 14 } },
  yaxis: { ...darkLayout.yaxis, title: 'Daily Count' },
  yaxis2: { overlaying: 'y', side: 'right', gridcolor: 'transparent', title: 'Cumulative', titlefont: { color: '#fbbf24' }, tickfont: { color: '#fbbf24' } },
  barmode: 'stack'
}, cfg);

// Chart 2: Status breakdown stacked bar
Plotly.newPlot('chart2', [
  { x: dates, y: resRecharged, type: 'bar', name: 'Cx Recharged', marker: { color: '#22d3ee' } },
  { x: dates, y: resRecovered, type: 'bar', name: 'Router Recovered', marker: { color: '#34d399' } },
  { x: dates, y: resBug, type: 'bar', name: 'Bug', marker: { color: '#fb923c' } },
  { x: dates, y: unresolved, type: 'bar', name: 'Unresolved', marker: { color: '#f87171' } },
  { x: dates, y: assigned, type: 'bar', name: 'Assigned', marker: { color: '#fbbf24' } },
  { x: dates, y: unassigned, type: 'bar', name: 'Unassigned', marker: { color: '#a78bfa' } }
], {
  ...darkLayout,
  title: { text: 'Ticket Status Breakdown', font: { size: 13 } },
  barmode: 'stack'
}, cfg);

// Chart 3: Resolution % line
const resPct = total.map((t, i) => t > 0 ? Math.round(resolved[i] * 100 / t * 10) / 10 : 0);
Plotly.newPlot('chart3', [
  { x: dates, y: resPct, type: 'scatter', mode: 'lines+markers',
    name: 'Resolution %', line: { color: '#4ade80', width: 2 }, marker: { size: 5 },
    text: resPct.map(v => v + '%'), textposition: 'top center', textfont: { size: 9 } }
], {
  ...darkLayout,
  title: { text: 'Daily Resolution Rate %', font: { size: 13 } },
  yaxis: { ...darkLayout.yaxis, title: 'Resolution %', range: [0, 105] }
}, cfg);

// Chart 5: Resolution bifurcation stacked bar (only resolved tickets)
Plotly.newPlot('chart5', [
  { x: dates, y: resRecharged, type: 'bar', name: 'Cx Recharged', marker: { color: '#22d3ee' },
    text: resRecharged.map(String), textposition: 'auto', textfont: { size: 9, color: '#fff' } },
  { x: dates, y: resRecovered, type: 'bar', name: 'Router Recovered', marker: { color: '#34d399' },
    text: resRecovered.map(String), textposition: 'auto', textfont: { size: 9, color: '#fff' } },
  { x: dates, y: resBug, type: 'bar', name: 'Bug', marker: { color: '#fb923c' },
    text: resBug.map(String), textposition: 'auto', textfont: { size: 9, color: '#fff' } }
], {
  ...darkLayout,
  title: { text: 'Resolved Tickets Bifurcation — Cx Recharged vs Router Recovered vs Bug', font: { size: 14 } },
  yaxis: { ...darkLayout.yaxis, title: 'Resolved Tickets' },
  barmode: 'stack'
}, cfg);

// Chart 4: 7-day moving average
const ma7 = total.map((_, i) => {
  if (i < 6) return null;
  let sum = 0;
  for (let j = i - 6; j <= i; j++) sum += total[j];
  return Math.round(sum / 7 * 10) / 10;
});
Plotly.newPlot('chart4', [
  { x: dates, y: total, type: 'bar', name: 'Daily', marker: { color: '#38bdf8', opacity: 0.4 } },
  { x: dates, y: ma7, type: 'scatter', mode: 'lines', name: '7-Day Avg',
    line: { color: '#f472b6', width: 3 } }
], {
  ...darkLayout,
  title: { text: 'Daily Tickets with 7-Day Moving Average', font: { size: 14 } },
  yaxis: { ...darkLayout.yaxis, title: 'Tickets' }
}, cfg);
</script>

</body>
</html>
"""

# Output to output_recharge/ so it's deployed alongside the main dashboard
os.makedirs('output_recharge', exist_ok=True)
out_path = os.path.join('output_recharge', 'put.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"PUT Dashboard saved to: {out_path}")
