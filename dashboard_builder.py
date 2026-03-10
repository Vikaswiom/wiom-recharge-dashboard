"""
WIOM Recharge Lifecycle Analysis - Dynamic Dashboard
Fetches live data from Metabase API (Snowflake) instead of CSV.
"""
import json, os, math, ssl, urllib.request
import numpy as np
from collections import Counter, defaultdict, OrderedDict
from datetime import datetime, timedelta

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output_recharge')
os.makedirs(OUT, exist_ok=True)

# =====================================================================
# FETCH DATA FROM METABASE
# =====================================================================
print("Reading API key...")
api_key = os.environ.get('METABASE_API_KEY')
if not api_key:
    # Fallback: read from local .env file
    env_path = r"C:\credentials\.env"
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('METABASE_API_KEY='):
                    api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                    break
if not api_key:
    raise RuntimeError("METABASE_API_KEY not found in env or .env file")

SQL_QUERY = """with installs_and_free_plan as(
select * from(
    select mobile, idmaker(shard,0,router_nas_id) as lng_nas_id,
    cast(OTP_ISSUED_TIME + interval '330 minute' as date) as install_date,
    OTP_ISSUED_TIME + interval '330 minute' as INSTALL_TIME,
    OTP_ISSUED_TIME + interval '330 minute' as free_plan_start_time,
    OTP_EXPIRY_TIME + interval '330 minute' as free_plan_end_time
    from T_ROUTER_USER_MAPPING WHERE otp = 'DONE' and store_group_id = 0
    and device_limit > 1 and mobile > '5999999999'
    qualify row_number() over(PARTITION by lng_nas_id order by OTP_EXPIRY_TIME) = 1
    ) where install_date >= '2026-01-26' and mobile not in('6900099267','7679376747')
),
free_plan_usage AS (
    SELECT i.lng_nas_id, i.install_date, i.install_time, i.free_plan_end_time,
        round(SUM(CASE WHEN u.hour_start >= i.install_time AND u.hour_start < DATEADD(HOUR, 48, i.install_time)
                THEN COALESCE(u.total_bytes_hourly, 0) ELSE 0 END) / (1024.0*1024.0*1024.0),2) AS gb_48h
    FROM installs_and_free_plan i
    LEFT JOIN HOURLY_USAGE_PRORATED_DT u ON u.nasid = i.lng_nas_id
       AND u.hour_start >= i.install_time AND u.hour_start < DATEADD(HOUR, 48, i.install_time)
    GROUP BY 1,2,3,4
),
first_paid_plan AS (
    SELECT idmaker(trum.shard,0,trum.router_nas_id) AS trum_nas,
        OTP_ISSUED_TIME + interval '300 minute' AS paid_plan_start_time,
        OTP_EXPIRY_TIME + interval '330 minute' AS paid_plan_end_time,
        tpc.TIME_LIMIT/86400 AS paid_plan_duration_days,
        ROW_NUMBER() OVER (PARTITION BY trum_nas ORDER BY (OTP_ISSUED_TIME + interval '300 minute')) AS rn
    FROM T_ROUTER_USER_MAPPING trum LEFT JOIN T_PLAN_CONFIGURATION tpc ON tpc.id = trum.SELECTED_PLAN_ID
    WHERE otp = 'DONE' AND store_group_id = 0 AND device_limit > 1 AND mobile > '5999999999' QUALIFY rn = 2
),
first_ping_after_install AS (
    SELECT i.lng_nas_id, MIN(h.FIRST_PING_TS_IST) AS first_ping_ts_after_install_ist
    FROM installs_and_free_plan i LEFT JOIN HOURLY_DEVICE_PING_HEALTH_VW h
        ON h.nas_id = i.lng_nas_id AND h.HOUR_START_IST >= DATE_TRUNC('hour', i.install_time) AND h.FIRST_PING_TS_IST IS NOT NULL
    GROUP BY 1
),
first_ping_after_paid AS (
    SELECT i.lng_nas_id, MIN(h.FIRST_PING_TS_IST) AS first_ping_ts_after_paid_plan_ist
    FROM installs_and_free_plan i LEFT JOIN first_paid_plan fpp ON fpp.trum_nas = i.lng_nas_id
    LEFT JOIN HOURLY_DEVICE_PING_HEALTH_VW h ON h.nas_id = i.lng_nas_id
       AND fpp.paid_plan_start_time IS NOT NULL AND h.HOUR_START_IST >= DATE_TRUNC('hour', fpp.paid_plan_start_time) AND h.FIRST_PING_TS_IST IS NOT NULL
    GROUP BY 1
),
install_enriched AS (
    SELECT fpu.lng_nas_id, fpu.install_date, fpu.install_time, fpp.paid_plan_start_time,
        fpp.paid_plan_duration_days, TRY_TO_NUMBER(ROUND(fpp.paid_plan_duration_days, 0)) AS paid_plan_duration_int,
        fpu.gb_48h, fpi.first_ping_ts_after_install_ist, fppg.first_ping_ts_after_paid_plan_ist, fpu.free_plan_end_time,
        IFF(fpi.first_ping_ts_after_install_ist IS NULL, NULL,
            IFF(DATEDIFF('second', fpu.install_time, fpi.first_ping_ts_after_install_ist) < 0, 2.5,
                DATEDIFF('second', fpu.install_time, fpi.first_ping_ts_after_install_ist) / 60.0)) AS ping_up_time_post_install_mins,
        DATEDIFF('second', fpu.free_plan_end_time, fpp.paid_plan_start_time) / 3600.0 AS recharge_delay_first_paid_plan_hrs,
        IFF(fpp.paid_plan_start_time IS NULL OR fppg.first_ping_ts_after_paid_plan_ist IS NULL, NULL,
            IFF(DATEDIFF('second', fpp.paid_plan_start_time, fppg.first_ping_ts_after_paid_plan_ist) < 0, 2.5,
                DATEDIFF('second', fpp.paid_plan_start_time, fppg.first_ping_ts_after_paid_plan_ist) / 60.0)) AS first_recharge_ping_delay_mins
    FROM free_plan_usage fpu LEFT JOIN first_paid_plan fpp ON fpp.trum_nas = fpu.lng_nas_id
    LEFT JOIN first_ping_after_install fpi ON fpi.lng_nas_id = fpu.lng_nas_id
    LEFT JOIN first_ping_after_paid fppg ON fppg.lng_nas_id = fpu.lng_nas_id
),
plan_sequence AS (
    SELECT idmaker(trum.shard,0,trum.router_nas_id) AS lng_nas_id,
        OTP_ISSUED_TIME + interval '330 minute' AS plan_start_time,
        OTP_EXPIRY_TIME + interval '330 minute' AS plan_end_time,
        tpc.TIME_LIMIT / 86400 AS plan_duration_days,
        ROW_NUMBER() OVER (PARTITION BY idmaker(trum.shard,0,trum.router_nas_id) ORDER BY OTP_ISSUED_TIME) AS plan_number
    FROM T_ROUTER_USER_MAPPING trum LEFT JOIN T_PLAN_CONFIGURATION tpc ON tpc.id = trum.SELECTED_PLAN_ID
    WHERE otp = 'DONE' AND store_group_id = 0 AND device_limit > 1 AND mobile > '5999999999'
),
final as (select *,ROW_NUMBER() over (PARTITION by lng_nas_id order by plan_start_time) as recharge_number
from (select a.lng_nas_id, a.install_time,
case when a.paid_plan_start_time is null then 'No' else 'Yes' end as paid_after_trial_or_not,
b.plan_start_time, plan_end_time, plan_duration_days
from install_enriched a
left join plan_sequence b on a.lng_nas_id=b.lng_nas_id and a.paid_plan_start_time<=b.plan_start_time))
select * from final"""

print("Fetching data from Metabase (Snowflake)...")
ctx = ssl.create_default_context()
payload = json.dumps({
    'database': 113,
    'type': 'native',
    'native': {'query': SQL_QUERY},
    'constraints': {'max-results': 50000, 'max-results-bare-rows': 50000}
}).encode()
req = urllib.request.Request('https://metabase.wiom.in/api/dataset', data=payload, headers={
    'x-api-key': api_key,
    'Content-Type': 'application/json'
})
resp = urllib.request.urlopen(req, context=ctx, timeout=300)
api_data = json.loads(resp.read())

columns = [c['name'] for c in api_data['data']['cols']]
api_rows = api_data['data']['rows']
print(f"Fetched {len(api_rows)} rows, columns: {columns}")

def parse_ts(s):
    if s is None: return None
    s = str(s).strip()
    if not s: return None
    if '+' in s and s.index('+') > 10:
        s = s[:s.rindex('+')]
    elif s.endswith('Z'):
        s = s[:-1]
    for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
        try: return datetime.strptime(s, fmt)
        except ValueError: continue
    return None

def safe_int(s, default=0):
    try: return int(float(s))
    except (ValueError, TypeError): return default

NOW = datetime.utcnow() + timedelta(hours=5, minutes=30)  # Convert UTC to IST
NOW_STR = NOW.strftime('%b %d, %Y %H:%M')
TODAY = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
TODAY_STR = TODAY.strftime('%b %d, %Y')
TRIAL_DURATION = 2

# =====================================================================
# STEP 0: PARSE & CLEAN
# =====================================================================
col_idx = {c: i for i, c in enumerate(columns)}
raw_rows = []
user_install = {}    # uid -> install_time
user_paid_type = {}  # uid -> paid_after_trial_or_not
for row in api_rows:
    uid = str(row[col_idx['LNG_NAS_ID']]).strip()
    install_time = parse_ts(row[col_idx['INSTALL_TIME']])
    paid_type = str(row[col_idx['PAID_AFTER_TRIAL_OR_NOT']]).strip()
    plan_start = parse_ts(row[col_idx['PLAN_START_TIME']])
    plan_end = parse_ts(row[col_idx['PLAN_END_TIME']])
    plan_duration = safe_int(row[col_idx['PLAN_DURATION_DAYS']])

    if uid:
        user_install[uid] = install_time
        user_paid_type[uid] = paid_type
        if plan_start and plan_end and plan_duration > 0:
            raw_rows.append({
                'uid': uid, 'install_time': install_time,
                'plan_start': plan_start, 'plan_end': plan_end,
                'plan_duration': plan_duration,
            })

print(f"Parsed {len(raw_rows)} paid recharge rows")

# Classify users (new CSV uses "Yes"/"No")
converted_uids = set(uid for uid, pt in user_paid_type.items()
                     if pt.strip().lower() == 'yes' or 'paid recharge done' in pt.lower())
trial_only_uids = set(uid for uid, pt in user_paid_type.items()
                      if pt.strip().lower() == 'no' or 'no paid' in pt.lower())
all_uids = converted_uids | trial_only_uids

# Paid rows: standard plan durations only (1, 2, 7, 14, 28)
paid_rows = [r for r in raw_rows if r['plan_duration'] in (1, 2, 7, 14, 28)]
print(f"Trial-only users: {len(trial_only_uids)}")
print(f"Converted users: {len(converted_uids)} ({len(paid_rows)} paid rows, plans 1/2/7/14/28d)")

# =====================================================================
# STEP 1: USER-LEVEL AGGREGATION
# =====================================================================
print("Building user-level data...")

# Group paid rows by user, sorted by plan_start
user_paid = defaultdict(list)
for r in paid_rows:
    user_paid[r['uid']].append(r)
for uid in user_paid:
    user_paid[uid].sort(key=lambda x: x['plan_start'])
    for i, r in enumerate(user_paid[uid]):
        r['recharge_num'] = i + 1  # 1-based recharge number

users = {}
for uid in all_uids:
    paid_recs = user_paid.get(uid, [])
    install_time = user_install.get(uid)
    is_converted = uid in converted_uids

    # Trial expiry = install + 2 days (implicit 2-day free trial)
    trial_expiry = (install_time + timedelta(days=TRIAL_DURATION)) if install_time else None

    # First paid time = first plan_start for converted users
    first_paid_time = paid_recs[0]['plan_start'] if paid_recs else None

    # Has standard paid plans?
    has_std_paid = len(paid_recs) > 0

    # Trial-to-paid days (install to first paid)
    trial_to_paid = None
    if is_converted and install_time and first_paid_time:
        trial_to_paid = (first_paid_time - install_time).total_seconds() / 86400

    # Days from trial expiry to first paid
    days_post_trial = None
    converted_before_trial = False
    converted_after_trial = False
    if is_converted and trial_expiry and first_paid_time:
        days_post_trial = (first_paid_time - trial_expiry).total_seconds() / 86400
        if days_post_trial < 0:
            converted_before_trial = True
        else:
            converted_after_trial = True

    # First paid plan duration (from first recharge row)
    first_paid_dur = paid_recs[0]['plan_duration'] if paid_recs else 0

    # Gaps between paid recharges
    gaps = []
    for i in range(1, len(paid_recs)):
        prev_end = paid_recs[i-1]['plan_end']
        curr_start = paid_recs[i]['plan_start']
        if prev_end and curr_start:
            gap = (curr_start - prev_end).total_seconds() / 86400
            gaps.append(gap)

    # Last plan end (from paid recs, or trial if no paid)
    if paid_recs:
        last_end = max((r['plan_end'] for r in paid_recs if r['plan_end']), default=trial_expiry)
    else:
        last_end = trial_expiry

    # Lifetime
    lifetime = None
    if install_time and last_end:
        lifetime = (last_end - install_time).total_seconds() / 86400

    # Max paid recharge count
    max_paid_recharge = len(paid_recs)

    # Plan status
    trial_expired = trial_expiry is not None and trial_expiry < TODAY
    trial_active = trial_expiry is not None and trial_expiry >= TODAY
    plan_active = last_end is not None and last_end >= TODAY
    is_churned = False
    retention_status = 'active'

    if not is_converted:
        if trial_expired:
            retention_status = 'never_converted'
        else:
            retention_status = 'trial_active'
    elif plan_active:
        retention_status = 'active'
    elif last_end is not None:
        is_churned = True
        retention_status = 'churned'

    # Eligibility per paid recharge step
    eligible_at = {}
    retained_at = {}
    for i, rec in enumerate(paid_recs):
        n = i + 1  # paid recharge number (1-based)
        pe = rec['plan_end']
        if pe and pe < TODAY:
            eligible_at[n] = True
            has_next = i + 1 < len(paid_recs)
            retained_at[n] = has_next
        else:
            eligible_at[n] = False
            retained_at[n] = None

    # Plan durations (paid only)
    durations = [r['plan_duration'] for r in paid_recs if r['plan_duration'] > 0]
    total_plan_days = sum(durations)
    avg_duration = float(np.mean(durations)) if durations else 0

    # R-day for non-converted
    r_day = None
    if not is_converted and trial_expired and trial_expiry:
        r_day = (TODAY - trial_expiry).days

    users[uid] = {
        'uid': uid,
        'install_time': install_time,
        'first_paid_time': first_paid_time,
        'trial_expiry': trial_expiry,
        'is_converted': is_converted,
        'has_std_paid': has_std_paid,
        'trial_to_paid': trial_to_paid,
        'days_post_trial': days_post_trial,
        'converted_before_trial': converted_before_trial,
        'converted_after_trial': converted_after_trial,
        'first_paid_dur': first_paid_dur,
        'max_paid_recharge': max_paid_recharge,
        'total_paid_recharges': len(paid_recs),
        'gaps': gaps,
        'median_gap': float(np.median(gaps)) if gaps else None,
        'avg_gap': float(np.mean(gaps)) if gaps else None,
        'lifetime_days': lifetime,
        'is_churned': is_churned,
        'plan_active': plan_active,
        'retention_status': retention_status,
        'eligible_at': eligible_at,
        'retained_at': retained_at,
        'total_plan_days': total_plan_days,
        'avg_duration': avg_duration,
        'durations': durations,
        'last_end': last_end,
        'trial_expired': trial_expired,
        'trial_active': trial_active,
        'r_day': r_day,
        'paid_recs': paid_recs,
    }

total_users = len(users)
converted_users_list = [u for u in users.values() if u['is_converted']]
trial_only_list = [u for u in users.values() if not u['is_converted']]
std_plan_users_list = [u for u in users.values() if u['has_std_paid']]

n_converted = len(converted_users_list)
n_trial_only = len(trial_only_list)
n_trial_active = sum(1 for u in trial_only_list if u['trial_active'])
n_never_converted = sum(1 for u in trial_only_list if u['trial_expired'])
n_evaluable = total_users - n_trial_active
conv_rate = n_converted * 100 / max(1, n_evaluable)

n_active = sum(1 for u in users.values() if u['plan_active'] and u['is_converted'])
n_churned = sum(1 for u in users.values() if u['is_churned'])

print(f"Total unique users: {total_users}")
print(f"  Trial still active: {n_trial_active}")
print(f"  Evaluable (trial expired): {n_evaluable}")
print(f"  Converted to paid: {n_converted} ({conv_rate:.1f}% of evaluable)")
print(f"  Never converted (trial expired): {n_never_converted}")
print(f"  Active paid plans: {n_active}, Churned: {n_churned}")

# Month-wise install & conversion (newest first)
month_data = OrderedDict()
for u in users.values():
    if u['install_time']:
        mk = u['install_time'].strftime('%Y-%m')
        if mk not in month_data:
            month_data[mk] = {'installs': 0, 'converted': 0, 'trial_active': 0, 'never_converted': 0}
        month_data[mk]['installs'] += 1
        if u['is_converted']:
            month_data[mk]['converted'] += 1
        elif u['trial_active']:
            month_data[mk]['trial_active'] += 1
        elif u['trial_expired']:
            month_data[mk]['never_converted'] += 1

# Sort newest first
month_data = OrderedDict(sorted(month_data.items(), reverse=True))

month_tbl = ""
for mk, md in month_data.items():
    evaluable = md['installs'] - md['trial_active']
    cr = md['converted'] * 100 / max(1, evaluable)
    cr_clr = "#27AE60" if cr >= 50 else "#F39C12" if cr >= 30 else "#E74C3C"
    month_label = datetime.strptime(mk, '%Y-%m').strftime('%b %Y')
    trial_note = f" <small style='color:#888'>({md['trial_active']} in trial)</small>" if md['trial_active'] > 0 else ""
    month_tbl += f"<tr><td><b>{month_label}</b></td><td>{md['installs']}</td><td>{md['converted']}</td><td>{md['never_converted']}</td><td>{md['trial_active']}</td><td>{evaluable}</td><td style='color:{cr_clr}'><b>{cr:.1f}%</b>{trial_note}</td></tr>"

print(f"\nMonth-wise breakdown ({len(month_data)} months):")
for mk, md in month_data.items():
    evaluable = md['installs'] - md['trial_active']
    cr = md['converted'] * 100 / max(1, evaluable)
    print(f"  {mk}: {md['installs']} installs, {md['converted']} converted, {cr:.1f}% rate (excl {md['trial_active']} in trial)")

# =====================================================================
# SECTION 1: CONVERSION OVERVIEW (Enhanced)
# =====================================================================
print("\n" + "="*70)
print("1. CONVERSION OVERVIEW")
print("="*70)

# Install to first paid
t2p = [u['trial_to_paid'] for u in converted_users_list if u['trial_to_paid'] is not None and u['trial_to_paid'] >= 0]
t2p_arr = np.array(t2p) if t2p else np.array([0])

median_t2p = float(np.median(t2p_arr))
p80_t2p = float(np.percentile(t2p_arr, 80))
p90_t2p = float(np.percentile(t2p_arr, 90))
median_t2p_h = round(median_t2p * 24, 1)
p80_t2p_h = round(p80_t2p * 24, 1)
p90_t2p_h = round(p90_t2p * 24, 1)

print(f"\nInstall to First Paid ({len(t2p)} users):")
print(f"  Median: {median_t2p_h:.1f} hours")
print(f"  P80: {p80_t2p_h:.1f} hours")
print(f"  P90: {p90_t2p_h:.1f} hours")

# Before vs after trial expiry
n_before_trial = sum(1 for u in converted_users_list if u['converted_before_trial'])
n_after_trial = sum(1 for u in converted_users_list if u['converted_after_trial'])

print(f"\n  Converted BEFORE trial expired: {n_before_trial}")
print(f"  Converted AFTER trial expired: {n_after_trial}")

# TAT for after-trial converters
after_trial_tat = [u['days_post_trial'] for u in converted_users_list
                   if u['converted_after_trial'] and u['days_post_trial'] is not None]
tat_arr = np.array(after_trial_tat) if after_trial_tat else np.array([0])

median_tat = float(np.median(tat_arr)) if after_trial_tat else 0
p80_tat = float(np.percentile(tat_arr, 80)) if after_trial_tat else 0
p90_tat = float(np.percentile(tat_arr, 90)) if after_trial_tat else 0
median_tat_h = round(median_tat * 24, 1)
p80_tat_h = round(p80_tat * 24, 1)
p90_tat_h = round(p90_tat * 24, 1)

print(f"\n  Post-Trial TAT ({len(after_trial_tat)} after-expiry users):")
print(f"    Median: {median_tat_h:.1f} hours")
print(f"    P80: {p80_tat_h:.1f} hours")
print(f"    P90: {p90_tat_h:.1f} hours")

# Conversion time buckets: within 2 days, 3rd day, 4th day, 5th day, 6+ days
conv_buckets = OrderedDict()
conv_buckets['Within 48 Hrs'] = sum(1 for d in t2p if d <= 2)
conv_buckets['48-72 Hrs'] = sum(1 for d in t2p if 2 < d <= 3)
conv_buckets['72-96 Hrs'] = sum(1 for d in t2p if 3 < d <= 4)
conv_buckets['96-120 Hrs'] = sum(1 for d in t2p if 4 < d <= 5)
conv_buckets['120+ Hrs'] = sum(1 for d in t2p if d > 5)

print(f"\n  Conversion Time Buckets (from install):")
for bucket, cnt in conv_buckets.items():
    pct = cnt * 100 / max(1, len(t2p))
    print(f"    {bucket:<18s}: {cnt:>5d} ({pct:>5.1f}%)")

# =====================================================================
# SECTION 2: FIRST PLAN DURATION ANALYSIS
# =====================================================================
print("\n" + "="*70)
print("2. FIRST PLAN DURATION ANALYSIS")
print("="*70)

def plan_cat(dur):
    if dur == 1: return '1-Day'
    elif dur == 2: return '2-Day'
    elif dur == 7: return '7-Day'
    elif dur == 14: return '14-Day'
    elif dur == 28: return '28-Day'
    else: return '28-Day'

plan_colors_map = {'1-Day': '#E74C3C', '2-Day': '#9B59B6', '7-Day': '#F39C12', '14-Day': '#FFEAA7', '28-Day': '#4ECDC4'}
_plan_order_list = ['1-Day', '2-Day', '7-Day', '14-Day', '28-Day']

fpd = [u['first_paid_dur'] for u in std_plan_users_list if u['first_paid_dur'] > 0]
fpd_buckets = OrderedDict()
fpd_buckets['1 day'] = sum(1 for d in fpd if d == 1)
fpd_buckets['2 days'] = sum(1 for d in fpd if d == 2)
fpd_buckets['7 days'] = sum(1 for d in fpd if d == 7)
fpd_buckets['14 days'] = sum(1 for d in fpd if d == 14)
fpd_buckets['28 days'] = sum(1 for d in fpd if d == 28)

n_std = len(std_plan_users_list)
print(f"\nFirst Paid Plan Duration ({n_std} users with standard plans):")
for bucket, cnt in fpd_buckets.items():
    pct = cnt * 100 / max(1, n_std)
    print(f"  {bucket:<15s}: {cnt:>5d} ({pct:>5.1f}%)")

pct_28d = fpd_buckets.get('28 days', 0) * 100 / max(1, n_std)
pct_1d = fpd_buckets.get('1 day', 0) * 100 / max(1, n_std)

# Cross: t2p by first plan duration
cross = defaultdict(list)
cross_cats = ['1d', '2d', '7d', '14d', '28d']
for u in std_plan_users_list:
    if u['trial_to_paid'] is not None:
        d = u['first_paid_dur']
        cat = '1d' if d == 1 else '2d' if d == 2 else '7d' if d == 7 else '14d' if d == 14 else '28d'
        cross[cat].append(u['trial_to_paid'])
cross_data = {}
print(f"\nMedian Install-to-Paid by First Plan:")
for cat in cross_cats:
    if cat in cross:
        arr = np.array(cross[cat])
        med_h = round(float(np.median(arr)) * 24, 1)
        print(f"  {cat:<8s}: median {med_h:.1f} hours, n={len(arr)}")
        cross_data[cat] = {'median_h': med_h, 'n': len(arr)}

# =====================================================================
# SECTION 3: RETENTION CURVE (Eligibility-Based, Paid Recharges)
# =====================================================================
print("\n" + "="*70)
print("3. RECHARGE RETENTION CURVE (Eligibility-Based)")
print("="*70)

max_rech = max((u['max_paid_recharge'] for u in users.values()), default=0)
retention = OrderedDict()
print(f"\n  {'Step':<22s} | {'Eligible':>8s} | {'Retained':>8s} | {'Rate':>6s} | {'Active':>8s}")
print("  " + "-" * 60)

for n in range(1, min(max_rech + 1, 16)):
    users_at_n = [u for u in users.values() if u['max_paid_recharge'] >= n]
    eligible = [u for u in users_at_n if u['eligible_at'].get(n, False)]
    retained = [u for u in eligible if u['retained_at'].get(n, False)]
    still_active = [u for u in users_at_n if not u['eligible_at'].get(n, False)]

    elig_cnt = len(eligible)
    ret_cnt = len(retained)
    act_cnt = len(still_active)
    ret_rate = ret_cnt * 100 / max(1, elig_cnt) if elig_cnt > 0 else None

    retention[n] = {
        'total': len(users_at_n), 'eligible': elig_cnt, 'retained': ret_cnt,
        'active': act_cnt, 'rate': round(ret_rate, 1) if ret_rate is not None else None,
    }
    rate_str = f"{ret_rate:.1f}%" if ret_rate is not None else "N/A"
    print(f"  Paid Recharge #{n:<5d} | {elig_cnt:>8d} | {ret_cnt:>8d} | {rate_str:>6s} | {act_cnt:>8d}")

r1_elig = retention.get(1, {}).get('eligible', 0)
r1_ret = retention.get(1, {}).get('retained', 0)
r1_act = retention.get(1, {}).get('active', 0)
r1_to_r2_rate = r1_ret * 100 / max(1, r1_elig) if r1_elig > 0 else 0

# =====================================================================
# SECTION 4: GAP ANALYSIS (Plan-Wise, Hours)
# =====================================================================
print("\n" + "="*70)
print("4. GAP ANALYSIS (Plan-Wise)")
print("="*70)

all_gaps = []
for u in users.values():
    all_gaps.extend(u['gaps'])

# Build plan-wise gaps: group by previous plan duration
plan_wise_gaps = defaultdict(list)
for u in users.values():
    recs = u['paid_recs']
    for i in range(1, len(recs)):
        prev_end = recs[i-1]['plan_end']
        curr_start = recs[i]['plan_start']
        prev_dur = recs[i-1]['plan_duration']
        if prev_end and curr_start and prev_dur in (1, 2, 7, 14, 28):
            gap_days = (curr_start - prev_end).total_seconds() / 86400
            plan_wise_gaps[prev_dur].append(gap_days)

plan_gap_stats = OrderedDict()
for dur in [1, 2, 7, 14, 28]:
    gaps_list = plan_wise_gaps.get(dur, [])
    if gaps_list:
        arr = np.array(gaps_list)
        plan_gap_stats[dur] = {
            'count': len(gaps_list),
            'median_h': round(float(np.median(arr)) * 24, 1),
            'p80_h': round(float(np.percentile(arr, 80)) * 24, 1),
            'p90_h': round(float(np.percentile(arr, 90)) * 24, 1),
            'mean_h': round(float(np.mean(arr)) * 24, 1),
        }
    else:
        plan_gap_stats[dur] = {'count': 0, 'median_h': 0, 'p80_h': 0, 'p90_h': 0, 'mean_h': 0}

if all_gaps:
    gap_arr = np.array(all_gaps)
    median_gap_val = float(np.median(gap_arr))
    median_gap_h = round(median_gap_val * 24, 1)
    print(f"\nOverall gap ({len(all_gaps)} gaps): Median {median_gap_h:.1f} hours")
    print(f"\nPlan-Wise Gap to Next Recharge:")
    for dur in [1, 2, 7, 14, 28]:
        s = plan_gap_stats[dur]
        print(f"  {dur}-Day Plan: Median {s['median_h']}h, P80 {s['p80_h']}h, P90 {s['p90_h']}h (n={s['count']})")
else:
    gap_arr = np.array([0])
    median_gap_val = 0
    median_gap_h = 0

# =====================================================================
# SECTION 5: USER SEGMENTATION
# =====================================================================
print("\n" + "="*70)
print("5. USER SEGMENTATION")
print("="*70)

segments = OrderedDict()
seg_order = ['Power Users (4+ paid)', 'Mid Retention (2-3 paid)', 'One-Time Paid',
             'Active - Not Yet Eligible', 'Never Converted', 'Trial Active']
for uid, u in users.items():
    if not u['is_converted']:
        seg = 'Trial Active' if u['trial_active'] else 'Never Converted'
    elif u['max_paid_recharge'] >= 4:
        seg = 'Power Users (4+ paid)'
    elif u['max_paid_recharge'] >= 2:
        seg = 'Mid Retention (2-3 paid)'
    elif u['plan_active']:
        seg = 'Active - Not Yet Eligible'
    else:
        seg = 'One-Time Paid'
    segments.setdefault(seg, []).append(u)

seg_profiles = OrderedDict()
for seg in seg_order:
    group = segments.get(seg, [])
    if not group: continue
    durations = [u['avg_duration'] for u in group if u['avg_duration'] > 0]
    gaps_s = [u['median_gap'] for u in group if u['median_gap'] is not None]
    lifetimes = [u['lifetime_days'] for u in group if u['lifetime_days'] is not None and u['lifetime_days'] > 0]
    ltv = [u['total_plan_days'] for u in group]
    churned = sum(1 for u in group if u['is_churned'])
    prof = {
        'count': len(group), 'pct': round(len(group) * 100 / total_users, 1),
        'avg_duration': round(float(np.mean(durations)), 1) if durations else 0,
        'median_gap': round(float(np.median(gaps_s)), 1) if gaps_s else 0,
        'avg_lifetime': round(float(np.mean(lifetimes)), 1) if lifetimes else 0,
        'avg_ltv_days': round(float(np.mean(ltv)), 1),
        'churn_pct': round(churned * 100 / max(1, len(group)), 1),
    }
    seg_profiles[seg] = prof
    print(f"\n  {seg}: {prof['count']} users ({prof['pct']}%)")
    print(f"    Avg plan dur: {prof['avg_duration']}d, Med gap: {prof['median_gap']}d, Avg lifetime: {prof['avg_lifetime']}d")

# =====================================================================
# SECTION 6: INSIGHTS
# =====================================================================
print("\n" + "="*70)
print("6. INSIGHTS & RECOMMENDATIONS")
print("="*70)

nc_pct = n_never_converted * 100 / max(1, n_evaluable)
power_count = seg_profiles.get('Power Users (4+ paid)', {}).get('count', 0)
power_pct = seg_profiles.get('Power Users (4+ paid)', {}).get('pct', 0)
onetime_count = seg_profiles.get('One-Time Paid', {}).get('count', 0)
onetime_pct = seg_profiles.get('One-Time Paid', {}).get('pct', 0)

insights = [
    f"1. Conversion: {conv_rate:.1f}% of evaluable users converted ({n_converted}/{n_evaluable}). {n_never_converted} never converted. {n_trial_active} still in trial.",
    f"2. 28-day plan dominates at {pct_28d:.0f}%. Median install-to-paid: {median_t2p_h:.1f}h (P80: {p80_t2p_h:.1f}h, P90: {p90_t2p_h:.1f}h).",
    f"3. {n_before_trial} users ({n_before_trial*100/max(1,n_converted):.0f}%) convert BEFORE trial expires -- high-intent segment.",
    f"4. Post-trial TAT: median {median_tat_h:.1f}h, P80 {p80_tat_h:.1f}h, P90 {p90_tat_h:.1f}h -- most convert quickly after expiry.",
    f"5. R1->R2 paid retention: {r1_to_r2_rate:.0f}% (of {r1_elig} eligible). Power users ({power_count}): {power_pct}% of base.",
]
churn_risks = [
    f"1. {n_never_converted} evaluable users ({nc_pct:.0f}%) never converted -- primary acquisition leak.",
    f"2. Gap > 72 hours = churn signal: {sum(1 for g in all_gaps if g > 3)} of {len(all_gaps)} renewals.",
    f"3. One-time paid users: {onetime_count} ({onetime_pct}%) -- plan expired, never recharged.",
]
recommendations = [
    "1. Day-2 Push: CTA on trial expiry day — highest conversion window.",
    "2. Auto-renewal nudge on paid plan expiry — target 0-3 day gap window.",
    "3. Win-back: Trial-expired non-converted within 3 days with special offer.",
]
experiments = [
    "1. A/B: Extend trial by 1 day for non-converters at day 2 — measure lift.",
    "2. A/B: Offer 7-day at discount vs 28-day default — measure conversion rate.",
]
for i in insights: print(f"  {i}")

# =====================================================================
# SECTION 9: DAY-WISE METRICS
# =====================================================================
print("\n" + "="*70)
print("7. DAY-WISE METRICS")
print("="*70)

all_dates = []
for u in users.values():
    if u['install_time']: all_dates.append(u['install_time'].date())
date_min = min(all_dates) if all_dates else TODAY.date()
date_max = min(max(all_dates), TODAY.date()) if all_dates else TODAY.date()

day_metrics = OrderedDict()
d = date_min
while d <= date_max:
    day_metrics[d] = {'installs': 0, 'conversions': 0, 'active': 0, 'expired': 0, 'recharged': 0}
    d += timedelta(days=1)

for u in users.values():
    if u['install_time']:
        dd = u['install_time'].date()
        if dd in day_metrics: day_metrics[dd]['installs'] += 1
    if u['first_paid_time']:
        dd = u['first_paid_time'].date()
        if dd in day_metrics: day_metrics[dd]['conversions'] += 1

# Active paid users per day
day_active_uids = defaultdict(set)
for r in paid_rows:
    if r['plan_start'] and r['plan_end']:
        s, e = r['plan_start'].date(), min(r['plan_end'].date(), date_max)
        d, cap = s, 0
        while d <= e and cap < 90:
            day_active_uids[d].add(r['uid'])
            d += timedelta(days=1); cap += 1
for d in day_metrics:
    day_metrics[d]['active'] = len(day_active_uids.get(d, set()))

for r in paid_rows:
    if r['plan_end']:
        dd = r['plan_end'].date()
        if dd in day_metrics: day_metrics[dd]['expired'] += 1
    if r.get('recharge_num', 1) > 1 and r['plan_start']:
        dd = r['plan_start'].date()
        if dd in day_metrics: day_metrics[dd]['recharged'] += 1

day_dates = [str(d) for d in day_metrics.keys()]
day_installs = [day_metrics[d]['installs'] for d in day_metrics]
day_conversions = [day_metrics[d]['conversions'] for d in day_metrics]
day_active_list = [day_metrics[d]['active'] for d in day_metrics]
day_expired_list = [day_metrics[d]['expired'] for d in day_metrics]
day_recharged_list = [day_metrics[d]['recharged'] for d in day_metrics]

day_cum_installs, _c = [], 0
for n in day_installs: _c += n; day_cum_installs.append(_c)

peak_active_val = max(day_active_list) if day_active_list else 0
peak_active_idx = day_active_list.index(peak_active_val) if day_active_list else 0
peak_active_date = day_dates[peak_active_idx] if day_dates else 'N/A'
print(f"  Date range: {date_min} to {date_max} ({len(day_dates)} days)")
print(f"  Peak active paid users: {peak_active_date} ({peak_active_val})")

# =====================================================================
# SECTION 10: PLAN COHORT MOVEMENT
# =====================================================================
print("\n" + "="*70)
print("8. PLAN-WISE COHORT MOVEMENT")
print("="*70)

step_trans = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for u in users.values():
    recs = u['paid_recs']
    for i in range(len(recs) - 1):
        fp = plan_cat(recs[i]['plan_duration'])
        tp = plan_cat(recs[i+1]['plan_duration'])
        step_trans[i+1][fp][tp] += 1

overall_trans = defaultdict(lambda: defaultdict(int))
for sd in step_trans.values():
    for fp, tgts in sd.items():
        for tp, cnt in tgts.items():
            overall_trans[fp][tp] += cnt

all_plan_types = sorted(set(
    list(overall_trans.keys()) +
    [tp for tgts in overall_trans.values() for tp in tgts.keys()]
), key=lambda x: _plan_order_list.index(x) if x in _plan_order_list else 99)
if not all_plan_types: all_plan_types = _plan_order_list

# Sankey
max_sankey = min(5, max((r.get('recharge_num', 1) for r in paid_rows), default=1))
sankey_labels, sankey_node_colors, node_idx = [], [], {}
for rch in range(1, max_sankey + 1):
    for pt in all_plan_types:
        node_idx[(rch, pt)] = len(sankey_labels)
        sankey_labels.append(f"P{rch}: {pt}")
        sankey_node_colors.append(plan_colors_map.get(pt, '#888'))
sankey_src, sankey_tgt, sankey_val, sankey_link_clr = [], [], [], []
for fr in range(1, max_sankey):
    if fr not in step_trans: continue
    for fp, tgts in step_trans[fr].items():
        for tp, cnt in tgts.items():
            si, ti = node_idx.get((fr, fp)), node_idx.get((fr+1, tp))
            if si is not None and ti is not None and cnt > 0:
                sankey_src.append(si); sankey_tgt.append(ti); sankey_val.append(cnt)
                bc = plan_colors_map.get(fp, '#888')
                if bc.startswith('#') and len(bc)==7:
                    _r,_g,_b = int(bc[1:3],16),int(bc[3:5],16),int(bc[5:7],16)
                    sankey_link_clr.append(f"rgba({_r},{_g},{_b},0.4)")
                else: sankey_link_clr.append("rgba(150,150,150,0.3)")

# Stickiness
same_p, up_p, down_p, tot_tr = 0, 0, 0, 0
_p_rank = {'1-Day': 1, '2-Day': 2, '7-Day': 7, '14-Day': 14, '28-Day': 28}
for fp, tgts in overall_trans.items():
    for tp, cnt in tgts.items():
        tot_tr += cnt
        if fp == tp: same_p += cnt
        elif _p_rank.get(tp,0) > _p_rank.get(fp,0): up_p += cnt
        else: down_p += cnt
stick_pct = same_p * 100 / max(1, tot_tr)
up_pct_plan = up_p * 100 / max(1, tot_tr)
down_pct_plan = down_p * 100 / max(1, tot_tr)

# Per-recharge plan distribution
rech_plan_dist = defaultdict(Counter)
for r in paid_rows:
    rech_plan_dist[r.get('recharge_num',1)][plan_cat(r['plan_duration'])] += 1

# Journey paths (only 3+ steps)
path_counter = Counter()
for u in users.values():
    recs = u['paid_recs']
    if len(recs) < 3: continue  # skip users with fewer than 3 paid recharges
    path = " -> ".join(plan_cat(r['plan_duration']) for r in recs)
    path_counter[path] += 1
top_paths = path_counter.most_common(20)

# Journey paths (2+ steps)
path_counter_2 = Counter()
for u in users.values():
    recs = u['paid_recs']
    if len(recs) < 2: continue
    path = " -> ".join(plan_cat(r['plan_duration']) for r in recs)
    path_counter_2[path] += 1
top_paths_2 = path_counter_2.most_common(20)

print(f"  Stickiness: {stick_pct:.1f}%, Upgrades: {up_pct_plan:.1f}%, Downgrades: {down_pct_plan:.1f}%")

# =====================================================================
# SECTION 11: TRIAL FUNNEL
# =====================================================================
print("\n" + "="*70)
print("9. TRIAL FUNNEL")
print("="*70)

conv_after_expiry = [u for u in converted_users_list if u['converted_after_trial']]
conv_before_expiry = [u for u in converted_users_list if u['converted_before_trial']]
conv_after_count = len(conv_after_expiry)
conv_before_count = len(conv_before_expiry)

days_post_trial_vals = [u['days_post_trial'] for u in conv_after_expiry if u['days_post_trial'] is not None]
conv_timing_bkt = OrderedDict()
conv_timing_bkt['0-24 Hrs'] = sum(1 for d in days_post_trial_vals if 0 <= d < 1)
conv_timing_bkt['24-48 Hrs'] = sum(1 for d in days_post_trial_vals if 1 <= d < 2)
conv_timing_bkt['48-96 Hrs'] = sum(1 for d in days_post_trial_vals if 2 <= d < 4)
conv_timing_bkt['96-168 Hrs'] = sum(1 for d in days_post_trial_vals if 4 <= d <= 7)
conv_timing_bkt['168+ Hrs'] = sum(1 for d in days_post_trial_vals if d > 7)

cum_conv_x = list(range(0, min(31, max(int(max(days_post_trial_vals))+2 if days_post_trial_vals else 2, 2))))
cum_conv_y = [sum(1 for d in days_post_trial_vals if d <= day)*100/max(1,conv_after_count) for day in cum_conv_x]
median_post_trial = float(np.median(days_post_trial_vals)) if days_post_trial_vals else 0
median_post_trial_h = round(median_post_trial * 24, 1)
conv_day0_count = conv_timing_bkt.get('0-24 Hrs', 0)

# Plan by timing
timing_bucket_names = list(conv_timing_bkt.keys())
plan_by_timing = OrderedDict()
for bkt in timing_bucket_names: plan_by_timing[bkt] = Counter()
for u in conv_after_expiry:
    if not u['has_std_paid'] or u['days_post_trial'] is None: continue
    d = u['days_post_trial']
    if d < 1: bkt = '0-24 Hrs'
    elif d < 2: bkt = '24-48 Hrs'
    elif d < 4: bkt = '48-96 Hrs'
    elif d <= 7: bkt = '96-168 Hrs'
    else: bkt = '168+ Hrs'
    plan_by_timing[bkt][plan_cat(u['first_paid_dur'])] += 1

funnel_labels = ['Total Installs', 'Trial Expired (Evaluable)', 'Converted to Paid', 'Never Converted']
funnel_values = [total_users, n_evaluable, n_converted, n_never_converted]

print(f"  Evaluable: {n_evaluable}, Converted: {n_converted}, Never converted: {n_never_converted}")
print(f"  Before trial: {conv_before_count}, After trial: {conv_after_count}")

# =====================================================================
# SECTION 12: NON-CONVERTED ANALYSIS
# =====================================================================
print("\n" + "="*70)
print("10. NON-CONVERTED ANALYSIS")
print("="*70)

nc_expired = [u for u in trial_only_list if u['trial_expired']]
nc_active = [u for u in trial_only_list if u['trial_active']]
total_nc_expired = len(nc_expired)

nc_segments = OrderedDict()
nc_segments['Fresh Expired (0-3 days)'] = []
nc_segments['Warm (4-7 days)'] = []
nc_segments['Cold (8-15 days)'] = []
nc_segments['Dead (>15 days)'] = []
for u in nc_expired:
    ds = (TODAY - u['trial_expiry']).total_seconds() / 86400
    u['days_since_trial_expiry'] = ds
    if ds <= 3: nc_segments['Fresh Expired (0-3 days)'].append(u)
    elif ds <= 7: nc_segments['Warm (4-7 days)'].append(u)
    elif ds <= 15: nc_segments['Cold (8-15 days)'].append(u)
    else: nc_segments['Dead (>15 days)'].append(u)
for seg, group in nc_segments.items():
    pct = len(group)*100/max(1,total_nc_expired)
    print(f"  {seg:<30s}: {len(group):>5d} ({pct:>5.1f}%)")

fast_conv_count = sum(1 for u in conv_after_expiry if u['days_post_trial'] is not None and u['days_post_trial'] <= 1)
late_conv_count = sum(1 for u in conv_after_expiry if u['days_post_trial'] is not None and u['days_post_trial'] > 5)

install_months = Counter()
for u in users.values():
    if u['install_time']: install_months[u['install_time'].strftime('%b %Y')] += 1
_month_order = {'Jan 2026':1,'Feb 2026':2,'Mar 2026':3,'Apr 2026':4,'May 2026':5,'Jun 2026':6,'Jul 2026':7,'Aug 2026':8,'Sep 2026':9,'Oct 2026':10,'Nov 2026':11,'Dec 2026':12}
im_labels = sorted(install_months.keys(), key=lambda x: _month_order.get(x, 99))
im_values = [install_months[m] for m in im_labels]

# =====================================================================
# PLAN-WISE RECHARGE FREQUENCY DISTRIBUTION
# =====================================================================
# Plan-wise purchase frequency: for each plan type, count how many times
# each converted user purchased THAT specific plan across their lifetime
user_plan_counts = defaultdict(lambda: Counter())
for r in paid_rows:
    user_plan_counts[r['uid']][r['plan_duration']] += 1

# Total purchases per plan type (row count)
plan_total_purchases = Counter()
for r in paid_rows:
    plan_total_purchases[r['plan_duration']] += 1

plan_freq_buckets = OrderedDict()
for dur in [1, 2, 7, 14, 28]:
    # All users who purchased this plan at least once
    all_counts = []
    for uid, pcounts in user_plan_counts.items():
        cnt = pcounts.get(dur, 0)
        if cnt > 0:
            all_counts.append(cnt)
    # Cumulative "at least" buckets
    buckets = OrderedDict()
    buckets['total_purchases'] = plan_total_purchases.get(dur, 0)
    buckets['1+ times'] = sum(1 for c in all_counts if c >= 1)
    buckets['2+ times'] = sum(1 for c in all_counts if c >= 2)
    buckets['3+ times'] = sum(1 for c in all_counts if c >= 3)
    buckets['5+ times'] = sum(1 for c in all_counts if c >= 5)
    buckets['10+ times'] = sum(1 for c in all_counts if c >= 10)
    plan_freq_buckets[dur] = buckets
    tp = buckets['total_purchases']
    uu = buckets['1+ times']
    print(f"\n  {dur}-Day Plan: {tp} total purchases by {uu} unique users")
    for bkt in ['2+ times','3+ times','5+ times','10+ times']:
        cnt = buckets[bkt]
        if cnt > 0: print(f"    Users who bought {bkt}: {cnt}")

# =====================================================================
# CHURN DISTRIBUTION (Days Since Last Plan Expired)
# =====================================================================
churned_users_list = [u for u in users.values() if u['is_churned']]
churn_dist = OrderedDict()
churn_dist['0-3 days ago'] = []
churn_dist['4-7 days ago'] = []
churn_dist['8-15 days ago'] = []
churn_dist['16-30 days ago'] = []
for u in churned_users_list:
    if u['last_end']:
        ds = (TODAY - u['last_end']).total_seconds() / 86400
        u['days_since_expiry'] = ds
        if ds <= 3: churn_dist['0-3 days ago'].append(u)
        elif ds <= 7: churn_dist['4-7 days ago'].append(u)
        elif ds <= 15: churn_dist['8-15 days ago'].append(u)
        else: churn_dist['16-30 days ago'].append(u)

print(f"\n  Churned Users Distribution ({n_churned} users):")
for seg, group in churn_dist.items():
    pct = len(group)*100/max(1,n_churned)
    print(f"    {seg:<20s}: {len(group):>5d} ({pct:>5.1f}%)")

# Churn by plan type (what was their last plan)
churn_by_plan = Counter()
for u in churned_users_list:
    if u['paid_recs']:
        last_dur = u['paid_recs'][-1]['plan_duration']
        churn_by_plan[plan_cat(last_dur)] += 1
    else:
        churn_by_plan['Trial Only'] += 1

# =====================================================================
# SECTION 13: R-DAY REPORT
# =====================================================================
print("\n" + "="*70)
print("11. R-DAY POST-EXPIRY REPORT")
print("="*70)

rday_users = [u for u in trial_only_list if u['r_day'] is not None]
rday_values = [u['r_day'] for u in rday_users]
total_rday = len(rday_users)
print(f"  Non-converted with expired trial: {total_rday}")

if rday_values:
    rday_dist = Counter(rday_values)
    max_rday = max(rday_values)
    rday_table_data = OrderedDict()
    for rd in range(0, max_rday+1): rday_table_data[rd] = rday_dist.get(rd, 0)
    rday_cum_x = list(range(0, max_rday+2))
    rday_cum_y = [sum(1 for r in rday_values if r >= rd) for rd in rday_cum_x]
    rday_cum_pct = [v*100/max(1,total_rday) for v in rday_cum_y]
    rday_buckets = OrderedDict()
    rday_buckets['R0 (< 24h)'] = sum(1 for r in rday_values if r == 0)
    rday_buckets['R1-R3 (1-3 days)'] = sum(1 for r in rday_values if 1<=r<=3)
    rday_buckets['R4-R7 (4-7 days)'] = sum(1 for r in rday_values if 4<=r<=7)
    rday_buckets['R8-R15 (8-15 days)'] = sum(1 for r in rday_values if 8<=r<=15)
    rday_buckets['R16-R30 (16-30 days)'] = sum(1 for r in rday_values if 16<=r<=30)
    rday_buckets['R30+ (>30 days)'] = sum(1 for r in rday_values if r > 30)
    peak_rday = max(rday_dist, key=rday_dist.get)
    peak_rday_count = rday_dist[peak_rday]
    highest_risk_bucket = max(rday_buckets, key=rday_buckets.get)
    highest_risk_count = rday_buckets[highest_risk_bucket]
    for bkt, cnt in rday_buckets.items():
        pct = cnt*100/max(1,total_rday)
        print(f"    {bkt:<25s}: {cnt:>5d} ({pct:>5.1f}%)")
else:
    rday_dist, max_rday, rday_table_data = Counter(), 0, OrderedDict()
    rday_cum_x, rday_cum_y, rday_cum_pct = [0], [0], [0]
    rday_buckets = OrderedDict()
    for bk in ['R0 (< 24h)','R1-R3 (1-3 days)','R4-R7 (4-7 days)','R8-R15 (8-15 days)','R16-R30 (16-30 days)','R30+ (>30 days)']:
        rday_buckets[bk] = 0
    peak_rday, peak_rday_count = 0, 0
    highest_risk_bucket, highest_risk_count = 'N/A', 0

trial_insights = [
    f"1. Conversion: {conv_rate:.1f}% of evaluable ({n_converted}/{n_evaluable}). {n_never_converted} never converted.",
    f"2. {conv_day0_count} ({conv_day0_count*100/max(1,conv_after_count):.0f}%) convert within 24 hrs of trial expiry.",
    f"3. {n_before_trial} users convert BEFORE trial ends -- high intent.",
    f"4. Peak non-conversion R-day: R{peak_rday} ({peak_rday_count} users). Bucket: {highest_risk_bucket} ({highest_risk_count}).",
    f"5. Post-trial TAT: median {median_tat_h:.1f}h, P80 {p80_tat_h:.1f}h -- most convert quickly.",
]
intervention_ideas = [
    f"1. Day-2 Push: CTA on trial expiry. {conv_day0_count} already convert within 24h.",
    f"2. R3-R7 Win-back: {rday_buckets.get('R4-R7 (4-7 days)',0)} users -- offer discount before going cold.",
    f"3. Auto-renewal nudge on paid plan expiry -- {sum(1 for g in all_gaps if 0<g<=3)} renew within 72 hours.",
]

# =====================================================================
# BUILD DASHBOARD
# =====================================================================
print("\n=== BUILDING DASHBOARD ===")

# --- CHARTS ---
# 1. Install-to-paid histogram (hours)
t2p_hist_h = [min(d * 24, 720) for d in t2p]
c1 = json.dumps({"data":[{"type":"histogram","x":t2p_hist_h,"nbinsx":30,"marker":{"color":"#4ECDC4","line":{"color":"#0f0f23","width":1}}}],
    "layout":{"title":{"text":"Install to First Paid (Hours)","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Hours","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":420}})

# 2. Conversion buckets bar
cb_l = list(conv_buckets.keys()); cb_v = list(conv_buckets.values()); cb_p = [round(v*100/max(1,len(t2p)),1) for v in cb_v]
c2 = json.dumps({"data":[{"type":"bar","x":cb_l,"y":cb_v,"marker":{"color":["#27AE60","#4ECDC4","#FFEAA7","#F39C12","#E74C3C"]},
    "text":[f"{v} ({p}%)" for v,p in zip(cb_v,cb_p)],"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"Conversion Time Buckets (from Install)","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"tickfont":{"color":"#ccc"},"gridcolor":"#1a1a3e"},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":420}})

# 3. Before vs After trial (pie)
c3 = json.dumps({"data":[{"type":"pie","labels":["Before Trial Expiry","After Trial Expiry","Never Converted"],"values":[n_before_trial,n_after_trial,n_never_converted],
    "marker":{"colors":["#27AE60","#4ECDC4","#E74C3C"],"line":{"color":"#0f0f23","width":2}},"textinfo":"label+value+percent","textfont":{"size":11,"color":"white"},"hole":0.4}],
    "layout":{"title":{"text":"Conversion Timing vs Trial Expiry","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},"height":420}})

# 4. First plan pie
fd_l = list(fpd_buckets.keys()); fd_v = list(fpd_buckets.values())
c4 = json.dumps({"data":[{"type":"pie","labels":fd_l,"values":fd_v,"marker":{"colors":["#E74C3C","#9B59B6","#F39C12","#FFEAA7","#4ECDC4"],"line":{"color":"#0f0f23","width":2}},
    "textinfo":"label+value+percent","textfont":{"size":11,"color":"white"},"hole":0.4}],
    "layout":{"title":{"text":"First Paid Plan Duration","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},"height":420}})

# 5. T2P by plan (bar, hours)
cr_m = [cross_data.get(c,{}).get('median_h',0) for c in cross_cats]; cr_n = [cross_data.get(c,{}).get('n',0) for c in cross_cats]
c5 = json.dumps({"data":[{"type":"bar","x":cross_cats,"y":cr_m,"marker":{"color":["#E74C3C","#9B59B6","#F39C12","#FFEAA7","#4ECDC4"]},
    "text":[f"{m}h (n={n})" for m,n in zip(cr_m,cr_n)],"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"Median Install-to-Paid by Plan (Hours)","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Plan Duration","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Median Hours","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":420}})

# 6. Retention curve
ret_x = [n for n in retention if retention[n]['rate'] is not None]
ret_y = [retention[n]['rate'] for n in ret_x]
ret_e = [retention[n]['eligible'] for n in ret_x]
c6 = json.dumps({"data":[
    {"type":"scatter","x":ret_x,"y":ret_y,"mode":"lines+markers+text","name":"Retention %","marker":{"color":"#4ECDC4","size":10},"line":{"color":"#4ECDC4","width":3},
     "text":[f"{v}%" for v in ret_y],"textposition":"top center","textfont":{"color":"white","size":10}},
    {"type":"bar","x":ret_x,"y":ret_e,"name":"Eligible","yaxis":"y2","opacity":0.3,"marker":{"color":"#FFEAA7"}}],
    "layout":{"title":{"text":"Paid Recharge Retention (Eligible Only)","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Paid Recharge #","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"},"dtick":1},
    "yaxis":{"title":"Retention %","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"},"range":[0,105]},
    "yaxis2":{"title":"Eligible Users","overlaying":"y","side":"right","gridcolor":"rgba(0,0,0,0)","tickfont":{"color":"#FFEAA7"}},
    "legend":{"font":{"color":"#ccc"},"x":0.7,"y":0.95},"height":480}})

# 7. Stacked dropoff
st_x = [f"#{n}" for n in list(retention.keys())[:12]]
st_r = [retention[n]['retained'] for n in list(retention.keys())[:12]]
st_c = [retention[n]['eligible']-retention[n]['retained'] for n in list(retention.keys())[:12]]
st_a = [retention[n]['active'] for n in list(retention.keys())[:12]]
c7 = json.dumps({"data":[
    {"type":"bar","name":"Retained","x":st_x,"y":st_r,"marker":{"color":"#27AE60"}},
    {"type":"bar","name":"Churned","x":st_x,"y":st_c,"marker":{"color":"#E74C3C"}},
    {"type":"bar","name":"Active (plan running)","x":st_x,"y":st_a,"marker":{"color":"#3498DB"}}],
    "layout":{"title":{"text":"User Status at Each Paid Recharge Step","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "barmode":"stack","height":460,"xaxis":{"tickfont":{"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"legend":{"font":{"color":"#ccc"}}}})

# 8. Plan-wise gap: Median/P80/P90 grouped bar (hours)
_pg_labels = ['1-Day', '2-Day', '7-Day', '14-Day', '28-Day']
_pg_med = [plan_gap_stats[d]['median_h'] for d in [1,2,7,14,28]]
_pg_p80 = [plan_gap_stats[d]['p80_h'] for d in [1,2,7,14,28]]
_pg_p90 = [plan_gap_stats[d]['p90_h'] for d in [1,2,7,14,28]]
_pg_cnt = [plan_gap_stats[d]['count'] for d in [1,2,7,14,28]]
c8 = json.dumps({"data":[
    {"type":"bar","name":"Median","x":_pg_labels,"y":_pg_med,"marker":{"color":"#4ECDC4"},
     "text":[f"{v}h" for v in _pg_med],"textposition":"outside","textfont":{"color":"white","size":11}},
    {"type":"bar","name":"P80","x":_pg_labels,"y":_pg_p80,"marker":{"color":"#FFEAA7"},
     "text":[f"{v}h" for v in _pg_p80],"textposition":"outside","textfont":{"color":"white","size":11}},
    {"type":"bar","name":"P90","x":_pg_labels,"y":_pg_p90,"marker":{"color":"#F39C12"},
     "text":[f"{v}h" for v in _pg_p90],"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"Gap to Next Recharge by Plan Type (Hours)","font":{"size":18,"color":"white"}},"barmode":"group",
    "paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Plan Duration","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Hours","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},
    "height":460,"legend":{"font":{"color":"#ccc"}}}})

# 9. Plan-wise gap distribution (overlaid histograms, hours)
_gap_hist_data = []
_gap_colors = {1:'#E74C3C',2:'#9B59B6',7:'#F39C12',14:'#FFEAA7',28:'#4ECDC4'}
for dur in [1,2,7,14,28]:
    gh = [g*24 for g in plan_wise_gaps.get(dur,[])]
    if gh:
        _gap_hist_data.append({"type":"histogram","x":[min(h,720) for h in gh],"name":f"{dur}-Day Plan",
            "opacity":0.6,"nbinsx":25,"marker":{"color":_gap_colors[dur]}})
c9 = json.dumps({"data":_gap_hist_data,
    "layout":{"title":{"text":"Gap Distribution by Plan Type (Hours)","font":{"size":18,"color":"white"}},"barmode":"overlay",
    "paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Hours","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Count","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},
    "height":420,"legend":{"font":{"color":"#ccc"}}}})

# 10. Plan-wise gap table chart (count per plan)
c10 = json.dumps({"data":[{"type":"bar","x":_pg_labels,"y":_pg_cnt,"marker":{"color":["#E74C3C","#9B59B6","#F39C12","#FFEAA7","#4ECDC4"]},
    "text":[f"n={n}" for n in _pg_cnt],"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"Recharge Gap Samples by Plan Type","font":{"size":18,"color":"white"}},
    "paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Plan Duration","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Gap Count","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":400}})

# 11. Segment pie
sn = list(seg_profiles.keys()); sc = [seg_profiles[s]['count'] for s in sn]
c11 = json.dumps({"data":[{"type":"pie","labels":sn,"values":sc,"marker":{"colors":["#27AE60","#FFEAA7","#E74C3C","#3498DB","#FF6B6B","#BB8FCE"][:len(sn)],"line":{"color":"#0f0f23","width":2}},
    "textinfo":"label+value+percent","textfont":{"size":10,"color":"white"},"hole":0.4}],
    "layout":{"title":{"text":"User Segments","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},"height":420,"legend":{"font":{"color":"#ccc","size":9}}}})

# 12. Segment compare
c12 = json.dumps({"data":[
    {"type":"bar","name":"Avg Plan Dur","x":sn,"y":[seg_profiles[s]['avg_duration'] for s in sn],"marker":{"color":"#4ECDC4"}},
    {"type":"bar","name":"Med Gap","x":sn,"y":[seg_profiles[s]['median_gap'] for s in sn],"marker":{"color":"#FFEAA7"}},
    {"type":"bar","name":"Avg Lifetime","x":sn,"y":[seg_profiles[s]['avg_lifetime'] for s in sn],"marker":{"color":"#BB8FCE"}}],
    "layout":{"title":{"text":"Segment Comparison","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "barmode":"group","height":460,"xaxis":{"tickfont":{"size":8,"color":"#ccc"}},"yaxis":{"title":"Days","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"legend":{"font":{"color":"#ccc"}}}})

# 14. Daily installs + active
c14 = json.dumps({"data":[
    {"type":"bar","x":day_dates,"y":day_installs,"name":"Installs","marker":{"color":"#4ECDC4"},"yaxis":"y"},
    {"type":"scatter","x":day_dates,"y":day_active_list,"name":"Active Paid","mode":"lines","line":{"color":"#27AE60","width":2},"yaxis":"y2"},
    {"type":"scatter","x":day_dates,"y":day_cum_installs,"name":"Cum Installs","mode":"lines","line":{"color":"#F39C12","width":2,"dash":"dot"},"yaxis":"y"}],
    "layout":{"title":{"text":"Daily Installs & Active Paid Users","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"tickangle":-45,"tickfont":{"size":9,"color":"#ccc"},"gridcolor":"#1a1a3e"},
    "yaxis":{"title":"Install/Cum","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},
    "yaxis2":{"title":"Active","overlaying":"y","side":"right","tickfont":{"color":"#27AE60"}},
    "legend":{"font":{"color":"#ccc"},"x":0.01,"y":0.99},"height":480,"margin":{"b":100}}})

# 15. Daily expired + recharged
c15 = json.dumps({"data":[
    {"type":"bar","x":day_dates,"y":day_expired_list,"name":"Plans Expired","marker":{"color":"rgba(231,76,60,0.7)"}},
    {"type":"bar","x":day_dates,"y":day_recharged_list,"name":"Recharged","marker":{"color":"rgba(39,174,96,0.7)"}}],
    "layout":{"title":{"text":"Daily Plan Expirations & Re-Recharges","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "barmode":"group","xaxis":{"tickangle":-45,"tickfont":{"size":9,"color":"#ccc"}},"yaxis":{"title":"Count","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"legend":{"font":{"color":"#ccc"}},"height":480,"margin":{"b":100}}})

# 16. Sankey
c16 = json.dumps({"data":[{"type":"sankey","node":{"label":sankey_labels,"color":sankey_node_colors,"pad":20,"thickness":20,"line":{"color":"#0f0f23","width":1}},
    "link":{"source":sankey_src,"target":sankey_tgt,"value":sankey_val,"color":sankey_link_clr}}],
    "layout":{"title":{"text":"Plan Duration Flow (Paid Recharges)","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white","size":10},"height":550,"margin":{"l":20,"r":20}}})

# 17. Transition heatmap
trans_z = [[overall_trans[fp].get(tp,0) for tp in all_plan_types] for fp in all_plan_types]
c17 = json.dumps({"data":[{"type":"heatmap","z":trans_z,"x":all_plan_types,"y":all_plan_types,
    "colorscale":[[0,"#0f0f23"],[1,"#4ECDC4"]],"text":trans_z,"texttemplate":"%{text}","textfont":{"size":12,"color":"white"}}],
    "layout":{"title":{"text":"Plan Transition Matrix","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"To Plan","tickfont":{"color":"#ccc"}},"yaxis":{"title":"From Plan","tickfont":{"color":"#ccc"},"autorange":"reversed"},"height":420}})

# 18. Plan dist per recharge
max_dist = min(10, max(rech_plan_dist.keys()) if rech_plan_dist else 1)
dist_x = [f"P#{n}" for n in range(1, max_dist+1)]
c18 = json.dumps({"data":[{"type":"bar","name":pt,"x":dist_x,"y":[rech_plan_dist[n].get(pt,0) for n in range(1,max_dist+1)],
    "marker":{"color":plan_colors_map.get(pt,'#888')}} for pt in all_plan_types],
    "layout":{"title":{"text":"Plan Type by Paid Recharge #","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "barmode":"stack","height":420,"xaxis":{"tickfont":{"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"legend":{"font":{"color":"#ccc"}}}})

# 19. Trial funnel
c19 = json.dumps({"data":[{"type":"funnel","y":funnel_labels,"x":funnel_values,"textinfo":"value+percent initial","textfont":{"color":"white","size":12},
    "marker":{"color":["#4ECDC4","#FFEAA7","#27AE60","#E74C3C"]},"connector":{"line":{"color":"#2a2a5e","width":1}}}],
    "layout":{"title":{"text":f"Trial Funnel","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},"height":420,"margin":{"l":220}}})

# 20. Conv timing
ct_l=list(conv_timing_bkt.keys()); ct_v=list(conv_timing_bkt.values()); ct_p=[v*100/max(1,conv_after_count) for v in ct_v]
c20 = json.dumps({"data":[{"type":"bar","x":ct_l,"y":ct_v,"marker":{"color":["#27AE60","#4ECDC4","#FFEAA7","#F39C12","#E74C3C"]},
    "text":[f"{v} ({p:.0f}%)" for v,p in zip(ct_v,ct_p)],"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"Post-Trial Conversion Timing","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"tickfont":{"size":9,"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":420}})

# 21. Cum conv
c21 = json.dumps({"data":[{"type":"scatter","x":cum_conv_x,"y":cum_conv_y,"mode":"lines+markers","fill":"tozeroy",
    "line":{"color":"#4ECDC4","width":2},"fillcolor":"rgba(78,205,196,0.15)","marker":{"size":4,"color":"#4ECDC4"}}],
    "layout":{"title":{"text":"Cumulative Conversion After Trial Expiry","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Days After Expiry","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"},"dtick":1},"yaxis":{"title":"% Converted","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"},"range":[0,105]},"height":420}})

# 22. Plan by timing
pt_traces = [{"type":"bar","name":pt,"x":timing_bucket_names,"y":[plan_by_timing[bkt].get(pt,0) for bkt in timing_bucket_names],
    "marker":{"color":plan_colors_map.get(pt,'#888')}} for pt in _plan_order_list]
c22 = json.dumps({"data":pt_traces,"layout":{"title":{"text":"Plan by Conversion Timing","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "barmode":"stack","height":420,"xaxis":{"tickfont":{"size":9,"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"legend":{"font":{"color":"#ccc"}}}})

# 23. Install month (fix: use category axis to prevent Plotly date auto-detect)
c23 = json.dumps({"data":[{"type":"bar","x":im_labels,"y":im_values,"marker":{"color":"#BB8FCE"},"text":im_values,"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"Install Month","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"type":"category","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":400}})

# 24-26. R-Day charts
if rday_values:
    rbl=sorted(rday_dist.keys()); rby=[rday_dist[rd] for rd in rbl]; rbla=[f"R{rd}" for rd in rbl]
else: rbl,rby,rbla=[0],[0],["R0"]
c24 = json.dumps({"data":[{"type":"bar","x":rbla,"y":rby,"marker":{"color":"#FF6B6B"},"text":rby,"textposition":"outside","textfont":{"color":"white","size":9}}],
    "layout":{"title":{"text":"R-Day Distribution (Non-Converted)","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"R-Day","tickfont":{"size":9,"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":460}})

c25 = json.dumps({"data":[{"type":"scatter","x":[f"R{d}" for d in rday_cum_x],"y":rday_cum_pct,"mode":"lines+markers","fill":"tozeroy",
    "line":{"color":"#E74C3C","width":2},"fillcolor":"rgba(231,76,60,0.15)","marker":{"size":4,"color":"#E74C3C"}}],
    "layout":{"title":{"text":"Cumulative Non-Converted by R-Day","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"R-Day","tickfont":{"size":9,"color":"#ccc"}},"yaxis":{"title":"% Still Not Converted","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"},"range":[0,105]},"height":420}})

rbl2=list(rday_buckets.keys()); rbv2=list(rday_buckets.values()); rbp2=[v*100/max(1,total_rday) for v in rbv2]
c26 = json.dumps({"data":[{"type":"bar","x":rbl2,"y":rbv2,"marker":{"color":["#4ECDC4","#27AE60","#FFEAA7","#F39C12","#E74C3C","#8B0000"]},
    "text":[f"{v} ({p:.0f}%)" for v,p in zip(rbv2,rbp2)],"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"R-Day Risk Buckets","font":{"size":18,"color":"white"}},"paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"tickangle":-15,"tickfont":{"size":9,"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":420,"margin":{"b":80}}})

# 27. Plan-wise recharge frequency (grouped bar)
_freq_bkt_names = ['1+ times','2+ times','3+ times','5+ times','10+ times']
_freq_colors = ['#3498DB','#4ECDC4','#FFEAA7','#F39C12','#E74C3C']
_plan_labels_27 = ['1-Day','2-Day','7-Day','14-Day','28-Day']
# First trace: total purchases
_tp_vals = [plan_freq_buckets[d].get('total_purchases', 0) for d in [1,2,7,14,28]]
_freq_traces = [{"type":"bar","name":"Total Purchases","x":_plan_labels_27,"y":_tp_vals,
    "marker":{"color":"#BB8FCE"},"text":[str(v) for v in _tp_vals],
    "textposition":"outside","textfont":{"color":"white","size":10}}]
for i, bkt in enumerate(_freq_bkt_names):
    vals = [plan_freq_buckets[d].get(bkt, 0) for d in [1,2,7,14,28]]
    _freq_traces.append({"type":"bar","name":bkt,"x":_plan_labels_27,"y":vals,
        "marker":{"color":_freq_colors[i]},"text":[str(v) if v > 0 else '' for v in vals],
        "textposition":"outside","textfont":{"color":"white","size":10}})
c27 = json.dumps({"data":_freq_traces,
    "layout":{"title":{"text":"Plan-Wise Purchase Frequency","font":{"size":18,"color":"white"}},"barmode":"group",
    "paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"title":"Plan Type","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Count","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},
    "height":460,"legend":{"font":{"color":"#ccc"}}}})

# 28. Churn distribution by days since expiry
_cd_labels = list(churn_dist.keys()); _cd_vals = [len(g) for g in churn_dist.values()]
_cd_pcts = [v*100/max(1,n_churned) for v in _cd_vals]
c28 = json.dumps({"data":[{"type":"bar","x":_cd_labels,"y":_cd_vals,
    "marker":{"color":["#FFEAA7","#F39C12","#E74C3C","#8B0000"]},
    "text":[f"{v} ({p:.0f}%)" for v,p in zip(_cd_vals,_cd_pcts)],"textposition":"outside","textfont":{"color":"white","size":11}}],
    "layout":{"title":{"text":"Churned Users by Days Since Plan Expired","font":{"size":18,"color":"white"}},
    "paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},
    "xaxis":{"type":"category","tickfont":{"color":"#ccc"}},"yaxis":{"title":"Users","gridcolor":"#1a1a3e","tickfont":{"color":"#ccc"}},"height":420}})

# 29. Churn by last plan type
_cbp_labels = [plan_cat(d) for d in [1,2,7,14,28]]
_cbp_vals = [churn_by_plan.get(p,0) for p in _cbp_labels]
c29 = json.dumps({"data":[{"type":"pie","labels":_cbp_labels,"values":_cbp_vals,
    "marker":{"colors":["#E74C3C","#9B59B6","#F39C12","#FFEAA7","#4ECDC4"],"line":{"color":"#0f0f23","width":2}},
    "textinfo":"label+value+percent","textfont":{"size":11,"color":"white"},"hole":0.4}],
    "layout":{"title":{"text":"Churned Users by Last Plan Type","font":{"size":18,"color":"white"}},
    "paper_bgcolor":"#0f0f23","plot_bgcolor":"#0f0f23","font":{"color":"white"},"height":420}})

ALL_CHARTS = json.dumps({
    "c2":json.loads(c2),
    "c4":json.loads(c4),"c5":json.loads(c5),
    "c6":json.loads(c6),"c7":json.loads(c7),
    "c8":json.loads(c8),"c9":json.loads(c9),"c10":json.loads(c10),
    "c11":json.loads(c11),"c12":json.loads(c12),
    "c14":json.loads(c14),"c15":json.loads(c15),
    "c16":json.loads(c16),"c17":json.loads(c17),"c18":json.loads(c18),
    "c19":json.loads(c19),"c20":json.loads(c20),"c21":json.loads(c21),"c22":json.loads(c22),
    "c23":json.loads(c23),
    "c24":json.loads(c24),"c25":json.loads(c25),"c26":json.loads(c26),
    "c27":json.loads(c27),"c28":json.loads(c28),"c29":json.loads(c29),
})

# --- TABLES ---
ret_tbl = ""
for n in list(retention.keys())[:12]:
    r = retention[n]
    rate_str = f"{r['rate']}%" if r['rate'] is not None else "N/A"
    color = "#27AE60" if (r['rate'] or 0)>=70 else "#F39C12" if (r['rate'] or 0)>=40 else "#E74C3C"
    ch = r['eligible']-r['retained']
    ret_tbl += f"<tr><td>Paid #{n}</td><td>{r['total']}</td><td><b>{r['eligible']}</b></td><td>{r['retained']}</td><td style='color:{color}'><b>{rate_str}</b></td><td style='color:#3498DB'>{r['active']}</td><td style='color:#E74C3C'>{ch}</td></tr>"

seg_tbl = ""
for seg in seg_order:
    p = seg_profiles.get(seg, {});
    if not p: continue
    seg_tbl += f"<tr><td><b>{seg}</b></td><td>{p['count']}</td><td>{p['pct']}%</td><td>{p['avg_duration']}d</td><td>{p['median_gap']}d</td><td>{p['avg_lifetime']}d</td><td>{p['avg_ltv_days']}d</td><td style='color:#E74C3C'>{p['churn_pct']}%</td></tr>"

# Plan-wise gap table for HTML
plan_gap_tbl = ""
for dur in [1, 2, 7, 14, 28]:
    s = plan_gap_stats[dur]
    plan_gap_tbl += f"<tr><td><b>{dur}-Day</b></td><td>{s['count']}</td><td><b>{s['median_h']}h</b></td><td>{s['p80_h']}h</td><td>{s['p90_h']}h</td><td>{s['mean_h']}h</td></tr>"

# Plan frequency table
plan_freq_tbl = ""
for dur in [1, 2, 7, 14, 28]:
    bkts = plan_freq_buckets[dur]
    tp = bkts.get('total_purchases', 0)
    uu = bkts.get('1+ times', 0)
    cells = "".join(f"<td>{bkts.get(b,0)}</td>" for b in _freq_bkt_names)
    plan_freq_tbl += f"<tr><td><b>{dur}-Day</b></td><td><b>{tp}</b></td><td>{uu}</td>{cells}</tr>"

# Churn distribution table
churn_dist_tbl = ""
for seg, group in churn_dist.items():
    cn = len(group)
    pn = cn*100/max(1,n_churned)
    # Show avg last plan duration for each bucket
    avg_dur = round(float(np.mean([u['avg_duration'] for u in group if u['avg_duration'] > 0])),1) if [u for u in group if u['avg_duration']>0] else 0
    churn_dist_tbl += f"<tr><td><b>{seg}</b></td><td>{cn}</td><td>{pn:.1f}%</td><td>{avg_dur}d</td></tr>"

insights_html = "".join(f'<div class="hyp" style="border-color:#4ECDC4"><span style="color:#ccc">{i}</span></div>' for i in insights)
churn_html = "".join(f'<div class="hyp" style="border-color:#E74C3C"><span style="color:#ccc">{r}</span></div>' for r in churn_risks)
rec_html = "".join(f'<div class="hyp" style="border-color:#27AE60"><span style="color:#ccc">{r}</span></div>' for r in recommendations)
exp_html = "".join(f'<div class="hyp" style="border-color:#FFEAA7"><span style="color:#ccc">{e}</span></div>' for e in experiments)

daily_tbl1 = ""
_ci,_cc = 0,0
for dk in day_metrics:
    m=day_metrics[dk]; _ci+=m['installs']; _cc+=m['conversions']
    daily_tbl1 += f"<tr><td>{dk}</td><td>{m['installs']}</td><td>{_ci}</td><td>{m['conversions']}</td><td>{_cc}</td><td><b>{m['active']}</b></td></tr>"

daily_tbl2 = ""
for dk in day_metrics:
    m=day_metrics[dk]; r=m['recharged']*100/max(1,m['expired']) if m['expired']>0 else 0; rs=f"{r:.0f}%" if m['expired']>0 else "-"
    clr="#27AE60" if r>=70 else "#F39C12" if r>=40 else "#E74C3C"
    daily_tbl2 += f"<tr><td>{dk}</td><td style='color:#E74C3C'>{m['expired']}</td><td style='color:#27AE60'>{m['recharged']}</td><td style='color:{clr}'><b>{rs}</b></td></tr>"

trans_hdr = "<th>From \\ To</th>"+"".join(f"<th>{tp}</th>" for tp in all_plan_types)+"<th>Total</th>"
trans_tbl = ""
for fp in all_plan_types:
    rt=sum(overall_trans[fp].values()); cells=""
    for tp in all_plan_types:
        cnt=overall_trans[fp][tp]; pct=cnt*100/max(1,rt)
        bg=f"rgba(78,205,196,{min(pct/100*2,0.6):.2f})" if cnt>0 else "transparent"
        cells += f"<td style='background:{bg}'>{cnt} <small>({pct:.0f}%)</small></td>"
    trans_tbl += f"<tr><td><b>{fp}</b></td>{cells}<td><b>{rt}</b></td></tr>"

paths_tbl = ""
for rank,(path,cnt) in enumerate(top_paths,1):
    steps=path.count("->")+1; pp=cnt*100/max(1,sum(path_counter.values()))
    paths_tbl += f"<tr><td>{rank}</td><td style='font-family:monospace;white-space:nowrap'>{path}</td><td>{steps}</td><td><b>{cnt}</b></td><td>{pp:.1f}%</td></tr>"

paths_tbl_2 = ""
total_2plus_all = sum(path_counter_2.values())
total_3plus_all = sum(path_counter.values())
for rank,(path,cnt) in enumerate(top_paths_2,1):
    steps=path.count("->")+1; pp=cnt*100/max(1,total_2plus_all)
    paths_tbl_2 += f"<tr><td>{rank}</td><td style='font-family:monospace;white-space:nowrap'>{path}</td><td>{steps}</td><td><b>{cnt}</b></td><td>{pp:.1f}%</td></tr>"

trial_funnel_tbl = "".join(f"<tr><td>{l}</td><td><b>{v}</b></td><td>{v*100/max(1,total_users):.1f}%</td></tr>" for l,v in zip(funnel_labels,funnel_values))

conv_timing_tbl = ""
_cum_ct = 0
for bkt,cnt in conv_timing_bkt.items():
    pct_ct=cnt*100/max(1,conv_after_count); _cum_ct+=cnt; cum_pct=_cum_ct*100/max(1,conv_after_count)
    conv_timing_tbl += f"<tr><td>{bkt}</td><td>{cnt}</td><td>{pct_ct:.1f}%</td><td>{_cum_ct}</td><td>{cum_pct:.1f}%</td></tr>"

nc_seg_tbl = ""
for seg,group in nc_segments.items():
    cn=len(group); pn=cn*100/max(1,total_nc_expired)
    nc_seg_tbl += f"<tr><td>{seg}</td><td><b>{cn}</b></td><td>{pn:.1f}%</td></tr>"

rday_detail_tbl = ""
_cr = 0
for rd in sorted(rday_table_data.keys()):
    cnt=rday_table_data[rd]; _cr+=cnt; pr=cnt*100/max(1,total_rday); cpr=_cr*100/max(1,total_rday)
    clr="#27AE60" if rd<=3 else "#FFEAA7" if rd<=7 else "#F39C12" if rd<=15 else "#E74C3C"
    rday_detail_tbl += f"<tr><td style='color:{clr}'><b>R{rd}</b></td><td>{cnt}</td><td>{pr:.1f}%</td><td>{_cr}</td><td>{cpr:.1f}%</td></tr>"

ti_html = "".join(f'<div class="hyp" style="border-color:#4ECDC4"><span style="color:#ccc">{t}</span></div>' for t in trial_insights)
iv_html = "".join(f'<div class="hyp" style="border-color:#FF6B6B"><span style="color:#ccc">{t}</span></div>' for t in intervention_ideas)

# --- HTML ---
html = f'''<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>WIOM Recharge Lifecycle Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',sans-serif;background:#0a0a1a;color:#e0e0e0}}
.hdr{{background:linear-gradient(135deg,#0f0f23,#1a1a4e,#0f0f23);padding:24px 36px;border-bottom:1px solid #2a2a5e;position:sticky;top:0;z-index:100}}
.hdr h1{{font-size:22px;background:linear-gradient(90deg,#4ECDC4,#FF6B6B);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;font-weight:800}}
.hdr p{{font-size:12px;color:#888;margin-top:4px}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px;padding:18px 36px}}
.kpi{{background:linear-gradient(145deg,#14142e,#1a1a3e);border:1px solid #2a2a5e;border-radius:12px;padding:16px;text-align:center;transition:transform .2s}}
.kpi:hover{{transform:translateY(-3px);box-shadow:0 6px 24px rgba(78,205,196,.12)}}
.kpi .v{{font-size:26px;font-weight:800}}.kpi .l{{font-size:10px;color:#888;text-transform:uppercase;letter-spacing:1px}}
.tabs{{display:flex;padding:0 36px;border-bottom:1px solid #2a2a5e;overflow-x:auto;gap:0}}
.tab{{padding:11px 18px;cursor:pointer;font-size:12px;font-weight:600;color:#555;border-bottom:3px solid transparent;transition:.2s;white-space:nowrap}}
.tab:hover{{color:#aaa}}.tab.active{{color:#4ECDC4;border-bottom-color:#4ECDC4}}
.tc{{display:none;padding:18px 36px}}.tc.active{{display:block}}
.box{{background:#0f0f23;border:1px solid #1a1a3e;border-radius:12px;padding:14px;margin-bottom:16px}}
.ins{{background:linear-gradient(145deg,#14142e,#1e1e4a);border-left:4px solid #4ECDC4;border-radius:0 10px 10px 0;padding:14px 18px;margin:12px 0;font-size:13px;line-height:1.7}}
.ins b{{color:#4ECDC4}}.ins .r{{color:#FF6B6B}}.ins .g{{color:#2ECC71}}.ins .y{{color:#F1C40F}}
table{{width:100%;border-collapse:collapse;margin:8px 0;font-size:12px}}
th{{background:#14142e;color:#4ECDC4;padding:9px 10px;text-align:left;border-bottom:2px solid #2a2a5e;font-size:10px;text-transform:uppercase;letter-spacing:.5px}}
td{{padding:7px 10px;border-bottom:1px solid #1a1a3e}}tr:hover{{background:#14142e}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:900px){{.g2{{grid-template-columns:1fr}}}}
.badge{{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;margin:2px}}
.badge-r{{background:rgba(231,76,60,.2);color:#E74C3C}}.badge-g{{background:rgba(39,174,96,.2);color:#27AE60}}
.badge-y{{background:rgba(241,196,15,.2);color:#F1C40F}}.badge-b{{background:rgba(52,152,219,.2);color:#3498DB}}
.hyp{{margin-bottom:10px;padding:12px;background:#1a1a3e;border-radius:8px;border-left:3px solid;font-size:13px;line-height:1.6}}
.stat-row{{display:flex;gap:16px;margin:8px 0;flex-wrap:wrap}}
.stat-card{{background:#14142e;border:1px solid #2a2a5e;border-radius:8px;padding:12px 18px;text-align:center;flex:1;min-width:120px}}
.stat-card .sv{{font-size:22px;font-weight:800;color:#4ECDC4}}.stat-card .sl{{font-size:10px;color:#888;text-transform:uppercase}}
footer{{text-align:center;padding:24px;color:#333;font-size:11px;border-top:1px solid #1a1a3e;margin-top:30px}}
</style></head><body>

<div class="hdr" style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap">
<div><h1>WIOM Recharge Lifecycle Analytics</h1>
<p>{total_users} users | {n_converted} converted, {n_never_converted} never converted, {n_trial_active} trial active | {len(paid_rows)} paid recharge records</p></div>
<div style="text-align:right">
<div id="update-time" style="font-size:11px;color:#4ECDC4;margin-bottom:4px">Last Updated: {NOW_STR} IST</div>
<button onclick="refreshDashboard(this)"
style="background:linear-gradient(135deg,#4ECDC4,#27AE60);color:#0a0a1a;padding:6px 16px;border-radius:6px;font-size:11px;font-weight:700;border:none;cursor:pointer">Refresh Data</button>
<script>
function refreshDashboard(btn){{
  btn.disabled=true;
  btn.textContent='Refreshing...';
  btn.style.opacity='0.7';
  document.getElementById('update-time').innerHTML='<span style="color:#FF6B6B">Connecting to Metabase... please wait ~45s</span>';
  fetch('/refresh').then(function(){{
    setTimeout(function(){{ location.reload(); }},45000);
  }}).catch(function(){{
    setTimeout(function(){{ location.reload(); }},45000);
  }});
}}
</script>
</div></div>

<div class="kpis">
<div class="kpi"><div class="v" style="color:#4ECDC4">{total_users}</div><div class="l">Total Users</div></div>
<div class="kpi"><div class="v" style="color:#27AE60">{conv_rate:.1f}%</div><div class="l">Conversion Rate</div></div>
<div class="kpi"><div class="v" style="color:#FFEAA7">{median_t2p_h:.1f}h</div><div class="l">Median Install-to-Paid</div></div>
<div class="kpi"><div class="v" style="color:#FF6B6B">{n_never_converted}</div><div class="l">Never Converted</div></div>
<div class="kpi"><div class="v" style="color:#BB8FCE">{r1_to_r2_rate:.0f}%</div><div class="l">Paid R1-R2 Retention</div></div>
<div class="kpi"><div class="v" style="color:#E74C3C">{pct_28d:.0f}%</div><div class="l">Choose 28-Day Plan</div></div>
<div class="kpi"><div class="v" style="color:#DDA0DD">{power_pct:.0f}%</div><div class="l">Power Users (4+)</div></div>
</div>

<div class="tabs" id="tb">
<div class="tab active" data-t="conv">1. Conversion</div>
<div class="tab" data-t="firstplan">2. First Plan</div>
<div class="tab" data-t="retention">3. Retention</div>
<div class="tab" data-t="gaps">4. Gap Analysis</div>
<div class="tab" data-t="segments">5. Segments</div>
<div class="tab" data-t="insights">6. Insights</div>
<div class="tab" data-t="daily">7. Daily Metrics</div>
<div class="tab" data-t="cohort">8. Plan Cohort</div>
<div class="tab" data-t="trialfunnel">9. Trial Funnel</div>
<div class="tab" data-t="nonconv">10. Non-Converted</div>
<div class="tab" data-t="rday">11. R-Day Report</div>
</div>

<div class="tc active" id="t-conv">
<div class="ins"><b>Trial-to-Paid Conversion:</b> <b class="g">{conv_rate:.1f}%</b> of {n_evaluable} evaluable users converted.
<b class="r">{n_never_converted}</b> never converted (trial expired).
<span class="badge badge-b">{n_trial_active} still in trial (excluded from rate)</span></div>
<div class="stat-row">
<div class="stat-card"><div class="sv" style="color:#4ECDC4">{total_users}</div><div class="sl">Total Installed</div></div>
<div class="stat-card"><div class="sv" style="color:#27AE60">{n_converted}</div><div class="sl">Converted to Paid</div></div>
<div class="stat-card"><div class="sv" style="color:#E74C3C">{n_never_converted}</div><div class="sl">Never Converted</div></div>
<div class="stat-card"><div class="sv" style="color:#FFEAA7">{n_trial_active}</div><div class="sl">Still in Trial</div></div>
<div class="stat-card"><div class="sv" style="color:#BB8FCE">{n_evaluable}</div><div class="sl">Evaluable (Trial Expired)</div></div>
</div>
<div class="box"><div id="c-c2"></div></div>
<div class="box" style="max-height:500px;overflow-y:auto"><h4 style="color:#4ECDC4;margin-bottom:8px;font-size:14px">Month-Wise Installs &amp; Conversion (Newest First)</h4>
<p style="color:#888;font-size:11px;margin-bottom:8px">Conversion rate excludes users still in trial period</p>
<table><tr><th>Month</th><th>Installs</th><th>Converted</th><th>Never Converted</th><th>In Trial</th><th>Evaluable</th><th>Conversion Rate</th></tr>{month_tbl}</table></div>
</div>

<div class="tc" id="t-firstplan">
<div class="ins"><b>First Paid Plan:</b> <b class="g">{pct_28d:.0f}%</b> choose 28-day. <b class="r">{pct_1d:.0f}%</b> start with 1-day.
Based on {n_std} users with standard plans (1/2/7/14/28 day).</div>
<div class="g2"><div class="box"><div id="c-c4"></div></div><div class="box"><div id="c-c5"></div></div></div>
</div>

<div class="tc" id="t-retention">
<div class="ins"><b>Paid Recharge Retention (Eligibility-Based):</b> Only expired-plan users evaluated.
Paid R1 to R2: <b class="g">{r1_to_r2_rate:.0f}%</b> ({r1_elig} eligible, {r1_ret} retained).
<span class="badge badge-b">{r1_act} active (plan running)</span></div>
<div class="g2"><div class="box"><div id="c-c6"></div></div><div class="box"><div id="c-c7"></div></div></div>
<div class="box"><table><tr><th>Step</th><th>Total</th><th>Eligible</th><th>Retained</th><th>Rate</th><th>Active</th><th>Churned</th></tr>{ret_tbl}</table></div>
</div>

<div class="tc" id="t-gaps">
<div class="ins"><b>Plan-Wise Gap Analysis:</b> Overall median gap: <b>{median_gap_h:.1f} hours</b>.
Gap = time from plan expiry to next recharge, grouped by plan duration.</div>
<div class="stat-row">
<div class="stat-card"><div class="sv" style="color:#E74C3C">{plan_gap_stats[1]['median_h']}h</div><div class="sl">1-Day Median Gap</div></div>
<div class="stat-card"><div class="sv" style="color:#9B59B6">{plan_gap_stats[2]['median_h']}h</div><div class="sl">2-Day Median Gap</div></div>
<div class="stat-card"><div class="sv" style="color:#F39C12">{plan_gap_stats[7]['median_h']}h</div><div class="sl">7-Day Median Gap</div></div>
<div class="stat-card"><div class="sv" style="color:#FFEAA7">{plan_gap_stats[14]['median_h']}h</div><div class="sl">14-Day Median Gap</div></div>
<div class="stat-card"><div class="sv" style="color:#4ECDC4">{plan_gap_stats[28]['median_h']}h</div><div class="sl">28-Day Median Gap</div></div>
</div>
<div class="g2"><div class="box"><div id="c-c8"></div></div><div class="box"><div id="c-c9"></div></div></div>
<div class="box"><table><tr><th>Plan</th><th>Gaps</th><th>Median</th><th>P80</th><th>P90</th><th>Mean</th></tr>{plan_gap_tbl}</table></div>
</div>

<div class="tc" id="t-segments">
<div class="ins"><b>User Segments:</b> <b class="r">{n_churned}</b> churned (plan expired, not recharged till date).</div>
<div class="g2"><div class="box"><div id="c-c11"></div></div><div class="box"><div id="c-c12"></div></div></div>
<div class="box"><table><tr><th>Segment</th><th>Users</th><th>%</th><th>Avg Duration</th><th>Med Gap</th><th>Avg Lifetime</th><th>Avg LTV</th><th>Churn%</th></tr>{seg_tbl}</table></div>
<h4 style="color:#E74C3C;margin:16px 0 8px;font-size:15px">Churned Users Distribution ({n_churned} users)</h4>
<div class="ins" style="border-color:#E74C3C"><b style="color:#E74C3C">Churned = Plan expired & not recharged till date.</b> Distribution by days since last plan expired:</div>
<div class="g2"><div class="box"><div id="c-c28"></div></div><div class="box"><div id="c-c29"></div></div></div>
<div class="box"><table><tr><th>Days Since Expired</th><th>Users</th><th>%</th><th>Avg Plan Duration</th></tr>{churn_dist_tbl}</table></div>
</div>

<div class="tc" id="t-insights">
<div class="ins"><b>Data-Driven Insights</b></div>
<div class="box" style="padding:20px"><h3 style="color:#4ECDC4;margin-bottom:14px;font-size:15px">Top Insights</h3>{insights_html}</div>
<div class="box" style="padding:20px"><h3 style="color:#E74C3C;margin-bottom:14px;font-size:15px">Churn Risks</h3>{churn_html}</div>
<div class="box" style="padding:20px"><h3 style="color:#27AE60;margin-bottom:14px;font-size:15px">Recommendations</h3>{rec_html}</div>
<div class="box" style="padding:20px"><h3 style="color:#FFEAA7;margin-bottom:14px;font-size:15px">Experiments</h3>{exp_html}</div>
</div>

<div class="tc" id="t-daily">
<div class="ins"><b>Daily Metrics:</b> Peak active: <b class="g">{peak_active_val}</b> on {peak_active_date}.</div>
<div class="g2"><div class="box"><div id="c-c14"></div></div><div class="box"><div id="c-c15"></div></div></div>
<div class="g2">
<div class="box" style="max-height:420px;overflow-y:auto"><h4 style="color:#4ECDC4;margin-bottom:8px;font-size:13px">Daily Installs & Active</h4>
<table><tr><th>Date</th><th>Installs</th><th>Cum</th><th>Conv</th><th>Cum Conv</th><th>Active Paid</th></tr>{daily_tbl1}</table></div>
<div class="box" style="max-height:420px;overflow-y:auto"><h4 style="color:#E74C3C;margin-bottom:8px;font-size:13px">Expired & Recharged</h4>
<table><tr><th>Date</th><th>Expired</th><th>Recharged</th><th>Rate</th></tr>{daily_tbl2}</table></div>
</div></div>

<div class="tc" id="t-cohort">
<div class="ins"><b>Plan Cohort:</b>
<span class="badge badge-g">Same: {stick_pct:.0f}%</span>
<span class="badge badge-b">Upgrades: {up_pct_plan:.0f}%</span>
<span class="badge badge-r">Downgrades: {down_pct_plan:.0f}%</span> ({tot_tr} transitions)</div>
<div class="box"><div id="c-c16"></div></div>
<div class="g2"><div class="box"><div id="c-c17"></div></div><div class="box"><div id="c-c18"></div></div></div>
<div class="g2">
<div class="box"><h4 style="color:#4ECDC4;margin-bottom:8px;font-size:13px">Transition Matrix</h4><table><tr>{trans_hdr}</tr>{trans_tbl}</table></div>
<div class="box" style="max-height:420px;overflow-y:auto"><h4 style="color:#FFEAA7;margin-bottom:8px;font-size:13px">Top 20 Journeys (3+ Steps)</h4>
<table><tr><th>#</th><th>Journey Path</th><th>Steps</th><th>Users</th><th>%</th></tr>{paths_tbl}</table></div>
</div>
<div class="g2">
<div class="box" style="max-height:420px;overflow-y:auto"><h4 style="color:#9B59B6;margin-bottom:8px;font-size:13px">Top 20 Journeys (2+ Steps)</h4>
<table><tr><th>#</th><th>Journey Path</th><th>Steps</th><th>Users</th><th>%</th></tr>{paths_tbl_2}</table></div>
<div class="box" style="padding:16px"><h4 style="color:#4ECDC4;margin-bottom:12px;font-size:13px">Journey Summary</h4>
<p style="color:#ccc;font-size:12px;line-height:1.6">
<b style="color:#9B59B6">2+ steps:</b> {total_2plus_all} users across {len(path_counter_2)} unique paths<br>
<b style="color:#FFEAA7">3+ steps:</b> {total_3plus_all} users across {len(path_counter)} unique paths</p></div>
</div></div>

<div class="tc" id="t-trialfunnel">
<div class="ins"><b>Trial Funnel:</b> <b class="g">{n_converted}</b>/{n_evaluable} evaluable converted (<b class="g">{conv_rate:.1f}%</b>).
Before trial: <b>{conv_before_count}</b>, After: <b>{conv_after_count}</b>.
<b class="r">{n_never_converted}</b> never converted. <span class="badge badge-b">{n_trial_active} in trial</span></div>
<div class="g2"><div class="box"><div id="c-c19"></div></div><div class="box"><div id="c-c20"></div></div></div>
<div class="g2"><div class="box"><div id="c-c21"></div></div><div class="box"><div id="c-c22"></div></div></div>
<div class="g2">
<div class="box"><h4 style="color:#4ECDC4;margin-bottom:8px;font-size:13px">Funnel</h4><table><tr><th>Stage</th><th>Users</th><th>%</th></tr>{trial_funnel_tbl}</table></div>
<div class="box"><h4 style="color:#FFEAA7;margin-bottom:8px;font-size:13px">Post-Trial Timing</h4><table><tr><th>Timing</th><th>Users</th><th>%</th><th>Cum</th><th>Cum%</th></tr>{conv_timing_tbl}</table></div>
</div></div>

<div class="tc" id="t-nonconv">
<div class="ins"><b>Non-Converted:</b> <b class="r">{n_never_converted}</b> trial-expired, never purchased.
<span class="badge badge-b">{n_trial_active} still in trial</span></div>
<div class="g2">
<div class="box"><h4 style="color:#E74C3C;margin-bottom:8px;font-size:13px">Non-Converted Segments (by days since trial expired)</h4>
<table><tr><th>Segment</th><th>Users</th><th>%</th></tr>{nc_seg_tbl}</table></div>
<div class="box"><div id="c-c23"></div></div></div>
<h4 style="color:#4ECDC4;margin:16px 0 8px;font-size:15px">Plan-Wise Purchase Frequency</h4>
<div class="ins" style="border-color:#FFEAA7"><b style="color:#FFEAA7">How many users purchased each plan type X+ times in their lifetime (converted users only):</b></div>
<div class="box"><div id="c-c27"></div></div>
<div class="box"><table><tr><th>Plan Type</th><th>Total Purchases</th><th>Unique Users</th><th>1+ times</th><th>2+ times</th><th>3+ times</th><th>5+ times</th><th>10+ times</th></tr>{plan_freq_tbl}</table></div>
</div>

<div class="tc" id="t-rday">
<div class="ins"><b>R-Day Report:</b> <b class="r">{total_rday}</b> non-converted with expired trial.
Peak: <b class="r">R{peak_rday}</b> ({peak_rday_count}). Bucket: <b class="y">{highest_risk_bucket}</b> ({highest_risk_count}).</div>
<div class="g2"><div class="box"><div id="c-c24"></div></div><div class="box"><div id="c-c26"></div></div></div>
<div class="box"><div id="c-c25"></div></div>
<div class="g2">
<div class="box" style="max-height:420px;overflow-y:auto"><h4 style="color:#E74C3C;margin-bottom:8px;font-size:13px">R-Day Detail</h4>
<table><tr><th>R-Day</th><th>Users</th><th>%</th><th>Cum</th><th>Cum%</th></tr>{rday_detail_tbl}</table></div>
<div class="box" style="padding:16px"><h4 style="color:#4ECDC4;margin-bottom:12px;font-size:13px">Insights</h4>{ti_html}
<h4 style="color:#FF6B6B;margin:16px 0 12px;font-size:13px">Interventions</h4>{iv_html}</div>
</div></div>

<footer>WIOM Recharge Lifecycle | {total_users} users | Generated {TODAY_STR}</footer>

<script>
var C={ALL_CHARTS};
var cfg={{responsive:true,displayModeBar:true,displaylogo:false}};
var R={{}};
var M={{
  conv:[["c-c2","c2"]],
  firstplan:[["c-c4","c4"],["c-c5","c5"]],
  retention:[["c-c6","c6"],["c-c7","c7"]],
  gaps:[["c-c8","c8"],["c-c9","c9"]],
  segments:[["c-c11","c11"],["c-c12","c12"],["c-c28","c28"],["c-c29","c29"]],
  daily:[["c-c14","c14"],["c-c15","c15"]],
  cohort:[["c-c16","c16"],["c-c17","c17"],["c-c18","c18"]],
  trialfunnel:[["c-c19","c19"],["c-c20","c20"],["c-c21","c21"],["c-c22","c22"]],
  nonconv:[["c-c23","c23"],["c-c27","c27"]],
  rday:[["c-c24","c24"],["c-c25","c25"],["c-c26","c26"]]
}};
function render(t){{if(R[t])return;var items=M[t];if(!items)return;items.forEach(function(p){{var el=document.getElementById(p[0]);if(el&&C[p[1]])Plotly.newPlot(p[0],C[p[1]].data,C[p[1]].layout,cfg)}});R[t]=true}}
function sw(t){{document.querySelectorAll(".tc").forEach(function(e){{e.classList.remove("active")}});document.querySelectorAll(".tab").forEach(function(e){{e.classList.remove("active")}});document.getElementById("t-"+t).classList.add("active");document.querySelector('[data-t="'+t+'"]').classList.add("active");setTimeout(function(){{render(t)}},50)}}
document.getElementById("tb").addEventListener("click",function(e){{var t=e.target.getAttribute("data-t");if(t)sw(t)}});
window.addEventListener("load",function(){{render("conv")}});
</script></body></html>'''

path = os.path.join(OUT, 'recharge_dashboard.html')
with open(path, 'w', encoding='utf-8') as f:
    f.write(html)
# Also save as index.html for GitHub Pages
index_path = os.path.join(OUT, 'index.html')
with open(index_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"\nSaved: {path} ({os.path.getsize(path)//1024} KB)")
print("DONE!")
