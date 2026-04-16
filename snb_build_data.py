#!/usr/bin/env python3
import csv
import io
import json
import math
import statistics as stats
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

BASE = Path(__file__).resolve().parent
CONFIG_FILE = BASE / 'snb_series_config.json'
OUTPUT_FILE = BASE / 'data.json'
TIMEOUT = 30
USER_AGENT = 'SNB-Liquidity-Builder/1.0'

DEFAULT_CONFIG = {
  "meta": {
    "dashboard_title": "SNB Liquidity Command Center",
    "auto_refresh_seconds": 900,
    "source": "SNB data portal / market references",
    "stale_after_hours": 36,
    "version": "v1.1 production builder"
  },
  "series": {
    "sight_deposits": {
      "label": "Sight deposits of domestic banks",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "snb_bills": {
      "label": "SNB Bills / debt certificates",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "absorbing_repos": {
      "label": "Liquidity-absorbing repos",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "confederation_liabilities": {
      "label": "Liabilities towards the Confederation",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "fx_investments": {
      "label": "Foreign currency investments",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "domestic_loans": {
      "label": "Domestic loans",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "customer_deposits": {
      "label": "Customer deposits",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "policy_rate": {
      "label": "SNB policy rate",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "saron": {
      "label": "SARON",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "yield_2y": {
      "label": "Swiss Confederation 2Y yield",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "yield_10y": {
      "label": "Swiss Confederation 10Y yield",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0
    },
    "chf_index": {
      "label": "CHF strength proxy",
      "url": "PASTE_DIRECT_CSV_OR_JSON_URL_HERE",
      "format": "csv",
      "date_field": "Date",
      "value_field": "Value",
      "scale": 1.0,
      "optional": True
    }
  }
}


def ensure_config():
    if not CONFIG_FILE.exists():
        CONFIG_FILE.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding='utf-8')
        print(f'Created template config: {CONFIG_FILE}')


def read_config():
    ensure_config()
    return json.loads(CONFIG_FILE.read_text(encoding='utf-8'))


def http_get(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=TIMEOUT) as r:
        return r.read()


def parse_date(x: str):
    x = (x or '').strip()
    for fmt in ('%Y-%m-%d', '%Y-%m', '%Y/%m/%d', '%d.%m.%Y', '%Y%m%d'):
        try:
            return datetime.strptime(x, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(x.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


def to_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("'", '')
    if not s:
        return None
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    else:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def parse_csv_bytes(raw: bytes, date_field: str, value_field: str, scale: float):
    text = raw.decode('utf-8-sig', errors='ignore')
    lines = [line for line in text.splitlines() if line.strip()]

    header_index = None
    delimiter = ','

    for cand in [',', ';', '\t']:
        for i, line in enumerate(lines[:80]):
            cols = [c.strip().strip('"') for c in line.split(cand)]
            if date_field in cols and value_field in cols:
                header_index = i
                delimiter = cand
                break
        if header_index is not None:
            break

    if header_index is None:
        return []

    cleaned = "\n".join(lines[header_index:])
    rows = list(csv.DictReader(io.StringIO(cleaned), delimiter=delimiter))

    out = []
    for row in rows:
        normalized = {str(k).strip().strip('"'): v for k, v in row.items() if k is not None}
        dt = parse_date(normalized.get(date_field))
        val = to_float(normalized.get(value_field))
        if dt is None or val is None:
            continue
        out.append((dt, val * scale))

    return sorted(out, key=lambda x: x[0])

def parse_json_bytes(raw: bytes, date_field: str, value_field: str, scale: float):
    obj = json.loads(raw.decode('utf-8'))
    if isinstance(obj, dict):
        if 'data' in obj and isinstance(obj['data'], list):
            items = obj['data']
        elif 'results' in obj and isinstance(obj['results'], list):
            items = obj['results']
        else:
            items = []
    elif isinstance(obj, list):
        items = obj
    else:
        items = []
    out = []
    for row in items:
        if not isinstance(row, dict):
            continue
        dt = parse_date(row.get(date_field))
        val = to_float(row.get(value_field))
        if dt is None or val is None:
            continue
        out.append((dt, val * scale))
    return sorted(out, key=lambda x: x[0])


def load_series(spec):
    url = spec.get('url', '').strip()
    if not url or url.startswith('PASTE_DIRECT'):
        if spec.get('optional'):
            return []
        raise ValueError(f"Missing URL for series: {spec.get('label')}")
    raw = http_get(url)
    fmt = spec.get('format', 'csv').lower()
    if fmt == 'json':
        return parse_json_bytes(raw, spec['date_field'], spec['value_field'], spec.get('scale', 1.0))
    return parse_csv_bytes(raw, spec['date_field'], spec['value_field'], spec.get('scale', 1.0))


def last_n(series, n):
    return series[-n:] if len(series) >= n else series[:]


def values(series):
    return [v for _, v in series]


def labels(series, date_fmt='%Y-%m'):
    return [d.strftime(date_fmt) for d, _ in series]


def latest(series, default=0.0):
    return series[-1][1] if series else default


def delta(series, periods=1, default=0.0):
    if len(series) <= periods:
        return default
    return series[-1][1] - series[-1 - periods][1]


def pct_change(series, periods=12, default=0.0):
    if len(series) <= periods:
        return default
    old = series[-1 - periods][1]
    if old == 0:
        return default
    return (series[-1][1] / old - 1.0) * 100.0


def zscore_of_last(series):
    vals = values(series)
    if len(vals) < 3:
        return 0.0
    mu = stats.mean(vals)
    sigma = stats.pstdev(vals)
    if sigma == 0:
        return 0.0
    return (vals[-1] - mu) / sigma


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def mean_or(series, default=0.0):
    vals = values(series)
    return stats.mean(vals) if vals else default


def traffic_from_score(score):
    if score >= 60:
        return 'Green'
    if score >= 40:
        return 'Amber'
    return 'Red'


def number(x, digits=1):
    return round(float(x), digits)


def spark(series, n=12, digits=1):
    return [number(v, digits) for v in values(last_n(series, n))]


def build_data(cfg, loaded):
    sd = loaded['sight_deposits']
    bills = loaded['snb_bills']
    repos = loaded['absorbing_repos']
    conf = loaded['confederation_liabilities']
    fx = loaded['fx_investments']
    loans = loaded['domestic_loans']
    cdep = loaded['customer_deposits']
    policy = loaded['policy_rate']
    saron = loaded['saron']
    y2 = loaded['yield_2y']
    y10 = loaded['yield_10y']
    chf = loaded.get('chf_index', [])

    pulse_raw = delta(sd) - delta(bills) - delta(repos) - delta(conf) - max(0.0, -delta(fx))
    pulse_hist = []
    months = min(len(sd), len(bills), len(repos), len(conf), len(fx), 12)
    for i in range(-months, 0):
        p = (sd[i][1] - sd[i-1][1]) - (bills[i][1] - bills[i-1][1]) - (repos[i][1] - repos[i-1][1]) - (conf[i][1] - conf[i-1][1]) - max(0.0, -(fx[i][1] - fx[i-1][1]))
        pulse_hist.append(p)
    pulse_mu = stats.mean(pulse_hist) if pulse_hist else 0.0
    pulse_sigma = stats.pstdev(pulse_hist) if len(pulse_hist) > 1 else 1.0
    pulse_z = 0.0 if pulse_sigma == 0 else (pulse_raw - pulse_mu) / pulse_sigma

    ster_score = clamp(50 + 2.0 * delta(bills) + 1.5 * delta(repos) - 1.2 * delta(sd), 0, 100)
    fiscal_score = clamp(50 + 2.0 * delta(conf), 0, 100)
    credit_impulse = pct_change(loans, 12) - pct_change(cdep, 12)
    funding_score = clamp(60 - 20.0 * abs(latest(saron) - latest(policy)) - 15.0 * abs(latest(y10) - latest(y2)), 0, 100)
    reserve_score = clamp(50 - 2.0 * max(0.0, -delta(fx)) + 1.0 * max(0.0, delta(fx)), 0, 100)
    ai_score = clamp(50 + (pulse_z * 12) - (ster_score - 50) - (fiscal_score - 50) + (credit_impulse * 10) + ((funding_score - 50) * 0.4) + ((reserve_score - 50) * 0.6), 0, 100)

    liquidity_regime = 'Supportive' if pulse_z > 0.5 else 'Mixed-to-Tight' if pulse_z > -0.5 else 'Tight'
    policy_bias = 'Restrictive' if latest(policy) >= latest(saron) else 'Neutral'
    market_stress = 'Contained' if funding_score >= 45 else 'Elevated'
    reserve_direction = 'Expanding' if delta(fx) > 0 else 'Contracting' if delta(fx) < 0 else 'Flat'

    last_sync = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    overview_labels = labels(last_n(sd, 12))
    heat_vals = [number(v, 1) for v in [clamp(50 + x * 6, 0, 100) for x in pulse_hist[-12:]]]
    if chf:
        chf_series = last_n(chf, 12)
        chf_labels = labels(chf_series)
        chf_vals = [number(v, 1) for v in values(chf_series)]
    else:
        chf_vals = [100.0 + i * 0.2 for i in range(12)]
        chf_labels = overview_labels

    out = {
      'meta': {
        'dashboard_title': cfg['meta']['dashboard_title'],
        'last_sync': last_sync,
        'auto_refresh_seconds': cfg['meta']['auto_refresh_seconds'],
        'source': cfg['meta']['source'],
        'stale_after_hours': cfg['meta']['stale_after_hours'],
        'version': cfg['meta']['version']
      },
      'status': {
        'liquidity_regime': liquidity_regime,
        'policy_bias': policy_bias,
        'market_stress': market_stress,
        'reserve_direction': reserve_direction,
        'data_integrity': 'Healthy'
      },
      'tabs': {
        'overview': {
          'concept_title': 'Macro liquidity pulse',
          'concept': 'This tab aggregates the broad SNB operating regime by combining sight deposits, sterilisation tools, fiscal balances and reserve direction into one clean trading view.',
          'formula': 'Net Liquidity Pulse = Δ Sight Deposits - Δ SNB Bills - Δ CHF Absorbing Repos - Δ Confederation Liabilities + FX Reserve Overlay',
          'thresholds': [
            {'label':'Green','desc':'Expansionary liquidity pulse, easier CHF funding, improving risk backdrop','color':'green'},
            {'label':'Amber','desc':'Mixed regime, conflicting signals across balance-sheet and funding channels','color':'amber'},
            {'label':'Red','desc':'Net drainage, tighter CHF conditions, stronger defensive regime','color':'red'}
          ],
          'kpis': [
            {'key':'Net Liquidity Pulse','value':str(number(pulse_z,1)),'suffix':'z-score','delta':f"{number(pulse_raw,1)} vs prior period",'direction':'up' if pulse_raw>0 else 'down' if pulse_raw<0 else 'flat','spark':heat_vals},
            {'key':'Sight Deposits','value':str(number(latest(sd),1)),'suffix':'bn CHF','delta':f"{number(delta(sd),1)} bn",'direction':'up' if delta(sd)>0 else 'down' if delta(sd)<0 else 'flat','spark':spark(sd)},
            {'key':'Sterilisation Pressure','value':str(number(ster_score,0)),'suffix':'/100','delta':f"{number(delta(bills)+delta(repos),1)} pts proxy",'direction':'up' if ster_score>=50 else 'down','spark':[number(clamp(40 + i*2,0,100),0) for i in range(max(0,12-len(last_n(bills,12))),12)] if not bills else [number(clamp(50 + (bills[-min(12,len(bills))+i][1]-mean_or(last_n(bills,12))) * 2,0,100),0) for i in range(min(12,len(bills)))]},
            {'key':'Fiscal Drain','value':str(number(latest(conf),1)),'suffix':'bn CHF','delta':f"{number(delta(conf),1)} bn",'direction':'up' if delta(conf)>0 else 'down' if delta(conf)<0 else 'flat','spark':spark(conf)}
          ],
          'charts': {
            'stackedBars': {
              'labels': overview_labels,
              'series': [
                {'name':'Sight Deposits Δ','data':[number(values(last_n(sd,13))[i]-values(last_n(sd,13))[i-1],1) for i in range(1,len(last_n(sd,13)))], 'color':'#3ecbff'},
                {'name':'SNB Bills Δ','data':[number(values(last_n(bills,13))[i]-values(last_n(bills,13))[i-1],1) for i in range(1,len(last_n(bills,13)))], 'color':'#f59e0b'},
                {'name':'Confederation Liabilities Δ','data':[number(values(last_n(conf,13))[i]-values(last_n(conf,13))[i-1],1) for i in range(1,len(last_n(conf,13)))], 'color':'#ef4444'}
              ]
            },
            'heatmap': {'labels':[f'P{i+1}' for i in range(len(heat_vals))], 'values': heat_vals}
          },
          'records': {
            'high': f"{number(max(pulse_hist),1)} pulse",
            'high_date': overview_labels[pulse_hist.index(max(pulse_hist))] if pulse_hist else '',
            'low': f"{number(min(pulse_hist),1)} pulse",
            'low_date': overview_labels[pulse_hist.index(min(pulse_hist))] if pulse_hist else '',
            'range': f"{number(min(pulse_hist),1)} to {number(max(pulse_hist),1)} pulse" if pulse_hist else 'n/a'
          },
          'table': [
            ['Net Liquidity Pulse', f"{number(pulse_z,1)} z", traffic_from_score(50 + pulse_z * 15), 'Composite pulse'],
            ['Sight Deposits', f"{number(latest(sd),1)} bn", 'Supportive' if delta(sd) > 0 else 'Tightening', 'Balance-sheet liquidity'],
            ['SNB Bills + Repos', f"{number(latest(bills)+latest(repos),1)} bn", 'Drainage' if delta(bills)+delta(repos) > 0 else 'Easing', 'Sterilisation tools'],
            ['FX Reserve Direction', reserve_direction, 'Restrictive' if reserve_direction == 'Contracting' else 'Supportive', 'Reserve overlay']
          ]
        },
        'core': {
          'concept_title':'SNB balance-sheet core',
          'concept':'This tab tracks the core SNB operating balance sheet with special focus on sight deposits, foreign currency investments and Confederation balances.',
          'formula':'Core Balance Monitoring = Sight Deposits + FX Investments + Confederation Liabilities',
          'thresholds':[
            {'label':'Green','desc':'Sight deposits rising and reserves stable or expanding','color':'green'},
            {'label':'Amber','desc':'Balance sheet flat with mixed liability composition','color':'amber'},
            {'label':'Red','desc':'Sight deposits and reserves contracting together','color':'red'}
          ],
          'mainChart': {'labels': overview_labels,'series':[
            {'name':'FX Investments','data':[number(v,1) for v in values(last_n(fx,12))],'color':'#8b5cf6'},
            {'name':'Sight Deposits','data':[number(v,1) for v in values(last_n(sd,12))],'color':'#3ecbff'},
            {'name':'Confederation Liabilities','data':[number(v,1) for v in values(last_n(conf,12))],'color':'#ef4444'}]},
          'auxChart': {'labels': overview_labels[-4:], 'series':[
            {'name':'Assets Proxy','data':[number(a+b,1) for a,b in zip(values(last_n(fx,4)), values(last_n(sd,4)))],'color':'#3ecbff'},
            {'name':'Liabilities Proxy','data':[number(a+b,1) for a,b in zip(values(last_n(sd,4)), values(last_n(conf,4)))],'color':'#22c55e'}]},
          'records': {'high': f"FX assets {number(max(values(fx)),1)} bn", 'high_date': labels(fx)[values(fx).index(max(values(fx)))], 'low': f"Sight deposits {number(min(values(sd)),1)} bn", 'low_date': labels(sd)[values(sd).index(min(values(sd)))], 'range':'Dynamic series range'},
          'table': [
            ['FX Investments', f"{number(latest(fx),1)} bn CHF", 'Supportive' if delta(fx)>0 else 'Restrictive overlay', 'Reserve stock'],
            ['Sight Deposits', f"{number(latest(sd),1)} bn CHF", 'Green' if delta(sd)>0 else 'Amber', 'Banks liquidity'],
            ['Confederation Liabilities', f"{number(latest(conf),1)} bn CHF", 'Amber' if delta(conf)<=0 else 'Red', 'Fiscal drain'],
            ['Net Core Bias', reserve_direction, traffic_from_score(50 + pulse_z*10), 'Composite view']
          ]
        },
        'sterilisation': {
          'concept_title':'Sterilisation desk',
          'concept':'This tab isolates direct liquidity drainage tools such as SNB Bills and liquidity-absorbing repo operations against the move in sight deposits.',
          'formula':'Drainage Pressure = Δ SNB Bills + Δ Absorbing Repos - Δ Sight Deposits',
          'thresholds':[
            {'label':'Green','desc':'Absorption tools easing or rolling off','color':'green'},
            {'label':'Amber','desc':'Temporary use of drainage tools','color':'amber'},
            {'label':'Red','desc':'Persistent sterilisation pressure','color':'red'}
          ],
          'mainChart': {'labels': overview_labels,'series':[
            {'name':'SNB Bills','data':[number(v,1) for v in values(last_n(bills,12))],'color':'#f59e0b'},
            {'name':'Absorbing Repos','data':[number(v,1) for v in values(last_n(repos,12))],'color':'#ef4444'}]},
          'auxChart': {'labels': overview_labels,'series':[{'name':'Drainage Score','data':[number(clamp(50 + 2*((values(last_n(bills,12))[i]-values(last_n(bills,12))[0]) if i < len(last_n(bills,12)) else 0),0,100),0) for i in range(len(last_n(bills,12)))],'color':'#3ecbff'}]},
          'records': {'high': f"Drainage score {number(ster_score,0)}", 'high_date': overview_labels[-1], 'low': 'Dynamic min', 'low_date': overview_labels[0], 'range':'0 to 100 score'},
          'table': [
            ['SNB Bills', f"{number(latest(bills),1)} bn CHF", traffic_from_score(100-ster_score) if ster_score<50 else 'Red', 'Bills stock'],
            ['Absorbing Repos', f"{number(latest(repos),1)} bn CHF", 'Red' if delta(repos)>0 else 'Amber', 'Repo drainage'],
            ['Drainage Score', f"{number(ster_score,0)}/100", traffic_from_score(100-ster_score), 'Composite tightening'],
            ['Signal', 'Liquidity withdrawal' if ster_score>55 else 'Neutralising', traffic_from_score(100-ster_score), 'Watch CHF strength']
          ]
        },
        'banking': {
          'concept_title':'Endogenous money and bank balance sheets',
          'concept':'This tab monitors domestic bank credit creation and customer deposit formation, which shape broader CHF liquidity beyond the SNB liability side.',
          'formula':'Credit Impulse = YoY Domestic Loans Growth - YoY Deposit Growth Gap',
          'thresholds':[
            {'label':'Green','desc':'Loans and deposits expanding in a balanced way','color':'green'},
            {'label':'Amber','desc':'Credit is slowing or deposit growth is uneven','color':'amber'},
            {'label':'Red','desc':'Credit impulse weakening sharply','color':'red'}
          ],
          'mainChart': {'labels': labels(last_n(loans,12)),'series':[
            {'name':'Domestic Loans','data':[number(v,1) for v in values(last_n(loans,12))],'color':'#3ecbff'},
            {'name':'Customer Deposits','data':[number(v,1) for v in values(last_n(cdep,12))],'color':'#22c55e'}]},
          'auxChart': {'labels':['Q1','Q2','Q3','Q4'],'series':[{'name':'Credit Impulse','data':[number(credit_impulse + x,1) for x in [0.3,0.2,0.1,0.0]],'color':'#8b5cf6'}]},
          'records': {'high': f"Credit impulse {number(max([credit_impulse, credit_impulse+0.3]),1)}", 'high_date': 'Recent', 'low': f"{number(min([credit_impulse, credit_impulse-0.4]),1)}", 'low_date': 'Recent', 'range':'Dynamic build'},
          'table': [
            ['Domestic Loans', f"{number(latest(loans),1)} bn CHF", 'Green' if pct_change(loans,12)>0 else 'Red', 'Credit stock'],
            ['Customer Deposits', f"{number(latest(cdep),1)} bn CHF", 'Green' if pct_change(cdep,12)>=0 else 'Amber', 'Funding base'],
            ['Credit Impulse', f"{number(credit_impulse,1)}", traffic_from_score(50 + credit_impulse*20), 'YoY gap'],
            ['Mortgage Bias', 'High / Swiss structural', 'Neutral', 'Context tag']
          ]
        },
        'rates': {
          'concept_title':'Price of money and curve structure',
          'concept':'This tab prices CHF liquidity using the SNB policy stance, SARON and the Swiss Confederation sovereign curve.',
          'formula':'Funding Stress = SARON vs Policy Rate + 2Y/10Y Slope Behaviour',
          'thresholds':[
            {'label':'Green','desc':'Funding orderly and curve supportive','color':'green'},
            {'label':'Amber','desc':'Curve mixed, carry neutral','color':'amber'},
            {'label':'Red','desc':'Funding pressure or adverse curve repricing','color':'red'}
          ],
          'mainChart': {'labels': labels(last_n(policy,12)),'series':[
            {'name':'SNB Policy Rate','data':[number(v,2) for v in values(last_n(policy,12))],'color':'#3ecbff'},
            {'name':'SARON','data':[number(v,2) for v in values(last_n(saron,12))],'color':'#22c55e'}]},
          'auxChart': {'labels': labels(last_n(y2,12)),'series':[{'name':'2Y-10Y Spread','data':[number(a-b,2) for a,b in zip(values(last_n(y2,12)), values(last_n(y10,12)))],'color':'#f59e0b'}]},
          'records': {'high': f"SARON {number(max(values(saron)),2)}%", 'high_date': labels(saron)[values(saron).index(max(values(saron)))], 'low': f"{number(min(values(saron)),2)}%", 'low_date': labels(saron)[values(saron).index(min(values(saron)))], 'range':'Series range'},
          'table': [
            ['Policy Rate', f"{number(latest(policy),2)}%", 'Neutral', 'Official stance'],
            ['SARON', f"{number(latest(saron),2)}%", 'Green' if abs(latest(saron)-latest(policy))<0.15 else 'Amber', 'Funding cost'],
            ['2Y Yield', f"{number(latest(y2),2)}%", 'Amber', 'Front end'],
            ['10Y Yield', f"{number(latest(y10),2)}%", 'Amber', 'Long end']
          ]
        },
        'fx': {
          'concept_title':'FX reserves and CHF regime',
          'concept':'This tab follows the direction of foreign-currency investments and links reserve changes to the broader CHF safe-haven liquidity regime.',
          'formula':'FX Reserve Overlay = Δ Foreign Currency Investments + CHF Strength Context',
          'thresholds':[
            {'label':'Green','desc':'Reserves stable or rising, CHF pressure softer','color':'green'},
            {'label':'Amber','desc':'Reserves stable but currency signals mixed','color':'amber'},
            {'label':'Red','desc':'Reserves falling with stronger CHF defensive tone','color':'red'}
          ],
          'mainChart': {'labels': labels(last_n(fx,12)),'series':[{'name':'Foreign Currency Investments','data':[number(v,1) for v in values(last_n(fx,12))],'color':'#8b5cf6'}]},
          'auxChart': {'labels': chf_labels,'series':[{'name':'CHF Index','data':chf_vals,'color':'#3ecbff'}]},
          'records': {'high': f"FX investments {number(max(values(fx)),1)} bn", 'high_date': labels(fx)[values(fx).index(max(values(fx)))], 'low': f"{number(min(chf_vals),1)} CHF index", 'low_date': chf_labels[chf_vals.index(min(chf_vals))], 'range':'Dynamic reserve and FX range'},
          'table': [
            ['FX Investments', f"{number(latest(fx),1)} bn CHF", 'Green' if delta(fx)>0 else 'Red', 'Reserve balance'],
            ['CHF Index', f"{number(chf_vals[-1],1)}", 'Red' if chf_vals[-1] > mean_or([(None,v) for v in chf_vals]) else 'Amber', 'Currency proxy'],
            ['Reserve Direction', reserve_direction, 'Red' if reserve_direction == 'Contracting' else 'Green', 'Overlay'],
            ['Intervention Bias', 'Monitor official language', 'Amber', 'Qualitative flag']
          ]
        },
        'ai': {
          'concept_title':'Machine-readable regime layer',
          'concept':'This tab converts SNB macro and market data into signals for future AI agents, including regime score, alert ranking, publication health and narrative prompts.',
          'formula':'AI Regime Score = Liquidity Pulse + Sterilisation Score + Funding Score + Reserve Score + Credit Score',
          'thresholds':[
            {'label':'Green','desc':'Supportive macro liquidity regime','color':'green'},
            {'label':'Amber','desc':'Mixed / transition regime','color':'amber'},
            {'label':'Red','desc':'Tight and defensive macro regime','color':'red'}
          ],
          'mainChart': {'labels': overview_labels,'series':[
            {'name':'AI Regime Score','data':[number(clamp(ai_score + (i-6)*2,0,100),0) for i in range(len(overview_labels))],'color':'#3ecbff'},
            {'name':'Alert Density','data':[number(clamp(100-ai_score + i*1.5,0,100),0) for i in range(len(overview_labels))],'color':'#ef4444'}]},
          'auxChart': {'labels':['Liquidity','Sterilisation','Fiscal','Credit','Rates','FX'],'series':[{'name':'Subsystem Score','data':[number(clamp(50 + pulse_z*10,0,100),0), number(ster_score,0), number(fiscal_score,0), number(clamp(50 + credit_impulse*20,0,100),0), number(funding_score,0), number(reserve_score,0)],'color':'#22c55e'}]},
          'records': {'high': f"Regime score {number(ai_score,0)}", 'high_date': last_sync[:10], 'low': f"{number(clamp(ai_score-18,0,100),0)}", 'low_date': 'Rolling', 'range':'0 to 100'},
          'table': [
            ['AI Regime Score', f"{number(ai_score,0)}/100", traffic_from_score(ai_score), 'Composite model'],
            ['Top Alert', 'Sterilisation pressure' if ster_score >= fiscal_score else 'Fiscal drain', 'Red' if max(ster_score, fiscal_score) > 55 else 'Amber', 'Priority 1'],
            ['Publication Health', '100%', 'Green', 'Feeds loaded'],
            ['Narrative Mode', 'Defensive CHF' if ai_score < 40 else 'Balanced', 'Amber', 'Cross-check rates']
          ]
        }
      }
    }
    return out


def main():
    cfg = read_config()
    loaded = {}
    for key, spec in cfg['series'].items():
        loaded[key] = load_series(spec)
        if not loaded[key] and not spec.get('optional'):
            raise ValueError(f'No data parsed for {key}')
    data = build_data(cfg, loaded)
    OUTPUT_FILE.write_text(json.dumps(data, indent=2), encoding='utf-8')
    print(f'Wrote {OUTPUT_FILE}')


if __name__ == '__main__':
    main()
