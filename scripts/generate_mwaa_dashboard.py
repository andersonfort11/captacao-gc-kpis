"""
Dashboard Monitoramento Dados — Time Captação e G&C
Coleta dados do MWAA via AWS API e gera página HTML com Plotly.
Uso: python scripts/generate_mwaa_dashboard.py
Requer: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION como env vars.
"""
import os
import json
import base64
import ssl
import urllib.request
import urllib.parse
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime, timedelta, date
from collections import defaultdict

# ============================================================
# CONFIGURAÇÃO
# ============================================================
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Período
PERIODO_INICIO = "2026-01-01"
TOTAL_DIAS_PERIODO = (date.today() - date(2026, 1, 1)).days + 1

# ============================================================
# AWS SIGNATURE V4 (simplificado via boto3)
# ============================================================
try:
    import boto3
except ImportError:
    print("ERRO: boto3 não instalado. Instale com: pip install boto3")
    exit(1)


def get_boto_session():
    return boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


# ============================================================
# COLETA: API REST (WebLoginToken) — cdl-airflow-mwaa
# ============================================================
def fetch_dag_runs_rest(mwaa_env, dag_id):
    """Busca DAG runs via API REST do Airflow (WebLoginToken)."""
    session_boto = get_boto_session()
    mwaa_client = session_boto.client("mwaa", region_name=AWS_REGION)
    resp = mwaa_client.create_web_login_token(Name=mwaa_env)
    base_url = f"https://{resp['WebServerHostname']}"

    import requests
    s = requests.Session()
    s.verify = False
    s.get(f"{base_url}/aws_mwaa/aws-console-sso?login=true")
    s.post(f"{base_url}/aws_mwaa/login", data={"token": resp["WebToken"]}, allow_redirects=True)

    all_runs = []
    offset = 0
    while True:
        url = f"{base_url}/api/v1/dags/{dag_id}/dagRuns?limit=100&offset={offset}&order_by=-start_date"
        r = s.get(url)
        if r.status_code != 200:
            print(f"  REST ERROR {r.status_code}")
            break
        data = r.json()
        runs = data.get("dag_runs", [])
        if not runs:
            break
        all_runs.extend(runs)
        if runs[-1].get("start_date", "") < f"{PERIODO_INICIO}T00:00:00":
            break
        offset += 100
        if offset >= data.get("total_entries", 0):
            break

    return [{"state": r["state"], "end_date": r.get("end_date", ""), "start_date": r.get("start_date", "")}
            for r in all_runs if (r.get("start_date") or "") >= f"{PERIODO_INICIO}T00:00:00"]


# ============================================================
# COLETA: CliToken — cdl-cogna-lakehouseMwaa (Airflow 2.2)
# ============================================================
def fetch_dag_runs_cli(mwaa_env, dag_id):
    """Busca DAG runs via CliToken (Airflow 2.2, output tabular)."""
    import requests
    session_boto = get_boto_session()
    mwaa_client = session_boto.client("mwaa", region_name=AWS_REGION)
    token = mwaa_client.create_cli_token(Name=mwaa_env)

    r = requests.post(
        f"https://{token['WebServerHostname']}/aws_mwaa/cli",
        headers={"Authorization": f"Bearer {token['CliToken']}"},
        data=f"dags list-runs -d {dag_id} --no-backfill -o table",
        verify=False,
    )
    if r.status_code != 200:
        print(f"  CLI ERROR {r.status_code}")
        return []

    d = json.loads(r.text)
    stdout = base64.b64decode(d.get("stdout", "")).decode("utf-8")

    runs = []
    for line in stdout.split("\n"):
        if "|" not in line or "dag_id" in line.lower() or "==" in line:
            continue
        cols = [c.strip() for c in line.split("|")]
        if len(cols) >= 6 and cols[2] in ("success", "failed"):
            if cols[4][:10] >= PERIODO_INICIO:
                runs.append({"state": cols[2], "end_date": cols[5], "start_date": cols[4]})
    return runs


# ============================================================
# PROCESSAMENTO
# ============================================================
def parse_dt(s):
    if not s:
        return None
    try:
        clean = s.replace("+00:00", "").replace("Z", "").strip()
        if "T" in clean:
            return datetime.fromisoformat(clean)
        return datetime.strptime(clean[:19], "%Y-%m-%d %H:%M:%S")
    except:
        return None


def process_standard(runs):
    daily = {}
    for run in runs:
        dt = parse_dt(run.get("end_date"))
        if not dt:
            continue
        dt_brt = dt - timedelta(hours=3)
        day = dt_brt.strftime("%Y-%m-%d")
        time_brt = dt_brt.strftime("%H:%M")
        if day not in daily:
            daily[day] = {"success": False, "max_end_brt": None}
        if run["state"] == "success":
            daily[day]["success"] = True
            if not daily[day]["max_end_brt"] or time_brt > daily[day]["max_end_brt"]:
                daily[day]["max_end_brt"] = time_brt
    return daily


def process_relalun(runs):
    daily = {}
    for run in runs:
        dt = parse_dt(run.get("end_date"))
        if not dt:
            continue
        dt_brt = dt - timedelta(hours=3)
        h = dt_brt.hour + dt_brt.minute / 60.0
        if h < 7.0 or h >= 12.0:
            continue
        day = dt_brt.strftime("%Y-%m-%d")
        time_brt = dt_brt.strftime("%H:%M")
        if day not in daily:
            daily[day] = {"success": False, "max_end_brt": None}
        if run["state"] == "success":
            daily[day]["success"] = True
            if not daily[day]["max_end_brt"] or time_brt > daily[day]["max_end_brt"]:
                daily[day]["max_end_brt"] = time_brt
    return daily


def calc_stats(daily):
    ok = sum(1 for d in daily.values() if d["success"])
    rate = ok / TOTAL_DIAS_PERIODO * 100
    minutos = sorted([int(d["max_end_brt"].split(":")[0]) * 60 + int(d["max_end_brt"].split(":")[1])
                      for d in daily.values() if d["success"] and d["max_end_brt"]])
    if minutos:
        n = len(minutos)
        med = (minutos[n // 2] + minutos[(n - 1) // 2]) / 2
    else:
        med = 0
    mediana_h = f"{int(med // 60):02d}:{int(med % 60):02d}"
    return ok, rate, mediana_h


def time_to_float(t):
    if not t:
        return None
    h, m = map(int, t.split(":"))
    return h + m / 60.0


def calc_mediana_mensal(daily):
    mensal = defaultdict(list)
    for day, info in daily.items():
        if info["success"] and info["max_end_brt"]:
            mes = day[:7]
            mensal[mes].append(time_to_float(info["max_end_brt"]))
    result = {}
    for mes, vals in sorted(mensal.items()):
        vals_sorted = sorted(vals)
        n = len(vals_sorted)
        result[mes] = (vals_sorted[n // 2] + vals_sorted[(n - 1) // 2]) / 2
    return result


# ============================================================
# MAIN
# ============================================================
print("=" * 60)
print("  Coleta de Dados — Dashboard Monitoramento MWAA")
print(f"  Período: {PERIODO_INICIO} a {date.today()}")
print("=" * 60)

# Suprimir warnings de SSL
import urllib3
urllib3.disable_warnings()
import warnings
warnings.filterwarnings("ignore")

# 1. PipelineGenteDiario
print("\n[1/3] PipelineGenteDiario...")
runs_gente = fetch_dag_runs_cli("cdl-cogna-lakehouseMwaa", "PipelineGenteDiario")
print(f"  Runs: {len(runs_gente)}")
daily_gente = process_standard(runs_gente)

# 2. CRESCIMENTO_CAPCOM
print("\n[2/3] CRESCIMENTO_CAPCOM...")
runs_capcom = fetch_dag_runs_cli("cdl-airflow-mwaa", "CRESCIMENTO_CAPCOM")
print(f"  Runs: {len(runs_capcom)}")
daily_capcom = process_standard(runs_capcom)

# 3. RELALUN_MAIN_ORCHESTRATOR
print("\n[3/3] RELALUN_MAIN_ORCHESTRATOR...")
runs_relalun = fetch_dag_runs_cli("cdl-cogna-lakehouseMwaa", "RELALUN_MAIN_ORCHESTRATOR")
print(f"  Runs: {len(runs_relalun)}")
daily_relalun = process_relalun(runs_relalun)

# Stats
stats_gente = calc_stats(daily_gente)
stats_capcom = calc_stats(daily_capcom)
stats_relalun = calc_stats(daily_relalun)

print(f"\n  G&C: {stats_gente[0]} OK | {stats_gente[1]:.1f}% | média {stats_gente[2]}")
print(f"  CAPCOM: {stats_capcom[0]} OK | {stats_capcom[1]:.1f}% | média {stats_capcom[2]}")
print(f"  RELALUN: {stats_relalun[0]} OK | {stats_relalun[1]:.1f}% | média {stats_relalun[2]}")

# ============================================================
# GRÁFICOS
# ============================================================
COR_SUCESSO = "#2ecc71"
COR_GENTE = "#3498db"
COR_CAPCOM = "#5dade2"
COR_RELALUN = "#e67e22"
COR_BG = "#1a1a2e"
COR_CARD = "#16213e"
COR_TEXT = "#eaeaea"

# Séries temporais
days_gente = sorted([d for d, v in daily_gente.items() if v["success"] and v["max_end_brt"]])
times_gente = [time_to_float(daily_gente[d]["max_end_brt"]) for d in days_gente]

days_capcom = sorted([d for d, v in daily_capcom.items() if v["success"] and v["max_end_brt"]])
times_capcom = [time_to_float(daily_capcom[d]["max_end_brt"]) for d in days_capcom]

days_relalun = sorted([d for d, v in daily_relalun.items() if v["success"] and v["max_end_brt"]])
times_relalun = [time_to_float(daily_relalun[d]["max_end_brt"]) for d in days_relalun]

# G&C: Linha
fig_gente = go.Figure()
fig_gente.add_trace(go.Scatter(
    x=days_gente, y=times_gente, mode="lines+markers", name="PipelineGenteDiario",
    line=dict(color=COR_GENTE, width=2), marker=dict(size=4),
    hovertemplate="%{x}<br>Entrega: %{text}<extra></extra>",
    text=[daily_gente[d]["max_end_brt"] for d in days_gente],
))
media_g = time_to_float(stats_gente[2])
fig_gente.add_hline(y=media_g, line_dash="dash", line_color="rgba(52,152,219,0.5)")
fig_gente.update_layout(
    title="G&C — Horário de Entrega",
    yaxis=dict(title="Horário", range=[10, 16],
               tickvals=[10, 11, 12, 13, 14, 15, 16],
               ticktext=["10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00"],
               gridcolor="#2c3e50"),
    xaxis=dict(gridcolor="#2c3e50", dtick="M1",
               tickvals=["2026-01-15", "2026-02-15", "2026-03-15", "2026-04-15", "2026-05-15"],
               ticktext=["jan/26", "fev/26", "mar/26", "abr/26", "mai/26"]),
    paper_bgcolor=COR_BG, plot_bgcolor=COR_BG, font=dict(color=COR_TEXT),
    height=350, margin=dict(t=70, b=40),
)

# Captação: Linha
fig_capt = go.Figure()
fig_capt.add_trace(go.Scatter(
    x=days_capcom, y=times_capcom, mode="lines+markers", name="CAPCOM",
    line=dict(color=COR_CAPCOM, width=2), marker=dict(size=4),
    hovertemplate="%{x}<br>CAPCOM: %{text}<extra></extra>",
    text=[daily_capcom[d]["max_end_brt"] for d in days_capcom],
))
fig_capt.add_trace(go.Scatter(
    x=days_relalun, y=times_relalun, mode="lines+markers", name="ALUNADO",
    line=dict(color=COR_RELALUN, width=2), marker=dict(size=4),
    hovertemplate="%{x}<br>ALUNADO: %{text}<extra></extra>",
    text=[daily_relalun[d]["max_end_brt"] for d in days_relalun],
))
media_c = time_to_float(stats_capcom[2])
media_r = time_to_float(stats_relalun[2])
fig_capt.add_hline(y=media_c, line_dash="dash", line_color="rgba(93,173,226,0.5)")
fig_capt.add_hline(y=media_r, line_dash="dash", line_color="rgba(230,126,34,0.5)")
fig_capt.update_layout(
    title="Captação e Alunado — Horário de Entrega",
    yaxis=dict(title="Horário", range=[6, 24],
               tickvals=[6, 8, 10, 12, 14, 16, 18, 20, 22, 24],
               ticktext=["06:00", "08:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00", "24:00"],
               gridcolor="#2c3e50"),
    xaxis=dict(gridcolor="#2c3e50", dtick="M1",
               tickvals=["2026-01-15", "2026-02-15", "2026-03-15", "2026-04-15", "2026-05-15"],
               ticktext=["jan/26", "fev/26", "mar/26", "abr/26", "mai/26"]),
    paper_bgcolor=COR_BG, plot_bgcolor=COR_BG, font=dict(color=COR_TEXT),
    height=430, margin=dict(t=70, b=80),
    legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
)

# Captação: Mensal
media_mensal_capcom = calc_mediana_mensal(daily_capcom)
media_mensal_relalun = calc_mediana_mensal(daily_relalun)
meses_labels = {"2026-01": "jan/26", "2026-02": "fev/26", "2026-03": "mar/26",
                "2026-04": "abr/26", "2026-05": "mai/26"}
all_meses = sorted(set(list(media_mensal_capcom.keys()) + list(media_mensal_relalun.keys())))

fig_mensal = go.Figure()
fig_mensal.add_trace(go.Bar(
    x=[meses_labels.get(m, m) for m in all_meses],
    y=[media_mensal_relalun.get(m, 0) for m in all_meses],
    name="ALUNADO", marker_color=COR_RELALUN,
    text=[f"{int(media_mensal_relalun.get(m, 0)//1):02d}:{int((media_mensal_relalun.get(m, 0)%1)*60):02d}" if media_mensal_relalun.get(m) else "" for m in all_meses],
    textposition="outside", textfont=dict(color=COR_TEXT),
))
fig_mensal.add_trace(go.Bar(
    x=[meses_labels.get(m, m) for m in all_meses],
    y=[media_mensal_capcom.get(m, 0) for m in all_meses],
    name="CAPCOM", marker_color=COR_CAPCOM,
    text=[f"{int(media_mensal_capcom.get(m, 0)//1):02d}:{int((media_mensal_capcom.get(m, 0)%1)*60):02d}" for m in all_meses],
    textposition="outside", textfont=dict(color=COR_TEXT),
))
fig_mensal.update_layout(
    title="Captação e Alunado — Horário Médio de Entrega por Mês",
    yaxis=dict(title="Horário", range=[4, 14],
               tickvals=[4, 6, 8, 10, 12, 14],
               ticktext=["04:00", "06:00", "08:00", "10:00", "12:00", "14:00"],
               gridcolor="#2c3e50"),
    xaxis=dict(title=""),
    barmode="group",
    paper_bgcolor=COR_BG, plot_bgcolor=COR_BG, font=dict(color=COR_TEXT),
    height=420, margin=dict(t=70, b=100),
    legend=dict(orientation="h", yanchor="top", y=-0.28, xanchor="center", x=0.5),
)

# ============================================================
# GERAR HTML
# ============================================================
div_gente = pio.to_html(fig_gente, full_html=False, include_plotlyjs=False)
div_capt = pio.to_html(fig_capt, full_html=False, include_plotlyjs=False)
div_mensal = pio.to_html(fig_mensal, full_html=False, include_plotlyjs=False)

updated_at = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")

html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Monitoramento Dados — Captação e G&C</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ background-color: #1a1a2e; color: #eaeaea; font-family: 'Segoe UI', Tahoma, sans-serif; margin: 0; padding: 20px; }}
        .back-btn {{ display: inline-block; margin: 10px 0 0 10px; padding: 8px 16px; background: #16213e; color: #00BCD4; border: 1px solid #333; border-radius: 8px; text-decoration: none; font-size: 0.9em; transition: border-color 0.2s; }}
        .back-btn:hover {{ border-color: #00BCD4; }}
        .header {{ text-align: center; padding: 20px 0; border-bottom: 1px solid #2c3e50; margin-bottom: 30px; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .header .subtitle {{ color: #7f8c8d; font-size: 14px; margin-top: 5px; }}
        .section {{ margin-bottom: 40px; padding: 20px; background: #16213e; border-radius: 12px; }}
        .section-title {{ font-size: 18px; font-weight: bold; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #2c3e50; }}
        .big-numbers {{ display: flex; justify-content: space-around; margin-bottom: 20px; flex-wrap: wrap; }}
        .big-number {{ text-align: center; padding: 15px 25px; background: #1a1a2e; border-radius: 8px; min-width: 120px; margin: 5px; }}
        .big-number .value {{ font-size: 32px; font-weight: bold; }}
        .big-number .label {{ font-size: 12px; color: #7f8c8d; margin-top: 5px; }}
        .chart-full {{ width: 100%; }}
        .success {{ color: #2ecc71; }}
    </style>
</head>
<body>
    <a href="index.html" class="back-btn">← Voltar</a>
    <div class="header">
        <h1>Monitoramento Dados — Time Captacao e G&C</h1>
        <div class="subtitle">Periodo: Janeiro a Maio 2026 | Atualizado: {updated_at}</div>
    </div>
    <div class="section">
        <div class="section-title" style="color: #3498db;">G&C</div>
        <div class="big-numbers">
            <div class="big-number"><div class="value success">{stats_gente[0]}</div><div class="label">Execucoes com Sucesso</div></div>
            <div class="big-number"><div class="value success">{stats_gente[1]:.1f}%</div><div class="label">Taxa de Sucesso</div></div>
            <div class="big-number"><div class="value" style="color:#3498db;">{stats_gente[2]}</div><div class="label">G&C (média)</div></div>
        </div>
        <div class="chart-full">{div_gente}</div>
    </div>
    <div class="section">
        <div class="section-title" style="color: #5dade2;">Captacao e Alunado</div>
        <div class="big-numbers">
            <div class="big-number"><div class="value success">{stats_capcom[0]}</div><div class="label">Execucoes com Sucesso</div></div>
            <div class="big-number"><div class="value success">{stats_capcom[1]:.1f}%</div><div class="label">Taxa de Sucesso</div></div>
            <div class="big-number"><div class="value" style="color:#5dade2;">{stats_capcom[2]}</div><div class="label">CAPCOM (média)</div></div>
            <div class="big-number"><div class="value" style="color:#e67e22;">{stats_relalun[2]}</div><div class="label">ALUNADO (média)</div></div>
        </div>
        <div class="chart-full">{div_capt}</div>
        <div class="chart-full" style="margin-top: 20px;">{div_mensal}</div>
    </div>
</body>
</html>"""

output_path = os.path.join(OUTPUT_DIR, "mwaa.html")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n✅ Dashboard gerado: {output_path}")
