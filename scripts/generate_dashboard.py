"""
KPIs Time Captação e G&C — Geração Automática via Jira API
Usa JQL 'status changed DURING' por sprint para determinar conclusão.
Cada card é contado apenas na sprint mais recente onde foi concluído/aceito.
Uso: python scripts/generate_dashboard.py
Requer: JIRA_USERNAME e JIRA_API_TOKEN como variáveis de ambiente.
"""
import os
import json
import base64
import urllib.request
import urllib.parse
import ssl
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime
from collections import defaultdict

# ============================================================
# CONFIGURAÇÃO
# ============================================================
JIRA_URL = "https://cogna.atlassian.net"
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", "anderson.fort@cogna.com.br")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN", "")

TEAM_MEMBERS = [
    "anderson.fort@cogna.com.br",
    "ananda.rado@cogna.com.br",
    "tiago.g.ferreira@cogna.com.br",
    "flavio.pires@cogna.com.br",
    "taisa.martins@cogna.com.br",
    "talitha.felix-lee@cogna.parceirosedu.com.br",
    "yuri.queiroz-jum@cogna.parceirosedu.com.br",
]

DONE_STATUSES = ("Concluído", "Aceito")
SP_FIELD = "customfield_10026"
BOARD_ID = 1094

OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SSL context
if os.environ.get("GITHUB_ACTIONS"):
    ssl_ctx = ssl.create_default_context()
else:
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE


def _auth_header():
    credentials = base64.b64encode(f"{JIRA_USERNAME}:{JIRA_API_TOKEN}".encode()).decode()
    return f"Basic {credentials}"


def _jira_get(url):
    req = urllib.request.Request(url, headers={
        "Authorization": _auth_header(),
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, context=ssl_ctx) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"  HTTP Error {e.code}: {error_body[:500]}")
        raise


def jira_search(jql, fields="summary,status,assignee,customfield_10026", max_results=100):
    """Busca issues no Jira com paginação."""
    all_issues = []
    start_at = 0
    while True:
        params = urllib.parse.urlencode({
            "jql": jql,
            "fields": fields,
            "maxResults": max_results,
            "startAt": start_at,
        })
        url = f"{JIRA_URL}/rest/api/3/search/jql?{params}"
        data = _jira_get(url)
        issues = data.get("issues", [])
        all_issues.extend(issues)
        total = data.get("total", 0)
        if len(all_issues) >= total or len(issues) == 0:
            break
        start_at += max_results
    return all_issues


def get_sprints_2026():
    """Busca sprints fechadas e ativas do board para 2026."""
    url = f"{JIRA_URL}/rest/agile/1.0/board/{BOARD_ID}/sprint?state=closed,active&maxResults=50"
    data = _jira_get(url)
    sprints_2026 = []
    for s in data.get("values", []):
        start = s.get("startDate", "")
        if start and "2026" in start:
            sprints_2026.append(s)
    return sorted(sprints_2026, key=lambda x: x.get("startDate", ""))


def format_sprint_label(sprint):
    name = sprint.get("name", "")
    start = sprint.get("startDate", "")[:10]
    end = sprint.get("endDate", "")[:10]
    try:
        start_fmt = datetime.strptime(start, "%Y-%m-%d").strftime("%d %b").lstrip("0").lower()
        end_fmt = datetime.strptime(end, "%Y-%m-%d").strftime("%d %b").lstrip("0").lower()
    except ValueError:
        start_fmt = start
        end_fmt = end
    suffix = "*" if sprint.get("state") == "active" else ""
    return f"{name}{suffix}<br><sub>{start_fmt} – {end_fmt}</sub>"


# ============================================================
# BUSCAR DADOS
# ============================================================
print("=" * 60)
print("KPIs Time Captação e G&C — JQL status changed DURING")
print("=" * 60)

# 1. Buscar sprints
print("\n[1/2] Buscando sprints do board 1094...")
sprints_data = get_sprints_2026()
print(f"      Sprints 2026: {len(sprints_data)}")
for s in sprints_data:
    print(f"        - {s['name']} ({s.get('startDate', '')[:10]} a {s.get('endDate', '')[:10]}) [{s.get('state')}]")

assignee_filter = ", ".join(f'"{m}"' for m in TEAM_MEMBERS)

# 2. Para cada sprint, buscar histórias e incidentes concluídos DURANTE o período
# Estratégia: buscar por sprint do mais recente ao mais antigo.
# Cada card é contado apenas na sprint mais recente (última conclusão).
print("\n[2/2] Buscando issues por sprint (status changed DURING)...")

# Processar sprints do mais recente ao mais antigo para garantir "última conclusão"
cards_already_counted = set()  # keys já contadas em sprint mais recente

sprint_labels = []
sp_por_sprint = []
historias_por_sprint = []
incidentes_por_sprint = []

# Primeiro, coletar dados na ordem reversa (mais recente primeiro)
sprint_results = []

for sprint in reversed(sprints_data):
    start_date = sprint.get("startDate", "")[:10]
    end_date = sprint.get("endDate", "")[:10]
    sprint_name = sprint.get("name", "")

    # Histórias concluídas/aceitas DURANTE este período
    hist_jql = (
        f'project = DENA AND issuetype = História AND '
        f'assignee in ({assignee_filter}) AND '
        f'status in ("Concluído", "Aceito") AND '
        f'status changed to ("Concluído", "Aceito") DURING ("{start_date}", "{end_date}") '
        f'ORDER BY key ASC'
    )
    historias = jira_search(hist_jql, fields=f"summary,status,assignee,{SP_FIELD}")

    # Incidentes concluídos/aceitos DURANTE este período
    inc_jql = (
        f'project = DENA AND issuetype = Incidente AND '
        f'assignee in ({assignee_filter}) AND '
        f'status in ("Concluído", "Aceito") AND '
        f'status changed to ("Concluído", "Aceito") DURING ("{start_date}", "{end_date}") '
        f'ORDER BY key ASC'
    )
    incidentes = jira_search(inc_jql, fields="summary,status,assignee")

    # Filtrar: só contar cards que NÃO foram contados em sprint mais recente
    sp_total = 0
    hist_count = 0
    hist_keys = []

    for issue in historias:
        key = issue.get("key")
        if key in cards_already_counted:
            continue
        cards_already_counted.add(key)
        fields = issue.get("fields", {})
        sp = fields.get(SP_FIELD) or 0
        sp_total += sp
        hist_count += 1
        hist_keys.append(f"{key}({sp})")

    inc_count = 0
    for issue in incidentes:
        key = issue.get("key")
        if key in cards_already_counted:
            continue
        cards_already_counted.add(key)
        inc_count += 1

    sprint_results.append({
        "sprint": sprint,
        "sp": sp_total,
        "historias": hist_count,
        "incidentes": inc_count,
        "hist_keys": hist_keys,
    })

    print(f"  {sprint_name}: {sp_total} SP | {hist_count} hist | {inc_count} inc | cards: {', '.join(hist_keys[:5])}{'...' if len(hist_keys) > 5 else ''}")

# Reverter para ordem cronológica
sprint_results.reverse()

for r in sprint_results:
    sprint_labels.append(format_sprint_label(r["sprint"]))
    sp_por_sprint.append(r["sp"])
    historias_por_sprint.append(r["historias"])
    incidentes_por_sprint.append(r["incidentes"])

print(f"\n  SP por sprint: {sp_por_sprint}")
print(f"  Histórias por sprint: {historias_por_sprint}")
print(f"  Incidentes por sprint: {incidentes_por_sprint}")


# ============================================================
# MÉTRICAS
# ============================================================
media_sp = round(sum(sp_por_sprint) / len(sp_por_sprint)) if sp_por_sprint else 0
media_incidentes = round(sum(incidentes_por_sprint) / len(incidentes_por_sprint), 1) if incidentes_por_sprint else 0
media_historias = round(sum(historias_por_sprint) / len(historias_por_sprint), 1) if historias_por_sprint else 0
total_sp = sum(sp_por_sprint)
total_historias = sum(historias_por_sprint)
total_incidentes = sum(incidentes_por_sprint)
sprints_completas = len([s for s in sprints_data if s.get("state") == "closed"])

print(f"\n  Total SP: {total_sp} | Média: {media_sp} SP/sprint")
print(f"  Total Histórias: {total_historias} | Média: {media_historias}/sprint")
print(f"  Total Incidentes: {total_incidentes} | Média: {media_incidentes}/sprint")


# ============================================================
# GRÁFICOS
# ============================================================
print("\nGerando gráficos...")

LAYOUT_TEMPLATE = dict(
    paper_bgcolor="#1a1a2e",
    plot_bgcolor="#16213e",
    font=dict(color="#e0e0e0", size=12),
    title_font=dict(size=16, color="#ffffff"),
    legend=dict(bgcolor="rgba(0,0,0,0.3)", bordercolor="#444"),
    xaxis=dict(gridcolor="rgba(255,255,255,0.08)", linecolor="rgba(255,255,255,0.15)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.08)", linecolor="rgba(255,255,255,0.15)"),
)

fig1 = go.Figure()
fig1.add_trace(go.Bar(
    x=sprint_labels, y=sp_por_sprint,
    text=[v if v > 0 else "" for v in sp_por_sprint],
    textposition="outside", textfont=dict(size=13, color="white"),
    marker_color="#00BCD4", showlegend=False,
))
fig1.add_hline(y=media_sp, line_dash="dash", line_color="#FFD700", line_width=2,
               annotation_text=f"Velocidade média: {media_sp} SP",
               annotation_position="top left",
               annotation_font=dict(color="#FFD700", size=11))
fig1.update_layout(title="Velocidade por Sprint (Story Points) — Time Captação e G&C (2026)",
                   xaxis_title="Sprint", yaxis_title="Story Points", **LAYOUT_TEMPLATE)

fig2 = go.Figure()
fig2.add_trace(go.Bar(
    x=sprint_labels, y=historias_por_sprint,
    text=[v if v > 0 else "" for v in historias_por_sprint],
    textposition="outside", textfont=dict(size=14, color="white"),
    marker_color="#4CAF50", showlegend=False,
))
fig2.add_hline(y=media_historias, line_dash="dash", line_color="#FFD700", line_width=2,
               annotation_text=f"Média: {media_historias} hist/sprint",
               annotation_position="top left",
               annotation_font=dict(color="#FFD700", size=11), annotation_yshift=15)
fig2.update_layout(title="Histórias Concluídas por Sprint — Time Captação e G&C (2026)",
                   xaxis_title="Sprint", yaxis_title="Quantidade", **LAYOUT_TEMPLATE)

fig3 = go.Figure()
fig3.add_trace(go.Bar(
    x=sprint_labels, y=incidentes_por_sprint,
    text=[v if v > 0 else "" for v in incidentes_por_sprint],
    textposition="outside", textfont=dict(size=13, color="white"),
    marker_color="#F44336", showlegend=False,
))
fig3.add_hline(y=media_incidentes, line_dash="dash", line_color="#FFD700", line_width=2,
               annotation_text=f"Média: {media_incidentes} inc/sprint",
               annotation_position="top left",
               annotation_font=dict(color="#FFD700", size=11))
fig3.update_layout(title="Incidentes Atendidos por Sprint — Time Captação e G&C (2026)",
                   xaxis_title="Sprint", yaxis_title="Quantidade", **LAYOUT_TEMPLATE)

sp_consumido = [round(inc * 2.25) for inc in incidentes_por_sprint]
fig4 = go.Figure()
fig4.add_trace(go.Bar(name="Entregue (histórias)", x=sprint_labels, y=sp_por_sprint,
                       text=[v if v > 0 else "" for v in sp_por_sprint],
                       textposition="inside", marker_color="#4CAF50"))
fig4.add_trace(go.Bar(name="Consumido (incidentes)", x=sprint_labels, y=sp_consumido,
                       text=[v if v > 0 else "" for v in sp_consumido],
                       textposition="inside", marker_color="#F44336"))
fig4.add_hline(y=media_sp, line_dash="dash", line_color="#FFD700", line_width=1.5,
               annotation_text=f"Capacidade média: {media_sp} SP",
               annotation_position="top left",
               annotation_font=dict(color="#FFD700", size=11), annotation_yshift=15)
fig4.update_layout(
    title="Impacto de Incidentes na Velocidade por Sprint<br><sub>~2.25 SP/incidente | Barras vermelhas = esforço consumido</sub>",
    xaxis_title="Sprint", yaxis_title="Story Points", barmode="stack",
    paper_bgcolor="#1a1a2e", plot_bgcolor="#16213e",
    font=dict(color="#e0e0e0", size=12), title_font=dict(size=16, color="#ffffff"),
    legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5,
                bgcolor="rgba(0,0,0,0.3)", bordercolor="#444"),
    xaxis=dict(gridcolor="rgba(255,255,255,0.08)", linecolor="rgba(255,255,255,0.15)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.08)", linecolor="rgba(255,255,255,0.15)"),
)


# ============================================================
# HTML
# ============================================================
print("Gerando HTML...")
div1 = pio.to_html(fig1, full_html=False, include_plotlyjs=False)
div2 = pio.to_html(fig2, full_html=False, include_plotlyjs=False)
div3 = pio.to_html(fig3, full_html=False, include_plotlyjs=False)
div4 = pio.to_html(fig4, full_html=False, include_plotlyjs=False)

updated_at = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")

dashboard_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>KPIs Time Captacao e G&C</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ background: #1a1a2e; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; }}
        .back-btn {{ display: inline-block; margin: 10px 0 0 10px; padding: 8px 16px; background: #16213e; color: #00BCD4; border: 1px solid #333; border-radius: 8px; text-decoration: none; font-size: 0.9em; transition: border-color 0.2s; }}
        .back-btn:hover {{ border-color: #00BCD4; }}
        .header {{ text-align: center; padding: 20px; }}
        .header h1 {{ color: #fff; margin: 0; font-size: 1.8em; }}
        .header p {{ color: #aaa; }}
        .kpi-row {{ display: flex; justify-content: center; gap: 20px; flex-wrap: wrap; margin: 10px 0; }}
        .kpi-card {{ background: #16213e; border-radius: 12px; padding: 20px 30px; text-align: center; min-width: 180px; border: 1px solid #333; }}
        .kpi-card .value {{ font-size: 2em; font-weight: bold; color: #00BCD4; }}
        .kpi-card .label {{ font-size: 0.85em; color: #aaa; margin-top: 5px; }}
        .chart-section {{ background: #16213e; border-radius: 12px; padding: 15px; border: 1px solid #333; margin: 20px 0; }}
        .chart-section .js-plotly-plot {{ width: 100% !important; height: 500px !important; }}
    </style>
</head>
<body>
    <a href="index.html" class="back-btn">← Voltar</a>
    <div class="header">
        <h1>KPIs — Time Captação e G&C</h1>
        <p>Período: 2026 | Atualizado em: {updated_at}</p>
    </div>
    <div class="kpi-row">
        <div class="kpi-card"><div class="value">{total_sp}</div><div class="label">Story Points Entregues</div></div>
        <div class="kpi-card"><div class="value">~{media_sp}</div><div class="label">SP/Sprint (média)</div></div>
        <div class="kpi-card"><div class="value">~2.25</div><div class="label">SP/Incidente (custo)</div></div>
    </div>
    <div class="kpi-row">
        <div class="kpi-card"><div class="value">{total_historias}</div><div class="label">Histórias Concluídas</div></div>
        <div class="kpi-card"><div class="value">{total_incidentes}</div><div class="label">Incidentes Atendidos</div></div>
        <div class="kpi-card"><div class="value">{sprints_completas}</div><div class="label">Sprints Completas</div></div>
    </div>
    <div style="text-align: center; margin: 30px auto 10px auto; padding: 10px 20px; width: fit-content; background: transparent; border-bottom: 1px solid rgba(255,215,0,0.3);">
        <span style="color: #FFD700; font-size: 0.8em; opacity: 0.8;">⚠ Disclaimer: 3 colaboradores full time atuando no Projeto de Migração para Arquitetura de Referência</span>
    </div>
    <div class="chart-section">{div1}</div>
    <div class="chart-section">{div2}</div>
    <div class="chart-section">{div3}</div>
    <div class="chart-section">{div4}</div>
</body>
</html>"""

output_path = os.path.join(OUTPUT_DIR, "kpis.html")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(dashboard_html)

print(f"\n{'=' * 60}")
print(f"✅ Dashboard gerado: {output_path}")
print(f"   SP total: {total_sp} | Média: {media_sp} SP/sprint")
print(f"   Histórias: {total_historias} | Incidentes: {total_incidentes}")
print(f"   Método: JQL status changed DURING (última sprint ganha)")
print(f"{'=' * 60}")
