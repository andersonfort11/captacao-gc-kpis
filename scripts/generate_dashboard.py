"""
KPIs Time Captação e G&C — Geração Automática via Jira API
Usa changelog para determinar em qual sprint cada card foi concluído.
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

# SSL context (desabilita verificação apenas localmente com proxy)
if os.environ.get("GITHUB_ACTIONS"):
    ssl_ctx = ssl.create_default_context()
else:
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE


def _auth_header():
    """Retorna header de autenticação Basic."""
    credentials = base64.b64encode(f"{JIRA_USERNAME}:{JIRA_API_TOKEN}".encode()).decode()
    return f"Basic {credentials}"


def _jira_get(url):
    """Faz GET autenticado no Jira e retorna JSON."""
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


def jira_search_with_changelog(jql, fields, max_results=100):
    """Busca issues no Jira com expand=changelog, com paginação."""
    all_issues = []
    start_at = 0

    while True:
        params = urllib.parse.urlencode({
            "jql": jql,
            "fields": fields,
            "expand": "changelog",
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


def get_completion_date(issue):
    """
    Percorre o changelog da issue para encontrar a data em que o status
    mudou para 'Concluído' ou 'Aceito'. Retorna a data mais recente
    dessa transição, ou None se não encontrada.
    """
    changelog = issue.get("changelog", {})
    histories = changelog.get("histories", [])

    completion_date = None

    for history in histories:
        created = history.get("created", "")
        for item in history.get("items", []):
            if item.get("field") == "status" and item.get("toString") in DONE_STATUSES:
                # Pega a data mais recente de conclusão
                if completion_date is None or created > completion_date:
                    completion_date = created

    return completion_date


def parse_datetime(date_str):
    """Parse ISO datetime string para datetime object."""
    if not date_str:
        return None
    # Remove milissegundos e ajusta timezone
    clean = date_str.split(".")[0].replace("+0000", "+00:00")
    if clean.endswith("Z"):
        clean = clean[:-1] + "+00:00"
    # Tenta parse com timezone offset
    try:
        # Formato: 2026-01-15T10:30:00+00:00 ou 2026-01-15T10:30:00
        if "+" in clean[10:] or clean[10:].count("-") > 0:
            # Has timezone
            return datetime.fromisoformat(clean)
        else:
            return datetime.fromisoformat(clean)
    except ValueError:
        # Fallback: pega apenas a parte de data
        try:
            return datetime.strptime(clean[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


def match_sprint(completion_date_str, sprints):
    """
    Dado uma data de conclusão (string ISO), encontra em qual sprint
    essa data se encaixa (entre startDate e endDate da sprint).
    Retorna o ID da sprint ou None.
    """
    if not completion_date_str:
        return None

    comp_dt = parse_datetime(completion_date_str)
    if not comp_dt:
        return None

    # Remove timezone info para comparação simples
    comp_naive = comp_dt.replace(tzinfo=None)

    for sprint in sprints:
        start_str = sprint.get("startDate", "")
        end_str = sprint.get("endDate", "") or sprint.get("completeDate", "")

        start_dt = parse_datetime(start_str)
        end_dt = parse_datetime(end_str)

        if not start_dt or not end_dt:
            continue

        start_naive = start_dt.replace(tzinfo=None)
        end_naive = end_dt.replace(tzinfo=None)

        if start_naive <= comp_naive <= end_naive:
            return sprint["id"]

    return None


def format_sprint_label(sprint):
    """Formata label da sprint com datas."""
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
# BUSCAR DADOS DO JIRA
# ============================================================
print("=" * 60)
print("KPIs Time Captação e G&C — Geração via Changelog")
print("=" * 60)

# 1. Buscar sprints de 2026
print("\n[1/3] Buscando sprints do board 1094...")
sprints_data = get_sprints_2026()
print(f"      Sprints 2026 encontradas: {len(sprints_data)}")
for s in sprints_data:
    print(f"        - {s['name']} ({s.get('startDate', '')[:10]} a {s.get('endDate', '')[:10]}) [{s.get('state')}]")

# 2. Buscar Histórias concluídas/aceitas do time
print("\n[2/3] Buscando Histórias concluídas (com changelog)...")
assignee_filter = ", ".join(f'"{m}"' for m in TEAM_MEMBERS)

historias_jql = (
    f'project = DENA AND issuetype = História AND '
    f'assignee in ({assignee_filter}) AND '
    f'status in ("Concluído", "Aceito") '
    f'ORDER BY created ASC'
)
historias = jira_search_with_changelog(
    historias_jql,
    fields=f"summary,status,assignee,{SP_FIELD},issuetype"
)
print(f"      Histórias encontradas: {len(historias)}")

# 3. Buscar Incidentes concluídos/aceitos do time
print("\n[3/3] Buscando Incidentes concluídos (com changelog)...")
incidentes_jql = (
    f'project = DENA AND issuetype = Incidente AND '
    f'assignee in ({assignee_filter}) AND '
    f'status in ("Concluído", "Aceito") '
    f'ORDER BY created ASC'
)
incidentes = jira_search_with_changelog(
    incidentes_jql,
    fields="summary,status,assignee,issuetype"
)
print(f"      Incidentes encontrados: {len(incidentes)}")


# ============================================================
# PROCESSAR: ASSOCIAR CADA ISSUE À SPRINT VIA CHANGELOG
# ============================================================
print("\nProcessando changelog para associar issues às sprints...")

# Mapear sprint_id -> dados acumulados
sprint_sp = defaultdict(float)
sprint_historias = defaultdict(int)
sprint_incidentes = defaultdict(int)

# Processar Histórias
historias_matched = 0
historias_unmatched = 0

for issue in historias:
    fields = issue.get("fields", {})

    # Validar assignee (dupla checagem)
    assignee = fields.get("assignee", {})
    if not assignee or assignee.get("emailAddress", "") not in TEAM_MEMBERS:
        continue

    # Validar tipo
    issue_type = fields.get("issuetype", {}).get("name", "")
    if issue_type != "História":
        continue

    # Encontrar data de conclusão via changelog
    completion_date = get_completion_date(issue)
    if not completion_date:
        historias_unmatched += 1
        continue

    # Associar à sprint
    sprint_id = match_sprint(completion_date, sprints_data)
    if sprint_id:
        sp = fields.get(SP_FIELD) or 0
        sprint_sp[sprint_id] += sp
        sprint_historias[sprint_id] += 1
        historias_matched += 1
    else:
        historias_unmatched += 1

print(f"  Histórias associadas a sprints: {historias_matched}")
print(f"  Histórias sem sprint (fora do range): {historias_unmatched}")

# Processar Incidentes
incidentes_matched = 0
incidentes_unmatched = 0

for issue in incidentes:
    fields = issue.get("fields", {})

    # Validar assignee
    assignee = fields.get("assignee", {})
    if not assignee or assignee.get("emailAddress", "") not in TEAM_MEMBERS:
        continue

    # Validar tipo
    issue_type = fields.get("issuetype", {}).get("name", "")
    if issue_type != "Incidente":
        continue

    # Encontrar data de conclusão via changelog
    completion_date = get_completion_date(issue)
    if not completion_date:
        incidentes_unmatched += 1
        continue

    # Associar à sprint
    sprint_id = match_sprint(completion_date, sprints_data)
    if sprint_id:
        sprint_incidentes[sprint_id] += 1
        incidentes_matched += 1
    else:
        incidentes_unmatched += 1

print(f"  Incidentes associados a sprints: {incidentes_matched}")
print(f"  Incidentes sem sprint (fora do range): {incidentes_unmatched}")


# ============================================================
# MONTAR ARRAYS POR SPRINT (na ordem cronológica)
# ============================================================
sprint_labels = []
sp_por_sprint = []
historias_por_sprint = []
incidentes_por_sprint = []

for sprint in sprints_data:
    sid = sprint["id"]
    sprint_labels.append(format_sprint_label(sprint))
    sp_por_sprint.append(sprint_sp.get(sid, 0))
    historias_por_sprint.append(sprint_historias.get(sid, 0))
    incidentes_por_sprint.append(sprint_incidentes.get(sid, 0))

print(f"\n  SP por sprint: {sp_por_sprint}")
print(f"  Histórias por sprint: {historias_por_sprint}")
print(f"  Incidentes por sprint: {incidentes_por_sprint}")


# ============================================================
# CALCULAR MÉTRICAS
# ============================================================
media_sp = round(sum(sp_por_sprint) / len(sp_por_sprint)) if sp_por_sprint else 0
media_incidentes = round(sum(incidentes_por_sprint) / len(incidentes_por_sprint), 1) if incidentes_por_sprint else 0
media_historias = round(sum(historias_por_sprint) / len(historias_por_sprint), 1) if historias_por_sprint else 0
total_sp = sum(sp_por_sprint)
total_historias = sum(historias_por_sprint)
total_incidentes = sum(incidentes_por_sprint)
sprints_completas = len([s for s in sprints_data if s.get("state") == "closed"])

print(f"\n  Métricas finais:")
print(f"    Total SP: {total_sp} | Média: {media_sp} SP/sprint")
print(f"    Total Histórias: {total_historias} | Média: {media_historias}/sprint")
print(f"    Total Incidentes: {total_incidentes} | Média: {media_incidentes}/sprint")
print(f"    Sprints completas: {sprints_completas}")


# ============================================================
# GERAR GRÁFICOS
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

# Gráfico 1: Velocidade por Sprint (SP)
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

# Gráfico 2: Histórias Concluídas por Sprint
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
               annotation_font=dict(color="#FFD700", size=11),
               annotation_yshift=15)
fig2.update_layout(title="Histórias Concluídas por Sprint — Time Captação e G&C (2026)",
                   xaxis_title="Sprint", yaxis_title="Quantidade", **LAYOUT_TEMPLATE)

# Gráfico 3: Incidentes por Sprint
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

# Gráfico 4: Impacto de Incidentes
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
               annotation_font=dict(color="#FFD700", size=11),
               annotation_yshift=15)
fig4.update_layout(
    title="Impacto de Incidentes na Velocidade por Sprint<br><sub>~2.25 SP/incidente | Barras vermelhas = esforço consumido</sub>",
    xaxis_title="Sprint", yaxis_title="Story Points", barmode="stack",
    paper_bgcolor="#1a1a2e", plot_bgcolor="#16213e",
    font=dict(color="#e0e0e0", size=12), title_font=dict(size=16, color="#ffffff"),
    legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5, bgcolor="rgba(0,0,0,0.3)", bordercolor="#444"),
    xaxis=dict(gridcolor="rgba(255,255,255,0.08)", linecolor="rgba(255,255,255,0.15)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.08)", linecolor="rgba(255,255,255,0.15)"),
)


# ============================================================
# GERAR HTML
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
print(f"   Método: changelog-based (sem duplicação entre sprints)")
print(f"{'=' * 60}")

