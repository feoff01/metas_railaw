# app.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
from typing import Dict, List, Optional, Set, Tuple
import psycopg2
from psycopg2 import sql
import re
from collections import defaultdict
import csv
import io
import random

# ====== CREDENCIAIS AWS (SP) ======
DB_HOST = "bdbarsi.clquwys0y8y8.sa-east-1.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "Barsi_admin"
DB_PASS = "*4482Barsi"
DB_SSLMODE = "require"

SCHEMA = "public"
TBL_REGEX = re.compile(
    r"^relatorio_positivador_(janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)_2025$",
    re.IGNORECASE
)

MESES_MAP = {
    "janeiro": "2025-01", "fevereiro": "2025-02",
    "marco": "2025-03", "março": "2025-03",
    "abril": "2025-04", "maio": "2025-05", "junho": "2025-06",
    "julho": "2025-07", "agosto": "2025-08", "setembro": "2025-09",
    "outubro": "2025-10", "novembro": "2025-11", "dezembro": "2025-12",
}
MESES_LABEL = {
    "2025-01":"Jan","2025-02":"Fev","2025-03":"Mar","2025-04":"Abr",
    "2025-05":"Mai","2025-06":"Jun","2025-07":"Jul","2025-08":"Ago",
    "2025-09":"Set","2025-10":"Out","2025-11":"Nov","2025-12":"Dez"
}

# ==== Pydantic models ====
class LinhaTabela(BaseModel):
    assessor: str
    mes: str
    ativacoes: int
    captacao: float
    receita: float

# NOVO: linha de detalhamento pedida
class LinhaDetalhe(BaseModel):
    assessor: str         # 1ª
    cliente: str          # 2ª
    ativou_em_m: str      # 4ª (Sim/Não)
    evadiu_em_m: str      # 5ª (Sim/Não)
    net_em_m: float       # 6ª
    receita_no_mes: float # 7ª
    captacao_liquida_em_m: float  # 8ª
    mes: str              # (AAAA-MM) — mantido no payload/CSV
    mes_nome: str         # (Jan/Fev/...)

class MetasModel(BaseModel):
    ativacoes: Dict[str, int]
    captacao: Dict[str, float]
    receita: Dict[str, float]

class MetricasPayload(BaseModel):
    series: List[str]
    meses: List[str]
    ativacoes: Dict[str, Dict[str, int]]
    captacao: Dict[str, Dict[str, float]]
    receita: Dict[str, Dict[str, float]]
    metas: MetasModel
    tabela: List[LinhaTabela]
    detalhes: List[LinhaDetalhe]  # NOVO

app = FastAPI(title="Positivadores 2025 • Ativações, Captação e Receita")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode=DB_SSLMODE
    )

def listar_tabelas_positivador_2025(limit: Optional[int] = None) -> List[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = %s
              AND tablename ~ %s
            ORDER BY tablename;
        """, (SCHEMA, r'^relatorio_positivador_.*_2025$'))
        rows = cur.fetchall()
    nomes = [t for (t,) in rows if TBL_REGEX.match(t)]
    return nomes[:limit] if limit else nomes

def mes_from_table(tname: str) -> str:
    m = TBL_REGEX.match(tname)
    if not m: return "2025-00"
    token = m.group(1).lower().replace("ç", "c")
    return MESES_MAP.get(token, "2025-00")

# Colunas candidatas (variações comuns)
COL_ATIV_CAND   = ["ativou_em_m", "Ativou em M?", "ativou_em_mes", "ativou_m"]
COL_EVAD_CAND   = ["evadiu_em_m", "Evadiu em M?", "evadiu_em_mes", "evadiu_m"]
COL_CAPT_CAND   = ["captacao_liquida_em_m", "Captação Líquida em M", "captacao_em_m", "captacao_liq_m"]
COL_RECE_CAND   = ["receita_no_mes", "Receita no Mês", "receita_mes", "receita_em_m"]
COL_NET_CAND    = ["net_em_m", "Net em M", "net_m", "soma_net_em_m"]
COL_CLIENTE_CAND= ["cliente", "id_cliente", "cod_cliente"]

def descobrir_coluna(conn, schema: str, tabela: str, candidatas: List[str], fallback_contains: str) -> str:
    with conn.cursor() as cur:
        cur.execute("""
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema=%s AND table_name=%s
        """, (schema, tabela))
        cols = {r[0] for r in cur.fetchall()}
    for c in candidatas:
        for col in cols:
            if col.lower() == c.lower():
                return col
    for col in cols:
        if fallback_contains in col.lower():
            return col
    raise RuntimeError(f"Coluna não encontrada em {schema}.{tabela}: {candidatas[0]} (ou similares)")

def _ident(col: str):
    return sql.Identifier(col) if '"' not in col else sql.SQL(f'"{col}"')

def consultar_metricas(conn, schema: str, tabela: str, col_ativ: str, col_capt: str, col_rece: str):
    col_ativ_sql = _ident(col_ativ)
    col_capt_sql = _ident(col_capt)
    col_rece_sql = _ident(col_rece)
    q = sql.SQL("""
        SELECT
            assessor,
            SUM(CASE
                  WHEN {col_ativ} ILIKE 'sim%' THEN 1
                  WHEN {col_ativ}::text IN ('1','true','t') THEN 1
                  ELSE 0
                END) AS ativacoes,
            SUM(COALESCE(NULLIF(REPLACE({col_capt}::text, ',', '.'), '')::numeric,0)) AS captacao,
            SUM(COALESCE(NULLIF(REPLACE({col_rece}::text, ',', '.'), '')::numeric,0)) AS receita
        FROM {sch}.{tb}
        GROUP BY assessor
    """).format(
        col_ativ=col_ativ_sql, col_capt=col_capt_sql, col_rece=col_rece_sql,
        sch=sql.Identifier(schema), tb=sql.Identifier(tabela)
    )
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()

# -------- NOVO: consulta detalhada por linha (sem agregar) ----------
def _as_sim_nao(v) -> str:
    if v is None: return "Não"
    s = str(v).strip().lower()
    if s in ("1","true","t","sim","s","y","yes"): return "Sim"
    return "Não"

def _as_float(v) -> float:
    if v is None: return 0.0
    s = str(v).strip()
    if s == "": return 0.0
    s = s.replace(".", "").replace(",", ".") if s.count(",") and s.count(".")>1 else s.replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def consultar_detalhes(conn, schema: str, tabela: str,
                       col_assessor: str, col_cliente: str,
                       col_ativ: str, col_evad: str,
                       col_net: str, col_rece: str, col_capt: str) -> List[Tuple]:
    q = sql.SQL("""
        SELECT
            {assessor}::text,
            {cliente}::text,
            {ativ}::text,
            {evad}::text,
            {net},
            {rece},
            {capt}
        FROM {sch}.{tb}
    """).format(
        assessor=_ident(col_assessor), cliente=_ident(col_cliente),
        ativ=_ident(col_ativ), evad=_ident(col_evad),
        net=_ident(col_net), rece=_ident(col_rece), capt=_ident(col_capt),
        sch=sql.Identifier(schema), tb=sql.Identifier(tabela)
    )
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()

def _seeded_rng(key: str) -> random.Random:
    h = abs(hash(key)) & 0xFFFFFFFF
    return random.Random(h)

def montar_payload(limit_tables: Optional[int] = None,
                   assessores_filter: Optional[Set[str]] = None) -> MetricasPayload:
    tabelas = listar_tabelas_positivador_2025(limit=limit_tables)

    ativ_matrix = defaultdict(lambda: defaultdict(int))
    capt_matrix = defaultdict(lambda: defaultdict(float))
    rece_matrix = defaultdict(lambda: defaultdict(float))
    linhas: List[LinhaTabela] = []
    detalhes: List[LinhaDetalhe] = []

    with get_conn() as conn:
        for t in tabelas:
            mes = mes_from_table(t)

            # descobre colunas (agregados)
            col_ativ = descobrir_coluna(conn, SCHEMA, t, COL_ATIV_CAND, "ativ")
            col_capt = descobrir_coluna(conn, SCHEMA, t, COL_CAPT_CAND, "capta")
            col_rece = descobrir_coluna(conn, SCHEMA, t, COL_RECE_CAND, "receit")

            # métricas agregadas por assessor (mantém gráficos/cards)
            for assessor, ativ, capt, rece in consultar_metricas(conn, SCHEMA, t, col_ativ, col_capt, col_rece):
                ass = str(assessor)
                if assessores_filter and ass not in assessores_filter:
                    continue
                a = int(ativ or 0)
                c = float(capt or 0.0)
                r = float(rece or 0.0)
                ativ_matrix[ass][mes] += a
                capt_matrix[ass][mes] += c
                rece_matrix[ass][mes] += r
                linhas.append(LinhaTabela(assessor=ass, mes=mes, ativacoes=a, captacao=c, receita=r))

            # ----------- DETALHES (linha a linha) -----------
            col_assessor = "assessor"
            col_cliente  = descobrir_coluna(conn, SCHEMA, t, COL_CLIENTE_CAND, "client")
            col_evad     = descobrir_coluna(conn, SCHEMA, t, COL_EVAD_CAND, "evad")
            col_net      = descobrir_coluna(conn, SCHEMA, t, COL_NET_CAND, "net")

            raw_rows = consultar_detalhes(
                conn, SCHEMA, t,
                col_assessor, col_cliente,
                col_ativ, col_evad,
                col_net, col_rece, col_capt
            )

            for r in raw_rows:
                ass = str(r[0]) if r[0] is not None else ""
                if assessores_filter and ass not in assessores_filter:
                    continue
                cliente = str(r[1]) if r[1] is not None else ""
                ativou  = _as_sim_nao(r[2])
                evadiu  = _as_sim_nao(r[3])
                net     = _as_float(r[4])
                receita = _as_float(r[5])
                capt    = _as_float(r[6])
                detalhes.append(
                    LinhaDetalhe(
                        assessor=ass,
                        cliente=cliente,
                        ativou_em_m=ativou,
                        evadiu_em_m=evadiu,
                        net_em_m=net,
                        receita_no_mes=receita,
                        captacao_liquida_em_m=capt,
                        mes=mes,
                        mes_nome=MESES_LABEL.get(mes, mes)
                    )
                )

    meses_presentes = sorted(
        {m for a in ativ_matrix for m in ativ_matrix[a].keys()} |
        {m for a in capt_matrix for m in capt_matrix[a].keys()} |
        {m for a in rece_matrix for m in rece_matrix[a].keys()}
    )
    assessores = sorted(set(list(ativ_matrix.keys()) + list(capt_matrix.keys()) + list(rece_matrix.keys())))

    ativacoes = {a: {m: int(ativ_matrix[a].get(m, 0)) for m in meses_presentes} for a in assessores}
    captacao  = {a: {m: float(capt_matrix[a].get(m, 0.0)) for m in meses_presentes} for a in assessores}
    receita   = {a: {m: float(rece_matrix[a].get(m, 0.0)) for m in meses_presentes} for a in assessores}

    # ---------- metas aleatórias (determinísticas) ----------
    metas_ativ: Dict[str, int] = {}
    metas_capt: Dict[str, float] = {}
    metas_rece: Dict[str, float] = {}
    for a in assessores:
        total_a = sum(ativacoes[a].values())
        total_c = sum(captacao[a].values())
        total_r = sum(receita[a].values())
        rng_a = _seeded_rng(a + "_ativ")
        rng_c = _seeded_rng(a + "_capt")
        rng_r = _seeded_rng(a + "_rece")
        f_a = rng_a.uniform(0.80, 1.40)
        f_c = rng_c.uniform(0.85, 1.35)
        f_r = rng_r.uniform(0.90, 1.30)
        metas_ativ[a] = max(1, int(round(total_a * f_a))) if total_a > 0 else rng_a.randint(2, 10)
        mc = total_c * f_c if total_c > 0 else rng_c.uniform(30000, 300000)
        mr = total_r * f_r if total_r > 0 else rng_r.uniform(20000, 200000)
        metas_capt[a] = float(int(mc // 1000) * 1000)
        metas_rece[a] = float(int(mr // 1000) * 1000)
    metas = MetasModel(ativacoes=metas_ativ, captacao=metas_capt, receita=metas_rece)

    return MetricasPayload(
        series=assessores, meses=meses_presentes,
        ativacoes=ativacoes, captacao=captacao, receita=receita,
        metas=metas, tabela=linhas, detalhes=detalhes
    )

def listar_assessores() -> List[str]:
    asses: Set[str] = set()
    tabelas = listar_tabelas_positivador_2025()
    with get_conn() as conn, conn.cursor() as cur:
        for t in tabelas:
            cur.execute(sql.SQL("""SELECT DISTINCT assessor FROM {sch}.{tb}""")
                       .format(sch=sql.Identifier(SCHEMA), tb=sql.Identifier(t)))
            for (a,) in cur.fetchall():
                asses.add(str(a))
    return sorted(asses, key=lambda x: (len(x), x))

# ====== Endpoints ======
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/tabelas", response_model=List[str])
def api_tabelas():
    return listar_tabelas_positivador_2025()

@app.get("/api/assessores", response_model=List[str])
def api_assessores():
    return listar_assessores()

@app.get("/api/metricas", response_model=MetricasPayload)
def api_metricas(
    limit_tables: Optional[int] = Query(default=None, ge=1),
    assessores: Optional[str] = Query(default=None, description="CSV de IDs de assessor")
):
    filtro = None
    if assessores:
        filtro = {s.strip() for s in assessores.split(",") if s.strip()}
    return montar_payload(limit_tables=limit_tables, assessores_filter=filtro)

@app.get("/api/metricas_csv")
def api_metricas_csv(
    limit_tables: Optional[int] = Query(default=None, ge=1),
    assessores: Optional[str] = Query(default=None)
):
    filtro = None
    if assessores:
        filtro = {s.strip() for s in assessores.split(",") if s.strip()}
    payload = montar_payload(limit_tables=limit_tables, assessores_filter=filtro)

    buf = io.StringIO()
    w = csv.writer(buf)
    # CSV dos DETALHES (colunas pedidas)
    w.writerow(["assessor","cliente","ativou_em_m","evadiu_em_m","net_em_m","receita_no_mes","captacao_liquida_em_m","mes","mes_nome"])
    for d in payload.detalhes:
        w.writerow([d.assessor, d.cliente, d.ativou_em_m, d.evadiu_em_m, f"{d.net_em_m:.2f}", f"{d.receita_no_mes:.2f}", f"{d.captacao_liquida_em_m:.2f}", d.mes, d.mes_nome])
    csv_bytes = buf.getvalue().encode("utf-8")
    return Response(content=csv_bytes, media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=detalhes_filtrado.csv"})

# ====== Frontend ======
@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!DOCTYPE html>
<html lang="pt-br"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Positivadores 2025 • Ativações / Captação / Receita</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  :root{
    --bg:#0b0c12; --card:#121424; --card2:#0e1020; --stroke:#23283b;
    --text:#e8ebff; --muted:#9fb0d9;
    --green:#4caf50; --green-800:#2e7d32;
    --amber:#ffb300; --amber-900:#996f00;
    --blue:#82aaff; --accent2:#5ad6b0;
  }
  *{box-sizing:border-box}
  body{margin:0;background:linear-gradient(180deg,#090a12 0%,#0b0c12 60%,#0b0c12 100%);color:var(--text);font-family:Inter,system-ui,Segoe UI,Roboto,Arial,sans-serif}
  header{position:sticky;top:0;z-index:5;background:rgba(9,10,18,.7);backdrop-filter:blur(10px);border-bottom:1px solid #1a1f33}
  header .bar{max-width:1400px;margin:0 auto;display:flex;align-items:center;gap:16px;padding:12px 20px}
  .brand{font-weight:700;letter-spacing:.2px}
  .wrap{max-width:1400px;margin:0 auto;padding:22px}

  .cards{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}
  .card{background:var(--card);border:1px solid var(--stroke);border-radius:18px;padding:16px;box-shadow:0 10px 30px rgba(0,0,0,.35)}
  .col-4{grid-column:span 4} .col-12{grid-column:span 12}
  @media (max-width:1100px){ .col-4{grid-column:span 12} }

  .kpicard h3{margin:0 0 2px 0;font-size:18px}
  .kpicard .meta{color:var(--muted);font-size:12px;margin-bottom:8px}
  .kpicard .valor{font-size:26px;font-weight:800;margin-bottom:6px}
  .kpicard .pct{font-weight:800;border-radius:12px;padding:2px 8px;display:inline-block}
  .pct.green{background:#16351a;color:#9ef8a1;border:1px solid #235a2a}
  .pct.amber{background:#3a2d00;color:#ffec99;border:1px solid #6b5200}
  .barwrap{height:10px;background:#1b2033;border:1px solid #2b3353;border-radius:999px;overflow:hidden;margin-top:10px}
  .bar{height:100%}
  .bar.green{background:linear-gradient(90deg,#2e7d32,#4caf50)}
  .bar.amber{background:linear-gradient(90deg,#996f00,#ffb300)}

  .toolbar{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin:16px 0 12px}
  .select{min-width:360px;position:relative}
  .select input{width:100%;padding:11px 12px;border:1px solid var(--stroke);background:var(--card2);color:var(--text);border-radius:12px;outline:none}
  .dropdown{position:absolute;top:48px;left:0;right:0;max-height:300px;overflow:auto;background:var(--card2);border:1px solid var(--stroke);border-radius:12px;display:none;z-index:10}
  .opt{padding:9px 10px;border-bottom:1px solid #1c2140;cursor:pointer;display:flex;gap:8px;align-items:center}
  .opt:hover{background:#171b33}
  .chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px;max-height:64px;overflow:auto}
  .chip{background:var(--card2);border:1px solid var(--stroke);border-radius:999px;padding:4px 10px;font-size:12px}
  .btn{padding:10px 12px;border:1px solid var(--stroke);background:var(--card2);color:var(--text);border-radius:12px;cursor:pointer}
  .btn:hover{filter:brightness(1.08)}

  .chart{
    height:260px;
    border-radius:14px;
    overflow:hidden;
    box-shadow:0 12px 30px rgba(0,0,0,.35), inset 0 0 0 1px rgba(255,255,255,.03);
  }
  .card h3{font-size:16px;font-weight:700;letter-spacing:.2px;margin:0 0 6px;color:#dfe6ff}

  #chart{display:none}

  /* ======= NOVO CSS DA TABELA (preenche container + sticky header + primeira coluna fixa) ======= */
  .tablewrap{
    max-height:58vh;
    overflow:auto;
    border:1px solid var(--stroke);
    border-radius:12px;
    margin-top:8px;
  }
  table{
    width:100%;
    table-layout:fixed;
    border-collapse:separate;
    border-spacing:0;
    margin:0;
  }
  thead th{
    position:sticky;
    top:0;
    background:#12152a;
    border-bottom:1px solid var(--stroke);
    z-index:3;
  }
  th,td{
    padding:10px;
    border-bottom:1px solid #20243a;
    overflow:hidden;
    text-overflow:ellipsis;
    white-space:nowrap;
  }
  th:first-child, td:first-child{
    position:sticky;
    left:0;
    background:#0f1220;
    border-right:1px solid #1e2136;
    z-index:2;
  }
  thead th:first-child{
    z-index:4;
  }
  /* ========================================================= */

  .muted{color:var(--muted);font-size:12px} .right{margin-left:auto} .small{font-size:12px}
  .loading{opacity:.5;pointer-events:none}
</style></head>
<body>
<header>
  <div class="bar">
    <div class="brand">Positivadores 2025 • Ativações / Captação / Receita</div>
    <div class="muted small">Dados do PostgreSQL (AWS) + Metas simuladas</div>
    <div class="right"></div>
    <a class="btn" id="btnCSV" href="#" download>Baixar CSV</a>
  </div>
</header>

<div class="wrap">
  <div class="cards">
    <div class="card kpicard col-4" id="cardAtiv">
      <h3>Ativação</h3>
      <div class="meta">Meta <b id="metaAtiv">—</b></div>
      <div class="valor" id="valAtiv">—</div>
      <span class="pct green" id="pctAtiv">—</span>
      <div class="barwrap"><div class="bar green" id="barAtiv" style="width:0%"></div></div>
    </div>
    <div class="card kpicard col-4" id="cardCapt">
      <h3>Captação</h3>
      <div class="meta">Meta <b id="metaCapt">—</b></div>
      <div class="valor" id="valCapt">—</div>
      <span class="pct amber" id="pctCapt">—</span>
      <div class="barwrap"><div class="bar amber" id="barCapt" style="width:0%"></div></div>
    </div>
    <div class="card kpicard col-4" id="cardRece">
      <h3>Receita</h3>
      <div class="meta">Meta <b id="metaRece">—</b></div>
      <div class="valor" id="valRece">—</div>
      <span class="pct green" id="pctRece">—</span>
      <div class="barwrap"><div class="bar green" id="barRece" style="width:0%"></div></div>
    </div>
  </div>

  <div class="card col-12" style="margin-top:16px">
    <div class="toolbar">
      <div class="select" id="assSel">
        <input id="assSearch" placeholder="Filtrar assessor... (digite para buscar, clique para abrir)" autocomplete="off"/>
        <div class="dropdown" id="assDrop"></div>
        <div class="chips" id="chips"></div>
      </div>
      <button class="btn" id="btnAll">Selecionar todos</button>
      <button class="btn" id="btnClear">Limpar</button>

      <div class="right"></div>
      <div class="toolbar" style="gap:8px;margin:0">
        <button class="btn" id="segLine" title="Linhas">Linhas</button>
        <button class="btn" id="segBar"  title="Barras empilhadas">Barras</button>
      </div>
    </div>

    <div class="cards">
      <div class="card col-4">
        <h3 style="margin:0 0 6px">Ativação</h3>
        <div id="chartAtiv" class="chart"></div>
      </div>
      <div class="card col-4">
        <h3 style="margin:0 0 6px">Captação</h3>
        <div id="chartCapt" class="chart"></div>
      </div>
      <div class="card col-4">
        <h3 style="margin:0 0 6px">Receita</h3>
        <div id="chartRece" class="chart"></div>
      </div>
    </div>
  </div>

  <!-- Tabela de Detalhamento (sem coluna AAAA-MM na UI) -->
  <div class="card col-12">
    <h3 style="margin:0 0 8px">Detalhamento (linhas da AWS)</h3>
    <div class="muted small">Colunas: Assessor, Cliente, Ativou em M?, Evadiu em M?, Net em M, Receita no Mês, Captação Líquida em M, Mês (nome)</div>
    <div id="tbl" class="tablewrap"></div>
  </div>
</div>

<script>
let ALL_ASSESSORES = [];
let SELECTED = new Set();
let CHART_MODE = 'line';
let DATA = null;

const COLORWAY = [
  '#82aaff','#5ad6b0','#ffd166','#ef476f','#06d6a0',
  '#f78c6b','#c792ea','#29b6f6','#ff9f1c','#8bd450',
  '#64b5f6','#9ccc65','#ffb74d','#ba68c8'
];

const BASE_LAYOUT = {
  paper_bgcolor:'#121424',
  plot_bgcolor:'#121424',
  font:{color:'#e6e9ff', family:'Inter, system-ui, Segoe UI, Roboto, Arial, sans-serif', size:12},
  margin:{l:56,r:14,t:6,b:40},
  hovermode:'x unified',
  hoverlabel:{bgcolor:'#0e1020', bordercolor:'#2b3353', font:{size:12}},
  legend:{orientation:'h', y:1.08, x:0, xanchor:'left', bgcolor:'rgba(14,16,32,.6)', bordercolor:'#2b3353', borderwidth:1, font:{size:11}},
  xaxis:{title:'Mês', tickangle:0, gridcolor:'#26304b', zerolinecolor:'#2c3656', linecolor:'#2b3353', tickfont:{size:11}},
  yaxis:{gridcolor:'#26304b', zerolinecolor:'#2c3656', linecolor:'#2b3353', tickfont:{size:11}},
  colorway: COLORWAY
};

function el(id){ return document.getElementById(id); }
function show(e,b){ e.style.display = b ? 'block':'none'; }

async function fetchAssessores(){ const r = await fetch('/api/assessores'); return await r.json(); }
async function fetchPayload(){
  const list = [...SELECTED];
  const query = list.length ? '?assessores='+encodeURIComponent(list.join(',')) : '';
  const r = await fetch('/api/metricas'+query);
  return await r.json();
}

function renderDropdown(filter=''){
  const box = el('assDrop');
  const f = filter.trim().toLowerCase();
  const arr = f ? ALL_ASSESSORES.filter(a => a.toLowerCase().includes(f)) : ALL_ASSESSORES.slice(0,600);
  box.innerHTML = arr.map(a => {
    const checked = SELECTED.has(a) ? '✓' : '';
    return `<div class="opt" data-value="${a}">
              <div style="width:18px;text-align:center">${checked}</div>
              <div>${a}</div>
            </div>`;
  }).join('') || `<div class="opt muted">Nenhum encontrado</div>`;
  show(box, true);
}
function renderChips(){
  const chips = el('chips'); const list = [...SELECTED].sort();
  chips.innerHTML = list.slice(0,30).map(a => `<span class="chip">${a}</span>`).join('') +
                    (list.length>30 ? `<span class="chip">+${list.length-30}</span>`:'');
}

function fmtRS(v){ return v.toLocaleString('pt-BR',{style:'currency',currency:'BRL'}); }
function fmtNum(v){ return v.toLocaleString('pt-BR',{minimumFractionDigits:2, maximumFractionDigits:2}); }

function sumSelected(metricDict){
  const series = DATA.series.filter(a => SELECTED.size ? SELECTED.has(a) : true);
  const meses = DATA.meses;
  let tot = 0;
  series.forEach(a => meses.forEach(m => { tot += (metricDict[a]?.[m] || 0); }));
  return tot;
}
function metaSelected(metasDict){
  const series = DATA.series.filter(a => SELECTED.size ? SELECTED.has(a) : true);
  return series.reduce((s,a)=> s + (metasDict[a] || 0), 0);
}
function pct(a,b){ if(!b || b<=0) return 0; return Math.max(0, Math.min(100, (a/b)*100)); }

function updateMainCards(){
  const ativ = sumSelected(DATA.ativacoes);
  const metaA = metaSelected(DATA.metas.ativacoes);
  const pA = pct(ativ, metaA);

  const capt = sumSelected(DATA.captacao);
  const metaC = metaSelected(DATA.metas.captacao);
  const pC = pct(capt, metaC);

  const rece = sumSelected(DATA.receita);
  const metaR = metaSelected(DATA.metas.receita);
  const pR = pct(rece, metaR);

  el('valAtiv').textContent = ativ.toLocaleString('pt-BR');
  el('metaAtiv').textContent = metaA.toLocaleString('pt-BR');
  el('pctAtiv').textContent = (pA||0).toFixed(0) + '%';
  el('barAtiv').style.width = (pA||0).toFixed(0) + '%';

  el('valCapt').textContent = fmtRS(capt);
  el('metaCapt').textContent = fmtRS(metaC);
  el('pctCapt').textContent = (pC||0).toFixed(0) + '%';
  el('barCapt').style.width = (pC||0).toFixed(0) + '%';

  el('valRece').textContent = fmtRS(rece);
  el('metaRece').textContent = fmtRS(metaR);
  el('pctRece').textContent = (pR||0).toFixed(0) + '%';
  el('barRece').style.width = (pR||0).toFixed(0) + '%';
}

function lineStyleFor(count){ return count <= 6 ? {shape:'spline', smoothing:0.6, width:2.4} : {shape:'linear', width:2}; }
function maybeFillFor(count){ return count <= 3 ? 'tozeroy' : 'none'; }

function plotMetric(metric, series, meses, payload, targetDiv){
  const mesesTicks = meses.map(m => ({ "2025-01":"Jan","2025-02":"Fev","2025-03":"Mar","2025-04":"Abr","2025-05":"Mai","2025-06":"Jun","2025-07":"Jul","2025-08":"Ago","2025-09":"Set","2025-10":"Out","2025-11":"Nov","2025-12":"Dez" }[m] || m));
  const dados    = payload[metric];
  const isAtiv   = metric === 'ativacoes';
  const axisTitle= isAtiv ? 'Ativações (unid.)' : (metric==='captacao' ? 'Captação (R$)' : 'Receita (R$)');
  const lconf = lineStyleFor(series.length);
  const fill  = maybeFillFor(series.length);

  let traces;
  if(CHART_MODE === 'bar'){
    traces = series.map(a => ({
      x: meses, y: meses.map(m => dados[a]?.[m]||0), type: 'bar', name: a,
      marker: { line:{width:0}, opacity:.92 },
      hovertemplate: `<b>${a}</b><br>%{x}<br>` + (isAtiv ? 'Ativações: %{y}' : 'R$ %{y:,.2f}') + `<extra></extra>`
    }));
  } else {
    traces = series.map(a => ({
      x: meses, y: meses.map(m => dados[a]?.[m]||0),
      type:'scatter', mode:'lines+markers', name:a, line:lconf, marker:{size:6, opacity:.95}, fill:fill,
      hovertemplate:`<b>${a}</b><br>%{x}<br>` + (isAtiv ? 'Ativações: %{y}' : 'R$ %{y:,.2f}') + `<extra></extra>`
    }));
  }

  const totalPorMes = meses.map(m => { let s=0; series.forEach(a => s += (dados[a]?.[m]||0)); return s; });
  const layout = {
    ...BASE_LAYOUT,
    yaxis:{...BASE_LAYOUT.yaxis, title: axisTitle, titlefont:{size:12}},
    xaxis:{...BASE_LAYOUT.xaxis, tickmode:'array', tickvals: meses, ticktext: mesesTicks},
    barmode: CHART_MODE==='bar' ? 'relative' : undefined,
    annotations: [{
      xref:'paper', yref:'paper', x:1, y:1.18, xanchor:'right', yanchor:'top',
      text:`Total Selecionado: <b>${
        (isAtiv ? totalPorMes.reduce((a,b)=>a+b,0).toLocaleString('pt-BR')
                : totalPorMes.reduce((a,b)=>a+b,0).toLocaleString('pt-BR',{style:'currency',currency:'BRL'}))
      }</b>`, showarrow:false, font:{size:12, color:'#b9c6ff'}
    }]
  };

  Plotly.react(targetDiv, traces, layout, {
    displayModeBar:true, responsive:true,
    modeBarButtonsToRemove:['lasso2d','select2d','toggleSpikelines','autoScale2d','hoverCompareCartesian'],
    displaylogo:false, toImageButtonOptions:{format:'png', filename:`${metric}_positivadores_2025`}
  });
  Plotly.animate(targetDiv, {data: traces},{transition:{duration:300, easing:'cubic-in-out'}, frame:{duration:300}});
  window.addEventListener('resize', () => Plotly.Plots.resize(targetDiv));
}

/* ===== NOVA VERSÃO: tabela sem coluna AAAA-MM, com colgroup e sticky ===== */
function buildDetailsTable(){
  const target = el('tbl');
  const rows = (DATA.detalhes || []).filter(d => SELECTED.size ? SELECTED.has(d.assessor) : true);

  const colgroup = `
    <colgroup>
      <col style="width:220px">   <!-- Assessor (fixa) -->
      <col style="width:260px">   <!-- Cliente  (fixa) -->
      <col>                       <!-- Ativou em M? -->
      <col>                       <!-- Evadiu em M? -->
      <col>                       <!-- Net em M -->
      <col>                       <!-- Receita no Mês -->
      <col>                       <!-- Captação Líquida em M -->
      <col style="width:120px">   <!-- Mês (nome) -->
    </colgroup>
  `;

  let html = `
    <table>
      ${colgroup}
      <thead>
        <tr>
          <th>Assessor</th>
          <th>Cliente</th>
          <th>Ativou em M?</th>
          <th>Evadiu em M?</th>
          <th>Net em M</th>
          <th>Receita no Mês</th>
          <th>Captação Líquida em M</th>
          <th>Mês</th>
        </tr>
      </thead>
      <tbody>
  `;

  for(const r of rows){
    html += `<tr>
      <td>${r.assessor||''}</td>
      <td>${r.cliente||''}</td>
      <td>${r.ativou_em_m}</td>
      <td>${r.evadiu_em_m}</td>
      <td>${Number(r.net_em_m||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td>${Number(r.receita_no_mes||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td>${Number(r.captacao_liquida_em_m||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td>${r.mes_nome}</td>
    </tr>`;
  }

  html += '</tbody></table>';
  target.innerHTML = html;
}

function mountCSVLink(){
  const list = [...SELECTED];
  const q = list.length ? '?assessores='+encodeURIComponent(list.join(',')) : '';
  el('btnCSV').href = '/api/metricas_csv'+q;
}

async function refresh(){
  document.body.classList.add('loading');
  DATA = await fetchPayload();
  const series = DATA.series.filter(a => SELECTED.size ? SELECTED.has(a) : true);
  const meses = DATA.meses;

  // Cards e Gráficos
  updateMainCards();
  const payload = { ativacoes: DATA.ativacoes, captacao: DATA.captacao, receita: DATA.receita };
  plotMetric('ativacoes', series, meses, payload, 'chartAtiv');
  plotMetric('captacao',  series, meses, payload, 'chartCapt');
  plotMetric('receita',   series, meses, payload, 'chartRece');

  // Tabela de DETALHES (linhas da AWS) - sem AAAA-MM
  buildDetailsTable();

  mountCSVLink();
  document.body.classList.remove('loading');
}

(async function init(){
  ALL_ASSESSORES = await fetchAssessores();
  renderDropdown(''); renderChips(); await refresh();

  el('assSearch').addEventListener('focus', () => renderDropdown(el('assSearch').value));
  el('assSearch').addEventListener('input', (e)=> renderDropdown(e.target.value));
  document.addEventListener('click', (e)=>{ const sel = el('assSel'); if(!sel.contains(e.target)) show(el('assDrop'), false); });
  el('assSel').addEventListener('click', (e)=>{
    const opt = e.target.closest('.opt');
    if(opt && opt.dataset.value){
      const v = opt.dataset.value;
      if(SELECTED.has(v)) SELECTED.delete(v); else SELECTED.add(v);
      renderDropdown(el('assSearch').value); renderChips(); refresh();
    }
  });

  el('btnAll').addEventListener('click', ()=>{ ALL_ASSESSORES.forEach(a=>SELECTED.add(a)); renderDropdown(el('assSearch').value); renderChips(); refresh(); });
  el('btnClear').addEventListener('click', ()=>{ SELECTED.clear(); renderDropdown(el('assSearch').value); renderChips(); refresh(); });

  el('segLine').addEventListener('click', ()=>{ CHART_MODE='line'; refresh(); });
  el('segBar').addEventListener('click',  ()=>{ CHART_MODE='bar';  refresh(); });
})();
</script>
</body></html>
"""
