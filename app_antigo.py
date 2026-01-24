import re
from typing import Optional

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Monitor Fiscal - Governadores",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  .block-container { padding-top: 1.5rem; }
  h1 { font-weight: 800; color: #111; margin: 0; }
  .stRadio { margin-top: 0px !important; }
  #MainMenu {visibility: hidden;} footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# === CONFIGURAÇÃO ===
ARQ_DADOS = "dados_ranking_estados.csv"
ARQ_GOV = "governadores.csv"

CONFIG_METRICAS = {
    "Endividamento": {
        "col_inicial": "DCL_RCL_Pct_Inicial", "col_atual": "DCL_RCL_Pct_Atual", "col_delta": "Delta_pp",
        "titulo_grafico": "Variação do Endividamento (DCL/RCL)", "desc_eixo": "Dívida / RCL (%)",
        "inverter_cores": True, "sulfixo_unidade": " pp"
    },
    "Gastos com Pessoal": {
        "col_inicial": "DTP_RCL_Pct_Inicial", "col_atual": "DTP_RCL_Pct_Atual", "col_delta": "Delta_DTP_pp",
        "titulo_grafico": "Despesa com Pessoal (Executivo/RCL)", "desc_eixo": "Pessoal / RCL (%)",
        "inverter_cores": True, "sulfixo_unidade": " pp"
    },
    "Resultado Primário": {
        "col_inicial": "Primario_RCL_Pct_Inicial", "col_atual": "Primario_RCL_Pct_Atual", "col_delta": "Delta_Primario_pp",
        "titulo_grafico": "Evolução do Resultado Primário (Déficit/Superávit)", "desc_eixo": "Primário / RCL (%)",
        "inverter_cores": False, "sulfixo_unidade": " pp"
    },
}

OPCOES_ORDENACAO = ["Melhor Desempenho", "Pior Desempenho", "Ordem Alfabética"]

# === FUNÇÕES ===
def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

def rgb_to_hex(rgb):
    return '#{:02x}{:02x}{:02x}'.format(int(rgb[0]), int(rgb[1]), int(rgb[2]))

def get_hex_color(value, min_val, max_val, start_hex, end_hex):
    if max_val == min_val: return end_hex
    t = (value - min_val) / (max_val - min_val) if max_val > min_val else 0.5
    c1 = hex_to_rgb(start_hex)
    c2 = hex_to_rgb(end_hex)
    return rgb_to_hex((
        c1[0] + (c2[0] - c1[0]) * t,
        c1[1] + (c2[1] - c1[1]) * t,
        c1[2] + (c2[2] - c1[2]) * t
    ))

def limpar_nome(series):
    return series.astype(str).str.replace(r"^(Governo|Estado) (do |da |de )?", "", regex=True).str.replace(r"^(do |da |de )", "", regex=True).str.strip().str.title()

def formatar_nome_gov(label):
    if not isinstance(label, str): return str(label)
    match = re.match(r"(.*)\s*\((.*)\)", label)
    if match:
        return f"{match.group(1).split()[-1]} ({match.group(2)})"
    return label

def kpi_card(titulo, valor, delta, invert, sulfixo):
    good = (delta < 0) if invert else (delta > 0)
    bg, fg = ("#eafaf1", "#27AE60") if good else ("#fdedec", "#C0392B")
    arrow = "▼" if delta < 0 else "▲"
    return f"""<div style="min-width: 200px; margin-right: 20px; margin-bottom: 20px;">
    <div style="color: #666; font-size: 0.9rem;">{titulo}</div>
    <div style="font-size: 1.8rem; font-weight: 700; color: #111;">{valor}</div>
    <div style="background:{bg}; color:{fg}; padding:4px 8px; border-radius:4px; font-weight:600; display:inline-block;">
    {arrow} {delta:+.1f}{sulfixo}</div></div>"""

# === CARREGAMENTO ===
@st.cache_data
def load_data():
    try:
        # Tenta ler UTF-8, se falhar tenta Latin-1
        try: df = pd.read_csv(ARQ_DADOS, encoding='utf-8')
        except: df = pd.read_csv(ARQ_DADOS, encoding='latin1')
        
        try: gov = pd.read_csv(ARQ_GOV, encoding='utf-8')
        except: gov = pd.read_csv(ARQ_GOV, encoding='latin1')

        # Limpeza para Join
        df["Join"] = limpar_nome(df["Estado"])
        gov["Join"] = limpar_nome(gov["estado"])
        gov = gov.drop_duplicates("Join", keep="last")

        df = df.merge(gov[["Join", "governador", "uf", "partido"]], on="Join", how="left")
        
        df["Label_Eixo"] = df.apply(lambda x: 
            f"{x['governador']} ({x['partido']}-{x['uf']})" if pd.notna(x['governador']) else x['Estado'], axis=1)
        return df
    except Exception as e:
        return None

# === APP ===
df_raw = load_data()
if df_raw is None: st.stop()

c_header, c_kpis, c_controls, c_chart = st.container(), st.container(), st.container(), st.container()

with c_controls:
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    metrica = col1.radio("Indicador", list(CONFIG_METRICAS.keys()), horizontal=True, label_visibility="collapsed")
    ordenacao = col2.selectbox("Ordenar", OPCOES_ORDENACAO, label_visibility="collapsed")

cfg = CONFIG_METRICAS[metrica]
sulfixo = cfg.get("sulfixo_unidade", " pp")
df = df_raw.copy()

for c in [cfg["col_inicial"], cfg["col_atual"], cfg["col_delta"]]:
    if c not in df.columns: df[c] = 0
    df[c] = pd.to_numeric(df[c], errors="coerce")

df = df.dropna(subset=[cfg["col_delta"]])
df["Badge"] = df[cfg["col_delta"]].map(lambda x: f"{x:+.1f}{sulfixo}")
df["Tend"] = df[cfg["col_delta"]].map(lambda x: "Positiva" if x > 0 else "Negativa")

inv = cfg['inverter_cores']
if ordenacao == "Melhor Desempenho": df.sort_values(cfg["col_delta"], ascending=inv, inplace=True)
elif ordenacao == "Pior Desempenho": df.sort_values(cfg["col_delta"], ascending=not inv, inplace=True)
else: df.sort_values("Estado", ascending=True, inplace=True)

sort_order = list(df["Label_Eixo"])

with c_header:
    st.title("Gestão Fiscal")
    st.markdown(f"### {cfg['titulo_grafico']}")

with c_kpis:
    if not df.empty:
        df_rank = df.sort_values(cfg["col_delta"], ascending=inv)
        melhor, pior = df_rank.iloc[0], df_rank.iloc[-1]
        mediana = df[cfg["col_delta"]].median()
        st.markdown(f"""<div style="display: flex; flex-wrap: wrap;">
        {kpi_card("Melhor Desempenho", formatar_nome_gov(melhor["Label_Eixo"]), melhor[cfg["col_delta"]], inv, sulfixo)}
        {kpi_card("Pior Desempenho", formatar_nome_gov(pior["Label_Eixo"]), pior[cfg["col_delta"]], inv, sulfixo)}
        {kpi_card("Mediana Nacional", "Brasil", mediana, inv, sulfixo)}
        </div>""", unsafe_allow_html=True)

with c_chart:
    if not df.empty:
        df["Cor"] = "#ccc"
        vals = df[cfg["col_delta"]].abs()
        p_neg, p_pos = (["#ABEBC6", "#186A3B"], ["#F1948A", "#922B21"]) if inv else (["#F1948A", "#922B21"], ["#ABEBC6", "#186A3B"])
        
        for i in df.index:
            v = df.at[i, cfg["col_delta"]]
            pal = p_neg if v < 0 else p_pos
            df.at[i, "Cor"] = get_hex_color(abs(v), vals.min(), vals.max(), pal[0], pal[1])

        base = alt.Chart(df).encode(y=alt.Y("Label_Eixo", sort=sort_order, axis=None))
        
        dots = base.encode(
            y=alt.Y("Label_Eixo", sort=sort_order, axis=alt.Axis(labels=True, ticks=False, domain=False, labelLimit=400, title=None)),
            tooltip=["Estado", "Label_Eixo", alt.Tooltip(cfg["col_delta"], format="+.1f")]
        )
        rule = dots.mark_rule(color="#ddd").encode(x=f"{cfg['col_inicial']}:Q", x2=f"{cfg['col_atual']}:Q")
        p1 = dots.mark_circle(size=80, color="#bbb", opacity=0.5).encode(x=alt.X(f"{cfg['col_inicial']}:Q", title=cfg["desc_eixo"]))
        p2 = dots.mark_circle(size=180, opacity=1).encode(
            x=f"{cfg['col_atual']}:Q", 
            color=alt.Color("Tend:N", scale=alt.Scale(domain=["Positiva", "Negativa"], range=["#C0392B", "#27AE60"] if inv else ["#27AE60", "#C0392B"]), legend=None)
        )
        
        chart_dots = (rule + p1 + p2).properties(width=600)
        
        badges = base.mark_rect(cornerRadius=10).encode(
            x=alt.value(0), x2=alt.value(90), color=alt.Color("Cor", scale=None, legend=None),
            tooltip=[alt.Tooltip(cfg["col_delta"], format="+.1f")]
        )
        text = base.mark_text(align="center", color="white", fontWeight="bold").encode(x=alt.value(45), text="Badge")
        
        chart_badges = (badges + text).properties(width=100, title=alt.TitleParams("Variação", anchor="middle"))
        
        st.altair_chart(alt.hconcat(chart_dots, chart_badges).configure_view(stroke=None), use_container_width=True)
    
    st.markdown("<div style='text-align: right; color: #aaa; font-size: 0.8rem;'>Fonte: Siconfi/Tesouro Nacional.</div>", unsafe_allow_html=True)