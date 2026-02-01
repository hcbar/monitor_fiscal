import re
from typing import Optional

import altair as alt
import pandas as pd
import streamlit as st

# === CONFIGURAÇÃO DA PÁGINA ===
st.set_page_config(
    page_title="Monitor Fiscal - Governadores",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# CSS customizado
st.markdown("""
<style>
  .block-container { padding-top: 1.5rem; }
  h1 { font-weight: 800; color: #111; margin: 0; }
  .stRadio { margin-top: 0px !important; }
  #MainMenu {visibility: hidden;} 
  footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# === CONFIGURAÇÕES ===
ARQ_DADOS = "dados_ranking_estados.csv"
ARQ_GOV = "governadores.csv"

# Configuração simplificada - apenas Poupança Fiscal por enquanto
# (DCL e DTP requerem CSVs adicionais do RGF)
CONFIG_METRICAS = {
    "Poupança Fiscal": {
        "col_inicial": "Poupanca_Fiscal_Pct",  # Usar a mesma coluna para evitar None
        "col_atual": "Poupanca_Fiscal_Pct",
        "col_delta": "Poupanca_Fiscal_Pct",
        "titulo_grafico": "Poupança Fiscal (Resultado Primário / RCL)",
        "desc_eixo": "% da RCL",
        "inverter_cores": False,
        "sufixo_unidade": "%",
        "descricao": """Resultado Primário dividido pela Receita Corrente Líquida do ano mais recente (2024). 
        
Mede se o governador deixou as contas no azul (superávit) ou no vermelho (déficit).

**Resultado Primário** = Receitas - Despesas (excluindo juros da dívida)

- **Positivo** 🟢: Superávit primário (poupou, pode pagar dívidas)
- **Negativo** 🔴: Déficit primário (gastou mais do que arrecadou)
- **Zero** ⚪: Equilíbrio fiscal""",
        "explicacao_extra": """
**📖 Por que esse indicador importa?**

O Resultado Primário mostra a VERDADEIRA situação fiscal, excluindo juros (que são herdados de mandatos anteriores).

Um governador pode ter **superávit primário** mas ainda pagar muitos juros de dívidas antigas. Mas se ele poupa no primário, está no caminho certo para reduzir o endividamento.

**Exemplo:** +5% significa que o estado economizou 5% da sua receita, diminuindo o endividamento.

**Fonte dos Dados:**
RREO Anexo 06 - Demonstrativo dos Resultados Primário e Nominal (SICONFI)
        """
    },
}

OPCOES_ORDENACAO = ["Melhor Desempenho", "Pior Desempenho", "Ordem Alfabética"]


# === FUNÇÕES AUXILIARES ===
def limpar_nome(series: pd.Series) -> pd.Series:
    """Remove prefixos comuns de nomes de estados."""
    return (series.astype(str)
            .str.replace(r"^(Governo|Estado) (do |da |de )?", "", regex=True)
            .str.replace(r"^(do |da |de )", "", regex=True)
            .str.strip()
            .str.title())


def formatar_nome_gov(label: str) -> str:
    """
    Formata o nome do governador para exibição nos KPIs.
    Remove o (R) e mostra apenas sobrenome + UF.
    """
    if not isinstance(label, str):
        return str(label)
    
    # Remove o (R) do início
    clean_label = label.replace("(R) ", "")
    
    # Extrai nome e info entre parênteses
    match = re.match(r"(.*)\s*\((.*)\)", clean_label)
    if match:
        nome_completo = match.group(1).strip()
        info_partido = match.group(2)
        
        # Pega apenas o último nome (sobrenome)
        sobrenome = nome_completo.split()[-1] if nome_completo else nome_completo
        
        return f"{sobrenome} ({info_partido})"
    
    return label


def kpi_card(titulo: str, valor: str, delta: float, invert: bool, sufixo: str) -> str:
    """
    Gera HTML para um card KPI com indicador de melhora/piora.
    """
    # Define se a variação é boa ou ruim
    good = (delta < 0) if invert else (delta > 0)
    
    # Cores: verde para bom, vermelho para ruim
    bg = "#eafaf1" if good else "#fdedec"
    fg = "#27AE60" if good else "#C0392B"
    
    # Seta: para baixo se negativo, para cima se positivo
    arrow = "▼" if delta < 0 else "▲"
    
    return f"""
    <div style="min-width: 200px; margin-right: 20px; margin-bottom: 20px;">
        <div style="color: #666; font-size: 0.9rem;">{titulo}</div>
        <div style="font-size: 1.8rem; font-weight: 700; color: #111;">{valor}</div>
        <div style="background:{bg}; color:{fg}; padding:4px 8px; border-radius:4px; 
                    font-weight:600; display:inline-block; font-size: 0.95rem;">
            {arrow} {delta:+.1f}{sufixo}
        </div>
    </div>
    """


# === CARREGAMENTO DE DADOS ===
@st.cache_data
def load_data() -> Optional[pd.DataFrame]:
    """
    Carrega e processa os dados de ranking dos estados e governadores.
    """
    try:
        # Carrega dados do ranking
        try:
            df = pd.read_csv(ARQ_DADOS, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(ARQ_DADOS, encoding='latin1')
        
        # Carrega dados dos governadores
        try:
            gov = pd.read_csv(ARQ_GOV, encoding='utf-8')
        except UnicodeDecodeError:
            gov = pd.read_csv(ARQ_GOV, encoding='latin1')

        # Preparação para join
        df["Join"] = limpar_nome(df["Estado"])
        gov["Join"] = limpar_nome(gov["estado"])
        
        # Remove duplicatas (mantém o último)
        gov = gov.drop_duplicates("Join", keep="last")

        # Merge dos dados
        df = df.merge(
            gov[["Join", "governador", "uf", "partido"]], 
            on="Join", 
            how="left"
        )
        
        # Criação do Label com (R) no INÍCIO para reeleitos
        def criar_label(row):
            if pd.notna(row.get('governador')):
                base = f"{row['governador']} ({row['partido']}-{row['uf']})"
            else:
                base = row['Estado']
            
            # Adiciona (R) no início se foi reeleito
            if row.get('Reeleito', False):
                return f"(R) {base}"
            return base

        df["Label_Eixo"] = df.apply(criar_label, axis=1)
        
        return df
        
    except FileNotFoundError as e:
        st.error(f"❌ Arquivo não encontrado: {e}")
        return None
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


# === APLICAÇÃO PRINCIPAL ===
df_raw = load_data()

if df_raw is None:
    st.stop()

# Validação básica dos dados
if df_raw.empty:
    st.error("❌ DataFrame vazio! Verifique o arquivo de dados.")
    st.stop()

# Containers para organização do layout
c_header = st.container()
c_kpis = st.container()
c_controls = st.container()
c_chart = st.container()

# === CONTROLES ===
with c_controls:
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    
    # Se só tem uma métrica, não precisa de radio
    if len(CONFIG_METRICAS) == 1:
        metrica_selecionada = list(CONFIG_METRICAS.keys())[0]
        col1.markdown(f"**Indicador:** {metrica_selecionada}")
    else:
        metrica_selecionada = col1.radio(
            "Indicador", 
            list(CONFIG_METRICAS.keys()), 
            horizontal=True, 
            label_visibility="collapsed"
        )
    
    ordenacao = col2.selectbox(
        "Ordenar", 
        OPCOES_ORDENACAO, 
        label_visibility="collapsed"
    )

# Configuração da métrica selecionada
cfg = CONFIG_METRICAS[metrica_selecionada]
sufixo = cfg.get("sufixo_unidade", " pp")

# Cópia do dataframe para manipulação
df = df_raw.copy()

# Validação e conversão das colunas necessárias
for col in [cfg["col_inicial"], cfg["col_atual"], cfg["col_delta"]]:
    if col is None:
        continue
    if col not in df.columns:
        st.warning(f"⚠️ Coluna '{col}' não encontrada nos dados.")
        df[col] = 0.0
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Remove linhas sem dados de delta
df = df.dropna(subset=[cfg["col_delta"]])

if df.empty:
    st.error("❌ Nenhum dado disponível após filtros.")
    st.stop()

# === LÓGICA DE ORDENAÇÃO ===
inv = cfg['inverter_cores']

if ordenacao == "Melhor Desempenho":
    # Para métricas invertidas (dívida/pessoal): menor delta é melhor
    df = df.sort_values(cfg["col_delta"], ascending=inv)
elif ordenacao == "Pior Desempenho":
    # Inverte a lógica
    df = df.sort_values(cfg["col_delta"], ascending=not inv)
else:  # Ordem Alfabética
    df = df.sort_values("Estado", ascending=True)

# Ordem dos labels para o gráfico
sort_order = list(df["Label_Eixo"])

# === CORES DO GRÁFICO ===
# Verde para bom, vermelho para ruim
color_cond = alt.condition(
    (alt.datum[cfg["col_delta"]] < 0) if inv else (alt.datum[cfg["col_delta"]] > 0),
    alt.value("#27AE60"),  # Verde
    alt.value("#C0392B")   # Vermelho
)

# === HEADER ===
with c_header:
    st.title("📊 Gestão Fiscal dos Governadores")
    st.markdown(f"### {cfg['titulo_grafico']}")
    
    # Adiciona descrição da métrica
    with st.expander("ℹ️ Sobre este indicador"):
        st.write(f"**{metrica_selecionada}**: {cfg['descricao']}")
        
        # Se tiver explicação extra, mostra
        if "explicacao_extra" in cfg:
            st.markdown(cfg["explicacao_extra"])
        else:
            if inv:
                st.write("✅ **Verde (negativo)**: Melhora - redução do indicador")
                st.write("❌ **Vermelho (positivo)**: Piora - aumento do indicador")
            else:
                st.write("✅ **Verde (positivo)**: Melhora - superávit fiscal")
                st.write("❌ **Vermelho (negativo)**: Piora - déficit fiscal")

# === KPIs ===
with c_kpis:
    if not df.empty:
        # Ranking: melhor desempenho sempre é o menor valor se invertido
        df_rank = df.sort_values(cfg["col_delta"], ascending=inv)
        
        melhor = df_rank.iloc[0]
        pior = df_rank.iloc[-1]
        mediana = df[cfg["col_delta"]].median()
        
        st.markdown(
            f"""
            <div style="display: flex; flex-wrap: wrap; margin-bottom: 20px;">
                {kpi_card("🏆 Melhor Desempenho", formatar_nome_gov(melhor["Label_Eixo"]), melhor[cfg["col_delta"]], inv, sufixo)}
                {kpi_card("⚠️ Pior Desempenho", formatar_nome_gov(pior["Label_Eixo"]), pior[cfg["col_delta"]], inv, sufixo)}
                {kpi_card("📊 Mediana Nacional", "Brasil", mediana, inv, sufixo)}
            </div>
            """, 
            unsafe_allow_html=True
        )

# === GRÁFICO ===
with c_chart:
    if not df.empty:
        # Construir tooltips dinamicamente (evita None)
        tooltip_list = [
            alt.Tooltip("Estado", title="Estado"),
            alt.Tooltip("Label_Eixo", title="Governador"),
            alt.Tooltip(cfg["col_delta"], format="+.2f", title="Valor (%)"),
            alt.Tooltip("Reeleito", title="Reeleito?")
        ]
        
        # Adicionar colunas extras se existirem
        if "Resultado_Primario_Ano_Recente_Bi" in df.columns:
            tooltip_list.append(alt.Tooltip("Resultado_Primario_Ano_Recente_Bi", format=".2f", title="RP (R$ bi)"))
        if "RCL_Ano_Recente_Bi" in df.columns:
            tooltip_list.append(alt.Tooltip("RCL_Ano_Recente_Bi", format=".2f", title="RCL (R$ bi)"))
        
        # Base do gráfico
        base = alt.Chart(df).encode(
            y=alt.Y(
                "Label_Eixo", 
                sort=sort_order, 
                title=None, 
                axis=alt.Axis(
                    labelLimit=400, 
                    titlePadding=20, 
                    offset=10, 
                    labelFontWeight='bold',
                    labelFontSize=11
                )
            ),
            tooltip=tooltip_list
        )

        # Barras
        bars = base.mark_bar(size=20).encode(
            x=alt.X(cfg["col_delta"], title=cfg["desc_eixo"]),
            color=color_cond
        )

        # Texto para valores positivos (à direita da barra)
        text_pos = base.transform_filter(
            alt.datum[cfg["col_delta"]] >= 0
        ).mark_text(
            align='left', 
            baseline='middle', 
            dx=5,
            fontSize=10,
            fontWeight='bold'
        ).encode(
            x=alt.X(cfg["col_delta"]),
            text=alt.Text(cfg["col_delta"], format="+.1f")
        )
        
        # Texto para valores negativos (à esquerda da barra)
        text_neg = base.transform_filter(
            alt.datum[cfg["col_delta"]] < 0
        ).mark_text(
            align='right', 
            baseline='middle', 
            dx=-5,
            fontSize=10,
            fontWeight='bold'
        ).encode(
            x=alt.X(cfg["col_delta"]),
            text=alt.Text(cfg["col_delta"], format="+.1f")
        )

        # Linha vertical no zero
        rule = alt.Chart(pd.DataFrame({'x': [0]})).mark_rule(
            color='black', 
            strokeWidth=2,
            opacity=0.5
        ).encode(x='x')

        # Composição do gráfico
        chart = (bars + rule + text_pos + text_neg).properties(
            height=max(650, len(df) * 28)
        )

        st.altair_chart(chart, use_container_width=True)
    
    # Rodapé com fonte
    st.markdown(
        """
        <div style='text-align: right; color: #888; font-size: 0.85rem; margin-top: 10px;'>
            📊 Fonte: Siconfi/Tesouro Nacional | (R) = Governador Reeleito | Dados: 2024
        </div>
        """, 
        unsafe_allow_html=True
    )

# === ANÁLISE ADICIONAL ===
with st.expander("📈 Estatísticas Detalhadas"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            "Média Nacional", 
            f"{df[cfg['col_delta']].mean():.2f}{sufixo}",
            delta=None
        )
        st.metric(
            "Desvio Padrão", 
            f"{df[cfg['col_delta']].std():.2f}{sufixo}",
            delta=None
        )
    
    with col2:
        # Comparação reeleitos vs não reeleitos
        if 'Reeleito' in df.columns:
            reeleitos = df[df['Reeleito'] == True]
            nao_reeleitos = df[df['Reeleito'] == False]
            
            if not reeleitos.empty and not nao_reeleitos.empty:
                media_reeleitos = reeleitos[cfg['col_delta']].mean()
                media_nao_reeleitos = nao_reeleitos[cfg['col_delta']].mean()
                
                st.metric(
                    "Média - Reeleitos", 
                    f"{media_reeleitos:.2f}{sufixo}",
                    delta=f"{len(reeleitos)} estados"
                )
                st.metric(
                    "Média - Não Reeleitos", 
                    f"{media_nao_reeleitos:.2f}{sufixo}",
                    delta=f"{len(nao_reeleitos)} estados"
                )

# === TABELA DE DADOS ===
with st.expander("📋 Ver Dados Completos"):
    cols_exibir = ["Estado", "Label_Eixo", "Poupanca_Fiscal_Pct", 
                   "Resultado_Primario_Ano_Recente_Bi", "RCL_Ano_Recente_Bi", 
                   "Diferenca_RP_Meta_Bi", "Reeleito"]
    cols_disponiveis = [c for c in cols_exibir if c in df.columns]
    st.dataframe(df[cols_disponiveis].sort_values("Poupanca_Fiscal_Pct", ascending=False))