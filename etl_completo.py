# etl_completo.py
# ETL SICONFI (Estados) – DCL/RCL, DTP/RCL, Poupança Fiscal (RP/RCL ano recente) e Diferença RP vs Meta
# 
# NOTA: Os valores do 6º bimestre já são consolidados do ano completo (não há "Até o Bimestre")

import re
import time
import os
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import requests

# --- Configurações ---
CODIGOS_UF = {
    11: "Rondônia", 12: "Acre", 13: "Amazonas", 14: "Roraima", 15: "Pará",
    16: "Amapá", 17: "Tocantins", 21: "Maranhão", 22: "Piauí", 23: "Ceará",
    24: "Rio Grande do Norte", 25: "Paraíba", 26: "Pernambuco", 27: "Alagoas",
    28: "Sergipe", 29: "Bahia", 31: "Minas Gerais", 32: "Espírito Santo",
    33: "Rio de Janeiro", 35: "São Paulo", 41: "Paraná", 42: "Santa Catarina",
    43: "Rio Grande do Sul", 50: "Mato Grosso do Sul", 51: "Mato Grosso",
    52: "Goiás", 53: "Distrito Federal",
}

UF_REELEITOS = [11, 12, 13, 14, 15, 17, 21, 24, 25, 28, 31, 32, 33, 50, 51, 52, 53]

# Arquivo opcional com anos de início/fim de mandato por estado (para calcular Delta do RP/RCL por governador)
ARQ_GOV = "governadores.csv"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (etl_siconfi)", "Accept": "application/json"})


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _parse_number(x) -> float:
    """Converte '1.234.567,89' / '1234,56' / 1234.56 em float."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return 0.0
    s = str(x).strip()
    if not s or s.lower() in ["nan", "none", ""]:
        return 0.0
    s = s.replace(" ", "").replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9\.\-\+]", "", s)
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _normalize_anexo_pattern(anexo: str) -> re.Pattern:
    """Regex tolerante a 'Anexo 01' vs 'Anexo 1' e sufixos longos."""
    m = re.search(r"anexo\s*0*([0-9]+)", anexo, flags=re.I)
    if not m:
        return re.compile(re.escape(anexo), flags=re.I)
    num = int(m.group(1))
    return re.compile(rf"\banexo\s*0*{num}\b", flags=re.I)


def _pick_col(df: pd.DataFrame, candidates) -> Optional[str]:
    """Retorna a primeira coluna encontrada da lista de candidatos."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _strip_accents_like_regex(text: str) -> str:
    """
    Cria regex tolerante a variações comuns 'á/a', 'í/i', etc, para os termos-chave.
    """
    repl = (
        ("á", "[aáàâã]"), ("à", "[aáàâã]"), ("â", "[aáàâã]"), ("ã", "[aáàâã]"), ("a", "[aáàâã]"),
        ("é", "[eéèê]"), ("è", "[eéèê]"), ("ê", "[eéèê]"), ("e", "[eéèê]"),
        ("í", "[iíìî]"), ("ì", "[iíìî]"), ("î", "[iíìî]"), ("i", "[iíìî]"),
        ("ó", "[oóòôõ]"), ("ò", "[oóòôõ]"), ("ô", "[oóòôõ]"), ("õ", "[oóòôõ]"), ("o", "[oóòôõ]"),
        ("ú", "[uúùû]"), ("ù", "[uúùû]"), ("û", "[uúùû]"), ("u", "[uúùû]"),
        ("ç", "[cç]"), ("c", "[cç]"),
    )
    out = text.lower()
    for a, rgx in repl:
        out = out.replace(a, rgx)
    return out


def _limpar_nome_estado(nome: str) -> str:
    """Normaliza nomes para casar com governadores.csv (ex: 'Estado do Rio Grande do Sul' -> 'Rio Grande Do Sul')."""
    s = str(nome)
    s = re.sub(r"^(Governo|Estado)\s+(do|da|de)\s+", "", s, flags=re.I)
    s = re.sub(r"^(do|da|de)\s+", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    return s.title()


def _load_mandatos_governadores(path: str = ARQ_GOV) -> dict:
    """
    Carrega (se existir) um CSV com colunas de estado e ano de início/fim do mandato.
    Retorna dict: nome_normalizado -> (ano_inicio, ano_fim) com None quando não disponível.

    O loader é tolerante a nomes de colunas diferentes.
    """
    if not os.path.exists(path):
        return {}

    try:
        gov = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        gov = pd.read_csv(path, encoding="latin1")

    col_estado = _pick_col(gov, ["estado", "Estado", "uf_nome", "nome_estado", "unidade_federativa"])
    if not col_estado:
        return {}

    col_ini = _pick_col(gov, ["ano_inicio", "inicio", "inicio_mandato", "ano_inicio_mandato", "mandato_inicio", "start_year", "ano_posse"])
    col_fim = _pick_col(gov, ["ano_fim", "fim", "fim_mandato", "ano_fim_mandato", "mandato_fim", "end_year", "ano_saida"])

    out = {}
    for _, r in gov.iterrows():
        key = _limpar_nome_estado(r[col_estado])
        ini = int(r[col_ini]) if col_ini and pd.notna(r[col_ini]) else None
        fim = int(r[col_fim]) if col_fim and pd.notna(r[col_fim]) else None
        out[key] = (ini, fim)

    return out


# --------------------------------------------------------------------------------------
# SICONFI API fetch
# --------------------------------------------------------------------------------------
def get_data(
    ano: int,
    periodo: int,
    cod_ibge: int,
    tipo_relatorio: str,
    anexo: Optional[str] = None,
    debug: bool = False,
    max_retries: int = 3,
) -> pd.DataFrame:
    """Busca dados no SICONFI Data Lake. Filtra localmente por anexo."""
    tipo = tipo_relatorio.upper()
    url = f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/{tipo.lower()}"

    if tipo == "RGF":
        periodicidade = "Q"
        periodo_api = periodo
    elif tipo == "RREO":
        periodicidade = "B"
        periodo_api = periodo
    else:
        raise ValueError("tipo_relatorio deve ser 'RGF' ou 'RREO'.")

    params = {
        "an_exercicio": int(ano),
        "nr_periodo": int(periodo_api),
        "co_tipo_demonstrativo": tipo,
        "co_esfera": "E",
        "id_ente": int(cod_ibge),
        "in_periodicidade": periodicidade,
        "co_poder": "E",
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if not items:
                return pd.DataFrame()

            df_full = pd.DataFrame(items)

            if anexo and "anexo" in df_full.columns:
                pat = _normalize_anexo_pattern(anexo)
                mask_anexo = df_full["anexo"].astype(str).str.contains(
                    pat.pattern, case=False, na=False, regex=True
                )
                df_full = df_full[mask_anexo]

            return df_full

        except requests.exceptions.RequestException as e:
            if debug:
                print(f"    Erro {attempt}/{max_retries}: {e}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
            else:
                return pd.DataFrame()

    return pd.DataFrame()


def get_data_rreo_bimestre(
    ano: int,
    bim: int,
    cod_ibge: int,
    anexo: Optional[str] = None,
    debug: bool = False,
) -> pd.DataFrame:
    """Wrapper para buscar RREO bimestral."""
    return get_data(ano, bim, cod_ibge, "RREO", anexo=anexo, debug=debug)


# --------------------------------------------------------------------------------------
# Extração de valores específicos
# --------------------------------------------------------------------------------------
def pick_by_identifier(df: pd.DataFrame, identifier: str, coluna: str) -> float:
    """Busca linha por identificador e retorna valor da coluna especificada."""
    if df.empty or "identificador_de_conta" not in df.columns or coluna not in df.columns:
        return 0.0
    mask = df["identificador_de_conta"].astype(str).str.strip() == identifier
    subset = df.loc[mask, coluna]
    if subset.empty:
        return 0.0
    return _parse_number(subset.iloc[0])


def pick_by_conta_name(df: pd.DataFrame, patterns: tuple, coluna: str) -> float:
    """Busca linha por nome de conta (regex) e retorna valor da coluna."""
    if df.empty or "conta" not in df.columns or coluna not in df.columns:
        return 0.0

    for pat in patterns:
        rgx = _strip_accents_like_regex(pat)
        mask = df["conta"].astype(str).str.lower().str.contains(rgx, case=False, na=False, regex=True)
        subset = df.loc[mask, coluna]
        if not subset.empty:
            return _parse_number(subset.iloc[0])
    return 0.0


def extrair_resultado_primario(df: pd.DataFrame, coluna: str = None, debug: bool = False) -> float:
    """
    Busca o 'Resultado Primário' em um DataFrame RREO (ex: Anexo 06, 07, 09 ou 10).
    No 6º bimestre, o valor já é o consolidado do ano completo.
    Retorna 0 se não encontrado.
    """
    if df.empty:
        return 0.0

    # Auto-detecta a coluna de valor se não especificada
    if coluna is None:
        coluna = _pick_col(df, ["VALOR", "Até o Bimestre", "valor", "Valor"])
        if not coluna:
            return 0.0

    # Tenta via identificador oficial
    rp = pick_by_identifier(df, "siconfi-cor_RREO6ResultadoPrimarioEstadosMunicipios", coluna)
    if rp != 0:
        return rp

    # Fallback: regex por nome de conta
    patterns = (
        r"resultado\s+prim[aáàâã]rio",
        r"prim[aáàâã]rio",
    )
    rp = pick_by_conta_name(df, patterns, coluna)

    if debug and rp == 0:
        print("    [DEBUG] Resultado Primário não encontrado! Contas disponíveis:")
        if "conta" in df.columns:
            print(df[["conta", coluna]].head(20))

    return rp


def extrair_meta_resultado_primario(df: pd.DataFrame, coluna: str = None, debug: bool = False) -> float:
    """
    Busca a 'Meta de Resultado Primário' fixada no Anexo de Metas Fiscais da LDO.
    No 6º bimestre, o valor já é o consolidado do ano completo.
    Retorna 0 se não encontrado.
    """
    if df.empty:
        return 0.0

    # Auto-detecta a coluna de valor se não especificada
    if coluna is None:
        coluna = _pick_col(df, ["VALOR CORRENTE", "VALOR", "Até o Bimestre", "valor corrente", "Valor Corrente"])
        if not coluna:
            return 0.0

    # Tenta via identificador oficial
    meta = pick_by_identifier(
        df, 
        "siconfi-cor_RREO6MetaDeResultadoPrimarioFixadaNoAnexoDeMetasFiscaisDaLDOParaOExercicioDeReferencia", 
        coluna
    )
    if meta != 0:
        return meta

    # Fallback: regex por nome de conta
    patterns = (
        r"meta\s+de\s+resultado\s+prim[aáàâã]rio",
        r"meta.*prim[aáàâã]rio.*ldo",
        r"meta.*anexo.*metas\s+fiscais",
    )
    meta = pick_by_conta_name(df, patterns, coluna)

    if debug and meta == 0:
        print("    [DEBUG] Meta de Resultado Primário não encontrada! Contas disponíveis:")
        if "conta" in df.columns:
            print(df[["conta", coluna]].head(20))

    return meta


# --------------------------------------------------------------------------------------
# Processamento por UF
# --------------------------------------------------------------------------------------
def process_uf(
    cod: int,
    nome: str,
    reeleito: bool,
    ano_ini: int = 2018,
    debug: bool = False,
    mandatos: Optional[dict] = None,
) -> dict:
    """
    Processa dados de um estado, retornando métricas de DCL, DTP e Resultado Primário.
    """
    if mandatos is None:
        mandatos = {}

    ano_fim = datetime.now().year - 1

    if debug:
        print(f"\n{'='*60}")
        print(f"  {nome} (Código {cod}) - Reeleito: {reeleito}")
        print(f"  Período base: {ano_ini} → {ano_fim}")
        print(f"{'='*60}")

    # --- 1) DCL/RCL ---
    df_a2_ini = get_data(ano_ini, 3, cod, "RGF", anexo="Anexo 02", debug=False)
    df_a2_fim = get_data(ano_fim, 3, cod, "RGF", anexo="Anexo 02", debug=False)

    dcl_ini = pick_by_identifier(df_a2_ini, "siconfi-cor_DividaConsolidadaLiquidaDcl", "Saldo do Exercício Anterior")
    dcl_fim = pick_by_identifier(df_a2_fim, "siconfi-cor_DividaConsolidadaLiquidaDcl", "Saldo do Exercício Atual")

    if dcl_ini == 0:
        dcl_ini = pick_by_conta_name(
            df_a2_ini, ("DÍVIDA CONSOLIDADA LÍQUIDA", "DCL"), "Saldo do Exercício Anterior"
        )
    if dcl_fim == 0:
        dcl_fim = pick_by_conta_name(df_a2_fim, ("DÍVIDA CONSOLIDADA LÍQUIDA", "DCL"), "Saldo do Exercício Atual")

    rcl_dcl_ini = pick_by_identifier(df_a2_ini, "siconfi-cor_ReceitaCorrenteLiquida", "Saldo do Exercício Anterior")
    rcl_dcl_fim = pick_by_identifier(df_a2_fim, "siconfi-cor_ReceitaCorrenteLiquida", "Saldo do Exercício Atual")

    if rcl_dcl_ini == 0:
        rcl_dcl_ini = pick_by_conta_name(
            df_a2_ini, ("RECEITA CORRENTE LÍQUIDA",), "Saldo do Exercício Anterior"
        )
    if rcl_dcl_fim == 0:
        rcl_dcl_fim = pick_by_conta_name(df_a2_fim, ("RECEITA CORRENTE LÍQUIDA",), "Saldo do Exercício Atual")

    dcl_pct_ini = (dcl_ini / rcl_dcl_ini * 100) if rcl_dcl_ini > 0 else 0.0
    dcl_pct_fim = (dcl_fim / rcl_dcl_fim * 100) if rcl_dcl_fim > 0 else 0.0

    # --- 2) DTP/RCL ---
    ano_a1_fim = datetime.now().year - 1
    df_a1_ini = get_data(ano_ini, 3, cod, "RGF", anexo="Anexo 01", debug=False)
    df_a1_fim = get_data(ano_a1_fim, 3, cod, "RGF", anexo="Anexo 01", debug=False)

    def extract_dtp_rcl(df_rgf: pd.DataFrame, ano: int) -> Tuple[float, float]:
        """Extrai DTP do RGF e RCL do RREO."""
        dtp = pick_by_identifier(df_rgf, "siconfi-cor_DespesaComPessoalBruta", "TOTAL (ÚLTIMOS 12 MESES)")
        if dtp == 0:
            dtp = pick_by_conta_name(df_rgf, ("DESPESA BRUTA COM PESSOAL",), "TOTAL (ÚLTIMOS 12 MESES)")

        df_rcl = get_data_rreo_bimestre(ano, 6, cod, anexo="Anexo 03", debug=False)
        rcl = pick_by_identifier(df_rcl, "siconfi-cor_RREO3ReceitaCorrenteLiquida", "TOTAL (ÚLTIMOS 12 MESES)")
        if rcl == 0:
            rcl = pick_by_conta_name(df_rcl, ("RECEITA CORRENTE LÍQUIDA",), "TOTAL (ÚLTIMOS 12 MESES)")

        return dtp, rcl

    dtp_ini, rcl_dtp_ini = extract_dtp_rcl(df_a1_ini, ano_ini)
    dtp_fim, rcl_dtp_fim = extract_dtp_rcl(df_a1_fim, ano_a1_fim)

    dtp_pct_ini = (dtp_ini / rcl_dtp_ini * 100) if rcl_dtp_ini > 0 else 0.0
    dtp_pct_fim = (dtp_fim / rcl_dtp_fim * 100) if rcl_dtp_fim > 0 else 0.0

    # --- 3) Resultado Primário e Meta por ano ---
    resultados_primarios = {}
    metas_primarias = {}
    rcls_periodo = {}

    ano_rreo_fim = datetime.now().year

    for ano in range(ano_ini, ano_rreo_fim + 1):
        # Resultado Primário (auto-detecta anexo)
        try:
            anexos_rp = ["Anexo 06", "Anexo 07", "Anexo 09", "Anexo 10"]
            rp = 0.0
            meta = 0.0
            anexo_usado = None

            for ax in anexos_rp:
                df_rp = get_data_rreo_bimestre(ano, 6, cod, anexo=ax, debug=False)
                rp = extrair_resultado_primario(df_rp, debug=(debug and cod == 11))
                meta = extrair_meta_resultado_primario(df_rp, debug=(debug and cod == 11))
                
                if rp != 0.0 or meta != 0.0:
                    anexo_usado = ax
                    break

            resultados_primarios[ano] = rp
            metas_primarias[ano] = meta

            if debug and cod == 11:
                if anexo_usado:
                    print(f"    RP {ano}: R$ {rp/1e9:+.2f} bi | Meta: R$ {meta/1e9:+.2f} bi ({anexo_usado})")
                else:
                    print(f"    RP {ano}: R$ {rp/1e9:+.2f} bi | Meta: R$ {meta/1e9:+.2f} bi (não encontrado)")
        except Exception as e:
            if debug:
                print(f"    Erro RREO RP {ano}: {e}")

        # RCL do RREO Anexo 03 (12 meses)
        try:
            df_rcl = get_data_rreo_bimestre(ano, 6, cod, anexo="Anexo 03", debug=False)
            rcl = pick_by_identifier(df_rcl, "siconfi-cor_RREO3ReceitaCorrenteLiquida", "TOTAL (ÚLTIMOS 12 MESES)")
            if rcl == 0:
                rcl = pick_by_conta_name(df_rcl, ("RECEITA CORRENTE LÍQUIDA",), "TOTAL (ÚLTIMOS 12 MESES)")
            if rcl > 0:
                rcls_periodo[ano] = rcl

                if debug and cod == 11:
                    print(f"    RCL {ano}: R$ {rcl/1e9:.2f} bi")
        except Exception as e:
            if debug:
                print(f"    Erro RCL {ano}: {e}")

    anos_comuns = set(resultados_primarios.keys()) & set(rcls_periodo.keys())

    # Série anual RP/RCL (%)
    rp_rcl_pct_by_year = {}
    for y in anos_comuns:
        rcl_y = rcls_periodo.get(y, 0.0)
        rp_y = resultados_primarios.get(y, 0.0)
        if rcl_y > 0:
            rp_rcl_pct_by_year[y] = (rp_y / rcl_y) * 100.0

    # Delta RP/RCL do início ao fim do mandato (início/fim pode vir de governadores.csv)
    mandato_ini = None
    mandato_fim = None
    if mandatos:
        mandato_ini, mandato_fim = mandatos.get(_limpar_nome_estado(nome), (None, None))

    anos_disponiveis = sorted(rp_rcl_pct_by_year.keys())
    rp_rcl_ini_pct = 0.0
    rp_rcl_fim_pct = 0.0
    delta_rp_rcl_pp = 0.0
    ano_ini_rp = None
    ano_fim_rp = None

    if anos_disponiveis:
        if mandato_ini is not None:
            candidatos = [y for y in anos_disponiveis if y >= mandato_ini]
            ano_ini_rp = candidatos[0] if candidatos else anos_disponiveis[0]
        else:
            ano_ini_rp = anos_disponiveis[0]

        if mandato_fim is not None:
            candidatos = [y for y in anos_disponiveis if y <= mandato_fim]
            ano_fim_rp = candidatos[-1] if candidatos else anos_disponiveis[-1]
        else:
            ano_fim_rp = anos_disponiveis[-1]

        rp_rcl_ini_pct = rp_rcl_pct_by_year.get(ano_ini_rp, 0.0)
        rp_rcl_fim_pct = rp_rcl_pct_by_year.get(ano_fim_rp, 0.0)
        delta_rp_rcl_pp = rp_rcl_fim_pct - rp_rcl_ini_pct

    # ✅ NOVA LÓGICA: Poupança Fiscal = RP/RCL do ano mais recente com dados
    poupanca_fiscal = 0.0
    ano_poupanca = None
    rp_ano_recente = 0.0
    rcl_ano_recente = 0.0
    
    if anos_disponiveis:
        ano_poupanca = anos_disponiveis[-1]  # Ano mais recente
        rp_ano_recente = resultados_primarios.get(ano_poupanca, 0.0)
        rcl_ano_recente = rcls_periodo.get(ano_poupanca, 0.0)
        
        if rcl_ano_recente > 0:
            poupanca_fiscal = (rp_ano_recente / rcl_ano_recente) * 100.0

    # ✅ NOVA VARIÁVEL: Diferença RP vs Meta do ano mais recente
    dif_rp_meta = 0.0
    meta_ano_recente = 0.0
    
    if ano_poupanca is not None:
        meta_ano_recente = metas_primarias.get(ano_poupanca, 0.0)
        dif_rp_meta = rp_ano_recente - meta_ano_recente

    if debug and cod == 11:
        print(f"\n  💰 MÉTRICAS FISCAIS (Rondônia):")
        print(f"    Ano mais recente: {ano_poupanca}")
        print(f"    RP ano recente: R$ {rp_ano_recente/1e9:+.2f} bi")
        print(f"    Meta ano recente: R$ {meta_ano_recente/1e9:+.2f} bi")
        print(f"    RCL ano recente: R$ {rcl_ano_recente/1e9:.2f} bi")
        print(f"    Poupança Fiscal (RP/RCL): {poupanca_fiscal:+.2f}%")
        print(f"    Diferença RP - Meta: R$ {dif_rp_meta/1e9:+.2f} bi")
        print(f"    RP/RCL inicial ({ano_ini_rp}): {rp_rcl_ini_pct:+.2f}%")
        print(f"    RP/RCL final   ({ano_fim_rp}): {rp_rcl_fim_pct:+.2f}%")
        print(f"    Δ RP/RCL (pp): {delta_rp_rcl_pp:+.2f} pp")

    return {
        "Estado": nome,
        "Codigo_UF": cod,
        "Reeleito": bool(reeleito),

        "DCL_RCL_Pct_Inicial": round(dcl_pct_ini, 2),
        "DCL_RCL_Pct_Atual": round(dcl_pct_fim, 2),
        "Delta_DCL_pp": round(dcl_pct_fim - dcl_pct_ini, 2),

        "DTP_RCL_Pct_Inicial": round(dtp_pct_ini, 2),
        "DTP_RCL_Pct_Atual": round(dtp_pct_fim, 2),
        "Delta_DTP_pp": round(dtp_pct_fim - dtp_pct_ini, 2),

        # Evolução do RP/RCL do início ao fim do mandato
        "RP_RCL_Pct_Inicial": round(rp_rcl_ini_pct, 2),
        "RP_RCL_Pct_Atual": round(rp_rcl_fim_pct, 2),
        "Delta_RP_RCL_pp": round(delta_rp_rcl_pp, 2),
        "Ano_Inicial_RP": int(ano_ini_rp) if ano_ini_rp is not None else None,
        "Ano_Final_RP": int(ano_fim_rp) if ano_fim_rp is not None else None,

        # ✅ NOVAS VARIÁVEIS: Poupança Fiscal (ano recente) e Diferença RP vs Meta
        "Poupanca_Fiscal_Pct": round(poupanca_fiscal, 2),
        "Ano_Poupanca_Fiscal": int(ano_poupanca) if ano_poupanca is not None else None,
        "Resultado_Primario_Ano_Recente_Bi": round(rp_ano_recente / 1e9, 2),
        "Meta_Primaria_Ano_Recente_Bi": round(meta_ano_recente / 1e9, 2),
        "RCL_Ano_Recente_Bi": round(rcl_ano_recente / 1e9, 2),
        "Diferenca_RP_Meta_Bi": round(dif_rp_meta / 1e9, 2),

        "Ano_Inicial": int(ano_ini),
        "Ano_Final_DCL": int(ano_fim),
        "Ano_Final_DTP": int(ano_a1_fim),
    }


def main(debug: bool = False, ano_inicial: int = 2018, out_csv: str = "dados_ranking_estados.csv") -> pd.DataFrame:
    """Executa o ETL completo para todos os estados."""
    print(f"Iniciando ETL para {len(CODIGOS_UF)} estados...")
    print(f"Ano inicial: {ano_inicial}")
    print("-" * 60)

    mandatos = _load_mandatos_governadores(ARQ_GOV)

    resultados = []
    for idx, (cod, nome) in enumerate(CODIGOS_UF.items(), 1):
        print(f"[{idx}/{len(CODIGOS_UF)}] Processando {nome}...")
        try:
            resultado = process_uf(cod, nome, cod in UF_REELEITOS, ano_ini=ano_inicial, debug=debug, mandatos=mandatos)
            resultados.append(resultado)
        except Exception as e:
            print(f"  ⚠️  Erro ao processar {nome}: {e}")
            if debug:
                import traceback
                traceback.print_exc()

    if not resultados:
        print("❌ Nenhum resultado foi gerado!")
        return pd.DataFrame()

    df = pd.DataFrame(resultados)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print("-" * 60)
    print(f"✅ Arquivo '{out_csv}' gerado com sucesso!")
    print(f"Total de estados processados: {len(df)}")

    # Estatísticas
    print(f"\n📊 Estatísticas:")
    print(f"  Estados com DCL > 0: {(df['DCL_RCL_Pct_Atual'] > 0).sum()}/{len(df)}")
    print(f"  Estados com DTP > 0: {(df['DTP_RCL_Pct_Atual'] > 0).sum()}/{len(df)}")
    print(f"  Estados com Poupança Fiscal != 0: {(df['Poupanca_Fiscal_Pct'] != 0).sum()}/{len(df)}")
    print(f"  Estados com Delta RP/RCL != 0: {(df['Delta_RP_RCL_pp'] != 0).sum()}/{len(df)}")
    print(f"  Estados com Diferença RP-Meta != 0: {(df['Diferenca_RP_Meta_Bi'] != 0).sum()}/{len(df)}")

    return df


if __name__ == "__main__":
    df_resultado = main(debug=True, ano_inicial=2018, out_csv="dados_ranking_estados.csv")

    if not df_resultado.empty:
        print("\n📊 Resumo dos dados:")
        print(df_resultado[[
            "Estado", "Reeleito",
            "Delta_DCL_pp", "Delta_DTP_pp",
            "RP_RCL_Pct_Inicial", "RP_RCL_Pct_Atual", "Delta_RP_RCL_pp",
            "Poupanca_Fiscal_Pct", "Diferenca_RP_Meta_Bi"
        ]].head(10))