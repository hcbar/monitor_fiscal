# etl_completo_final.py
# ETL SICONFI (Estados) – DCL/RCL, DTP/RCL e Poupança Corrente/RCL

import re
import time
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
        return float(x)
    s = str(x).strip()
    if not s or s.lower() in ['nan', 'none', '']:
        return 0.0
    s = s.replace(" ", "").replace(".", "").replace(",", ".")
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
            r = SESSION.get(url, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            items = payload.get("items", [])
            
            if not items:
                if debug:
                    print(f"[DEBUG] Vazio: {tipo} {ano} p{periodo_api} UF{cod_ibge}")
                return pd.DataFrame()
            
            df = pd.DataFrame(items)

            # Filtra por anexo se especificado
            if anexo and not df.empty:
                pat = _normalize_anexo_pattern(anexo)
                col_anexo = _pick_col(df, ["no_anexo", "anexo", "ds_anexo"])
                if col_anexo:
                    mask = df[col_anexo].astype(str).str.contains(pat, na=False, regex=True)
                    df = df[mask].reset_index(drop=True)
                    if df.empty and debug:
                        print(f"[DEBUG] Anexo '{anexo}' não encontrado: {tipo} {ano} p{periodo_api} UF{cod_ibge}")

            return df

        except requests.exceptions.RequestException as e:
            if debug:
                print(f"[DEBUG] Tentativa {attempt}/{max_retries} falhou: {tipo} {ano} p{periodo_api} UF{cod_ibge} -> {e}")
            if attempt < max_retries:
                time.sleep(1.5 * attempt)
        except Exception as e:
            if debug:
                print(f"[DEBUG] Erro inesperado: {tipo} {ano} p{periodo_api} UF{cod_ibge} -> {e}")
            break

    return pd.DataFrame()


def get_data_rreo_bimestre(ano: int, bimestre: int, cod_ibge: int, anexo: str = "Anexo 01", debug: bool = False) -> pd.DataFrame:
    """RREO usa nr_periodo = bimestre (1..6)."""
    return get_data(ano, bimestre, cod_ibge, "RREO", anexo=anexo, debug=debug)


# --------------------------------------------------------------------------------------
# Extractor usando Identificador da Conta (mais confiável)
# --------------------------------------------------------------------------------------
def pick_by_identifier(
    df: pd.DataFrame,
    identifier: str,
    coluna_contains: str,
    debug_name: str = ""
) -> float:
    """
    Busca por 'Identificador da Conta' (id técnico) - MAIS CONFIÁVEL!
    """
    if df is None or df.empty:
        return 0.0
    
    col_id = _pick_col(df, ["identificador_conta", "id_conta", "cod_conta"])
    col_coluna = _pick_col(df, ["coluna", "no_coluna", "ds_coluna"])
    col_valor = _pick_col(df, ["valor", "vl_valor", "nu_valor"])
    
    if not col_id or not col_coluna or not col_valor:
        return 0.0
    
    # Escape regex
    coluna_escaped = re.escape(coluna_contains)
    id_escaped = re.escape(identifier)
    
    # Filtra por identificador E coluna
    mask = (
        df[col_id].astype(str).str.contains(id_escaped, case=False, na=False, regex=True) &
        df[col_coluna].astype(str).str.contains(coluna_escaped, case=False, na=False, regex=True)
    )
    
    hits = df[mask]
    if hits.empty:
        return 0.0
    
    # Pega o primeiro (ou soma se houver múltiplos)
    return _parse_number(hits.iloc[0][col_valor])


def pick_by_conta_name(
    df: pd.DataFrame,
    conta_terms: tuple,
    coluna_contains: str
) -> float:
    """Fallback: busca por nome da conta (menos confiável)."""
    if df is None or df.empty:
        return 0.0

    col_conta = _pick_col(df, ["conta", "no_conta", "ds_conta"])
    col_coluna = _pick_col(df, ["coluna", "no_coluna", "ds_coluna"])
    col_valor = _pick_col(df, ["valor", "vl_valor", "nu_valor"])

    if not col_conta or not col_coluna or not col_valor:
        return 0.0

    # Filtra coluna
    coluna_escaped = re.escape(coluna_contains)
    mask_col = df[col_coluna].astype(str).str.contains(coluna_escaped, case=False, na=False, regex=True)
    d = df[mask_col].copy()
    if d.empty:
        return 0.0

    # Filtra conta (todos os termos devem estar presentes)
    mask = pd.Series(True, index=d.index)
    for term in conta_terms:
        term_escaped = re.escape(term)
        mask &= d[col_conta].astype(str).str.contains(term_escaped, case=False, na=False, regex=True)

    hits = d[mask]
    if hits.empty:
        return 0.0

    return _parse_number(hits.iloc[0][col_valor])


# --------------------------------------------------------------------------------------
# Find latest available
# --------------------------------------------------------------------------------------
def find_latest_rgf_anexo(cod_ibge: int, anexo: str, debug: bool = False, start_year: int = 2018) -> Tuple[int, int, pd.DataFrame]:
    """Procura do ano corrente para trás o último RGF (3º quadrimestre)."""
    this_year = datetime.now().year
    for y in range(this_year, start_year - 1, -1):
        df = get_data(y, 3, cod_ibge, "RGF", anexo=anexo, debug=debug)
        if not df.empty:
            return y, 3, df
    return start_year, 3, pd.DataFrame()


def find_latest_rreo_year_end(cod_ibge: int, anexo: str = "Anexo 01", debug: bool = False, start_year: int = 2018) -> Tuple[int, int, pd.DataFrame]:
    """Procura do ano corrente para trás o último RREO com 6º bimestre."""
    this_year = datetime.now().year
    for y in range(this_year, start_year - 1, -1):
        df = get_data_rreo_bimestre(y, 6, cod_ibge, anexo=anexo, debug=debug)
        if not df.empty:
            return y, 6, df
    return start_year, 6, pd.DataFrame()


# --------------------------------------------------------------------------------------
# Core per-Estado
# --------------------------------------------------------------------------------------
def process_uf(cod: int, nome: str, reeleito: bool, ano_ini: int = 2018, debug: bool = False) -> dict:
    """Processa dados de um estado específico."""
    
    # --- 1) DCL/RCL (RGF Anexo 02) ---
    df_a2_ini = get_data(ano_ini, 3, cod, "RGF", anexo="Anexo 02", debug=debug)
    ano_fim, _, df_a2_fim = find_latest_rgf_anexo(cod, anexo="Anexo 02", debug=debug, start_year=ano_ini)

    # Tenta pegar o percentual direto
    dcl_pct_ini = pick_by_conta_name(df_a2_ini, ("DÍVIDA CONSOLIDADA LÍQUIDA", "RCL"), "Até o 3")
    dcl_pct_fim = pick_by_conta_name(df_a2_fim, ("DÍVIDA CONSOLIDADA LÍQUIDA", "RCL"), "Até o 3")

    # Se não encontrou, calcula manualmente
    if dcl_pct_ini == 0:
        dcl = pick_by_conta_name(df_a2_ini, ("DÍVIDA CONSOLIDADA LÍQUIDA",), "Até o 3")
        rcl = pick_by_conta_name(df_a2_ini, ("RECEITA CORRENTE LÍQUIDA",), "Até o 3")
        if rcl > 0:
            dcl_pct_ini = (dcl / rcl) * 100

    if dcl_pct_fim == 0:
        dcl = pick_by_conta_name(df_a2_fim, ("DÍVIDA CONSOLIDADA LÍQUIDA",), "Até o 3")
        rcl = pick_by_conta_name(df_a2_fim, ("RECEITA CORRENTE LÍQUIDA",), "Até o 3")
        if rcl > 0:
            dcl_pct_fim = (dcl / rcl) * 100

    # --- 2) DTP/RCL (RGF Anexo 01 para DTP, RREO Anexo 03 para RCL) ---
    df_a1_ini = get_data(ano_ini, 3, cod, "RGF", anexo="Anexo 01", debug=debug)
    ano_a1_fim, _, df_a1_fim = find_latest_rgf_anexo(cod, anexo="Anexo 01", debug=debug, start_year=ano_ini)

    def extract_dtp_rcl(df_rgf: pd.DataFrame, ano: int) -> Tuple[float, float]:
        """Extrai DTP do RGF e RCL do RREO."""
        # Busca DTP (DESPESA BRUTA COM PESSOAL) no RGF Anexo 01
        dtp = pick_by_identifier(df_rgf, "siconfi-cor_DespesaComPessoalBruta", "TOTAL (ÚLTIMOS 12 MESES)")
        if dtp == 0:
            # Fallback: busca por nome
            dtp = pick_by_conta_name(df_rgf, ("DESPESA BRUTA COM PESSOAL",), "TOTAL (ÚLTIMOS 12 MESES)")
        
        # Busca RCL no RREO Anexo 03 (onde realmente está!)
        df_rcl = get_data_rreo_bimestre(ano, 6, cod, anexo="Anexo 03", debug=False)
        rcl = pick_by_identifier(df_rcl, "siconfi-cor_RREO3ReceitaCorrenteLiquida", "TOTAL (ÚLTIMOS 12 MESES)")
        if rcl == 0:
            # Fallback: busca por nome
            rcl = pick_by_conta_name(df_rcl, ("RECEITA CORRENTE LÍQUIDA",), "TOTAL (ÚLTIMOS 12 MESES)")
        
        return dtp, rcl

    dtp_ini, rcl_dtp_ini = extract_dtp_rcl(df_a1_ini, ano_ini)
    dtp_fim, rcl_dtp_fim = extract_dtp_rcl(df_a1_fim, ano_a1_fim)
    
    dtp_pct_ini = (dtp_ini / rcl_dtp_ini * 100) if rcl_dtp_ini > 0 else 0.0
    dtp_pct_fim = (dtp_fim / rcl_dtp_fim * 100) if rcl_dtp_fim > 0 else 0.0

    if debug and cod == 11:
        print(f"\n  💼 DTP (Rondônia):")
        print(f"    DTP inicial = {dtp_ini:,.2f}, RCL = {rcl_dtp_ini:,.2f}, % = {dtp_pct_ini:.2f}%")
        print(f"    DTP atual = {dtp_fim:,.2f}, RCL = {rcl_dtp_fim:,.2f}, % = {dtp_pct_fim:.2f}%")

    # --- 3) POUPANÇA CORRENTE: (RC - DC)/RCL (do RREO Anexo 01) ---
    df_rreo_ini = get_data_rreo_bimestre(ano_ini, 6, cod, anexo="Anexo 01", debug=debug)
    ano_rreo_fim, _, df_rreo_fim = find_latest_rreo_year_end(cod, anexo="Anexo 01", debug=debug, start_year=ano_ini)

    def calc_poup(df_rreo: pd.DataFrame, ano: int) -> float:
        """Calcula poupança corrente como % da RCL."""
        if df_rreo.empty:
            return 0.0
        
        # Receitas Correntes - usar identificador "Até o Bimestre (c)"
        rc = pick_by_identifier(df_rreo, "siconfi-cor_ReceitasCorrentes", "Até o Bimestre (c)")
        if rc == 0:
            # Fallback
            rc = pick_by_conta_name(df_rreo, ("RECEITAS CORRENTES",), "Até o Bimestre (c)")
        
        # Despesas Correntes - usar identificador "Até o Bimestre (c)" ou "Despesas Liquidadas"
        dc = pick_by_identifier(df_rreo, "siconfi-cor_DespesasCorrentes", "Despesas Liquidadas")
        if dc == 0:
            dc = pick_by_identifier(df_rreo, "siconfi-cor_DespesasCorrentes", "Até o Bimestre")
        if dc == 0:
            # Fallback
            dc = pick_by_conta_name(df_rreo, ("DESPESAS CORRENTES",), "Despesas Liquidadas")
        
        # RCL do mesmo ano (busca no RREO Anexo 03)
        df_rcl = get_data_rreo_bimestre(ano, 6, cod, anexo="Anexo 03", debug=False)
        rcl = pick_by_identifier(df_rcl, "siconfi-cor_RREO3ReceitaCorrenteLiquida", "TOTAL (ÚLTIMOS 12 MESES)")
        if rcl == 0:
            rcl = pick_by_conta_name(df_rcl, ("RECEITA CORRENTE LÍQUIDA",), "TOTAL (ÚLTIMOS 12 MESES)")
        
        if debug and cod == 11:
            print(f"\n  💰 POUPANÇA (Rondônia, ano {ano}):")
            print(f"    RC = {rc:,.2f}, DC = {dc:,.2f}, RCL = {rcl:,.2f}")
            if rcl > 0:
                print(f"    Poupança = {((rc - dc) / rcl * 100):.2f}%")
        
        return ((rc - dc) / rcl * 100) if rcl > 0 else 0.0

    poup_pct_ini = calc_poup(df_rreo_ini, ano_ini)
    poup_pct_fim = calc_poup(df_rreo_fim, ano_rreo_fim)


    # --- 4) ARRECADAÇÃO PRÓPRIA: Tributos/RCL (do RREO Anexo 01) ---
    def calc_tributos_rcl(df_rreo: pd.DataFrame, ano: int) -> float:
        """Calcula arrecadação própria (Tributos/RCL)."""
        if df_rreo.empty:
            return 0.0
        
        # Busca Impostos (identificador: siconfi-cor_Impostos)
        impostos = pick_by_identifier(df_rreo, "siconfi-cor_Impostos", "Até o Bimestre")
        if impostos == 0:
            impostos = pick_by_conta_name(df_rreo, ("Impostos",), "Até o Bimestre")
        
        # Busca Taxas (identificador: siconfi-cor_Taxas)
        taxas = pick_by_identifier(df_rreo, "siconfi-cor_Taxas", "Até o Bimestre")
        if taxas == 0:
            taxas = pick_by_conta_name(df_rreo, ("Taxas",), "Até o Bimestre")
        
        # Busca Contribuição de Melhoria (identificador: siconfi-cor_ContribuicaoDeMelhoria)
        contrib = pick_by_identifier(df_rreo, "siconfi-cor_ContribuicaoDeMelhoria", "Até o Bimestre")
        if contrib == 0:
            contrib = pick_by_conta_name(df_rreo, ("Contribuição de Melhoria",), "Até o Bimestre")
        
        # Soma os tributos
        tributos = impostos + taxas + contrib
        
        # RCL do mesmo ano (busca no RREO Anexo 03)
        df_rcl = get_data_rreo_bimestre(ano, 6, cod, anexo="Anexo 03", debug=False)
        rcl = pick_by_identifier(df_rcl, "siconfi-cor_RREO3ReceitaCorrenteLiquida", "TOTAL (ÚLTIMOS 12 MESES)")
        if rcl == 0:
            rcl = pick_by_conta_name(df_rcl, ("RECEITA CORRENTE LÍQUIDA",), "TOTAL (ÚLTIMOS 12 MESES)")
        
        if debug and cod == 11:
            print(f"\n  💰 ARRECADAÇÃO PRÓPRIA (Rondônia, ano {ano}):")
            print(f"    Impostos = {impostos:,.2f}, Taxas = {taxas:,.2f}, Contrib = {contrib:,.2f}")
            print(f"    Tributos = {tributos:,.2f}, RCL = {rcl:,.2f}")
            if rcl > 0:
                print(f"    Arrecadação Própria = {(tributos / rcl * 100):.2f}%")
        
        return (tributos / rcl * 100) if rcl > 0 else 0.0

    tributos_pct_ini = calc_tributos_rcl(df_rreo_ini, ano_ini)
    tributos_pct_fim = calc_tributos_rcl(df_rreo_fim, ano_rreo_fim)

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
        "Poupanca_RCL_Pct_Inicial": round(poup_pct_ini, 2),
        "Poupanca_RCL_Pct_Atual": round(poup_pct_fim, 2),
        "Delta_Poupanca_pp": round(poup_pct_fim - poup_pct_ini, 2),
        "Tributos_RCL_Pct_Inicial": round(tributos_pct_ini, 2),
        "Tributos_RCL_Pct_Atual": round(tributos_pct_fim, 2),
        "Delta_Tributos_pp": round(tributos_pct_fim - tributos_pct_ini, 2),
        
        "Delta_Poupanca_Ajustada_pp": round(
            (poup_pct_fim - poup_pct_ini) - (dcl_pct_fim - dcl_pct_ini),
            2
        ),
        "Ano_Inicial": int(ano_ini),
        "Ano_Final_DCL": int(ano_fim),
        "Ano_Final_DTP": int(ano_a1_fim),
        "Ano_Final_Poup": int(ano_rreo_fim),
    }


def main(debug: bool = False, ano_inicial: int = 2018, out_csv: str = "dados_ranking_estados.csv") -> pd.DataFrame:
    """Executa o ETL completo para todos os estados."""
    print(f"Iniciando ETL para {len(CODIGOS_UF)} estados...")
    print(f"Ano inicial: {ano_inicial}")
    print("-" * 60)
    
    resultados = []
    for idx, (cod, nome) in enumerate(CODIGOS_UF.items(), 1):
        print(f"[{idx}/{len(CODIGOS_UF)}] Processando {nome}...")
        try:
            resultado = process_uf(cod, nome, cod in UF_REELEITOS, ano_ini=ano_inicial, debug=debug)
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
    df.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print("-" * 60)
    print(f"✅ Arquivo '{out_csv}' gerado com sucesso!")
    print(f"Total de estados processados: {len(df)}")
    
    # Estatísticas
    print(f"\n📊 Estatísticas:")
    print(f"  Estados com DCL > 0: {(df['DCL_RCL_Pct_Atual'] > 0).sum()}/{len(df)}")
    print(f"  Estados com DTP > 0: {(df['DTP_RCL_Pct_Atual'] > 0).sum()}/{len(df)}")
    print(f"  Estados com Poupança != 0: {(df['Poupanca_RCL_Pct_Atual'] != 0).sum()}/{len(df)}")
    print(f"  Estados com Tributos > 0: {(df['Tributos_RCL_Pct_Atual'] > 0).sum()}/{len(df)}")
    
    return df


if __name__ == "__main__":
    # Debug ativado para Rondônia (cod 11)
    df_resultado = main(debug=True, ano_inicial=2018, out_csv="dados_ranking_estados.csv")
    
    # Exibir resumo
    if not df_resultado.empty:
        print("\n📊 Resumo dos dados:")
        print(df_resultado[['Estado', 'Reeleito', 'Delta_DCL_pp', 'Delta_DTP_pp', 'Delta_Poupanca_pp', 'Delta_Tributos_pp']].head(10))
