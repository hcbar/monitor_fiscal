# etl_csv_local.py
# ETL para processar CSVs baixados manualmente do SICONFI
# 
# Este script lê os arquivos CSV baixados do site do Tesouro Nacional
# e gera o arquivo dados_ranking_estados.csv para o dashboard
#
# Estrutura esperada dos CSVs:
#   dados_brutos/{ano}/resultado_primario/{ano}_{bim}bim_resultado_primario_acima_da_linha.csv
#   dados_brutos/{ano}/receita_corrente_liquida/{ano}_{bim}bim_receita_corrente_liquida.csv
#   dados_brutos/{ano}/meta_primario/{ano}_{bim}bim_meta_primario.csv

import os
import re
import glob
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, field
import pandas as pd

# === CONFIGURAÇÕES ===
PASTA_DADOS = "dados_brutos"
ARQ_GOV = "governadores.csv"
ARQ_SAIDA = "dados_ranking_estados.csv"

CODIGOS_UF = {
    11: "Rondônia", 12: "Acre", 13: "Amazonas", 14: "Roraima", 15: "Pará",
    16: "Amapá", 17: "Tocantins", 21: "Maranhão", 22: "Piauí", 23: "Ceará",
    24: "Rio Grande do Norte", 25: "Paraíba", 26: "Pernambuco", 27: "Alagoas",
    28: "Sergipe", 29: "Bahia", 31: "Minas Gerais", 32: "Espírito Santo",
    33: "Rio de Janeiro", 35: "São Paulo", 41: "Paraná", 42: "Santa Catarina",
    43: "Rio Grande do Sul", 50: "Mato Grosso do Sul", 51: "Mato Grosso",
    52: "Goiás", 53: "Distrito Federal",
}

# Estados com governadores reeleitos (mandato 2023-2026)
UF_REELEITOS = [11, 12, 13, 14, 15, 17, 21, 24, 25, 28, 31, 32, 33, 50, 51, 52, 53]


# === FUNÇÕES AUXILIARES ===

def parse_valor_brasileiro(valor: str) -> float:
    """Converte valor no formato brasileiro '1.234.567,89' para float."""
    if pd.isna(valor) or valor == '' or valor is None:
        return 0.0
    s = str(valor).strip().replace('"', '')
    if not s or s.lower() in ['nan', 'none', '-']:
        return 0.0
    # Remove pontos de milhar e troca vírgula por ponto
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def limpar_nome_estado(nome: str) -> str:
    """Remove prefixos como 'Governo do Estado do/da/de'."""
    s = str(nome)
    s = re.sub(r"^(Governo|Estado)\s+(do|da|de)\s+", "", s, flags=re.I)
    s = re.sub(r"^(do|da|de)\s+", "", s, flags=re.I)
    return s.strip().title()


def carregar_governadores(path: str = ARQ_GOV) -> Dict[str, Tuple[Optional[int], Optional[int]]]:
    """Carrega dados de mandatos dos governadores."""
    if not os.path.exists(path):
        return {}
    
    try:
        gov = pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        gov = pd.read_csv(path, encoding='latin1')
    
    # Encontrar colunas
    col_estado = next((c for c in gov.columns if c.lower() in ['estado', 'uf_nome']), None)
    col_ini = next((c for c in gov.columns if 'inicio' in c.lower()), None)
    col_fim = next((c for c in gov.columns if 'fim' in c.lower()), None)
    
    if not col_estado:
        return {}
    
    out = {}
    for _, r in gov.iterrows():
        key = limpar_nome_estado(r[col_estado])
        ini = int(r[col_ini]) if col_ini and pd.notna(r.get(col_ini)) else None
        fim = int(r[col_fim]) if col_fim and pd.notna(r.get(col_fim)) else None
        out[key] = (ini, fim)
    
    return out


# === LEITURA DE CSVs ===

def ler_csv_siconfi(filepath: str) -> pd.DataFrame:
    """
    Lê CSV do SICONFI pulando as 5 linhas de cabeçalho.
    Retorna DataFrame com colunas padronizadas.
    """
    if not os.path.exists(filepath):
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(
            filepath,
            encoding='latin1',
            sep=';',
            skiprows=5,
            quotechar='"'
        )
        return df
    except Exception as e:
        print(f"  ⚠️ Erro ao ler {filepath}: {e}")
        return pd.DataFrame()


def extrair_resultado_primario_csv(df: pd.DataFrame, cod_uf: int) -> float:
    """
    Extrai o Resultado Primário (COM RPPS) para um estado específico.
    A partir de 2024, há duas linhas: COM RPPS e SEM RPPS. Usamos COM RPPS.
    """
    if df.empty or 'Cod.IBGE' not in df.columns:
        return 0.0
    
    # Filtrar pelo código do estado
    df_uf = df[df['Cod.IBGE'] == cod_uf]
    if df_uf.empty:
        return 0.0
    
    # Preferir "COM RPPS", senão pegar a primeira linha
    mask_com_rpps = df_uf['Conta'].str.contains('COM RPPS', case=False, na=False)
    if mask_com_rpps.any():
        valor = df_uf.loc[mask_com_rpps, 'Valor'].iloc[0]
    else:
        valor = df_uf['Valor'].iloc[0]
    
    return parse_valor_brasileiro(valor)


def extrair_rcl_csv(df: pd.DataFrame, cod_uf: int) -> float:
    """
    Extrai a Receita Corrente Líquida (12 meses) para um estado específico.
    """
    if df.empty or 'Cod.IBGE' not in df.columns:
        return 0.0
    
    # Filtrar pelo código do estado
    df_uf = df[df['Cod.IBGE'] == cod_uf]
    if df_uf.empty:
        return 0.0
    
    # Filtrar pela conta de RCL e coluna de 12 meses
    mask_rcl = df_uf['Conta'].str.contains(r'RECEITA CORRENTE LÍQUIDA.*VII', case=False, na=False, regex=True)
    mask_12m = df_uf['Coluna'] == 'TOTAL (ÚLTIMOS 12 MESES)'
    
    df_rcl = df_uf[mask_rcl & mask_12m]
    if df_rcl.empty:
        # Fallback: tentar outra forma
        mask_rcl2 = df_uf['Conta'].str.upper() == 'RECEITA CORRENTE LÍQUIDA (VII) = (I - II + III - IV + V - VI)'
        df_rcl = df_uf[mask_rcl2 & mask_12m]
    
    if df_rcl.empty:
        return 0.0
    
    return parse_valor_brasileiro(df_rcl['Valor'].iloc[0])


def extrair_meta_primario_csv(df: pd.DataFrame, cod_uf: int) -> float:
    """
    Extrai a Meta de Resultado Primário para um estado específico.
    """
    if df.empty or 'Cod.IBGE' not in df.columns:
        return 0.0
    
    df_uf = df[df['Cod.IBGE'] == cod_uf]
    if df_uf.empty:
        return 0.0
    
    # Procurar pela meta (COM RPPS se disponível)
    mask_meta = df_uf['Conta'].str.contains('META', case=False, na=False)
    mask_com_rpps = df_uf['Conta'].str.contains('COM RPPS', case=False, na=False)
    
    df_meta = df_uf[mask_meta & mask_com_rpps]
    if df_meta.empty:
        df_meta = df_uf[mask_meta]
    
    if df_meta.empty:
        return 0.0
    
    return parse_valor_brasileiro(df_meta['Valor'].iloc[0])


# === PROCESSAMENTO PRINCIPAL ===

def listar_anos_disponiveis() -> List[int]:
    """Lista os anos que têm CSVs disponíveis."""
    anos = []
    for item in os.listdir(PASTA_DADOS):
        path = os.path.join(PASTA_DADOS, item)
        if os.path.isdir(path) and item.isdigit():
            anos.append(int(item))
    return sorted(anos)


def carregar_dados_ano(ano: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Carrega os CSVs do 6º bimestre de um ano específico.
    Retorna: (df_resultado_primario, df_rcl, df_meta)
    """
    pasta_ano = os.path.join(PASTA_DADOS, str(ano))
    
    # Resultado Primário - 6º bimestre
    path_rp = os.path.join(pasta_ano, 'resultado_primario', f'{ano}_6bim_resultado_primario_acima_da_linha.csv')
    df_rp = ler_csv_siconfi(path_rp)
    
    # RCL - 6º bimestre
    path_rcl = os.path.join(pasta_ano, 'receita_corrente_liquida', f'{ano}_6bim_receita_corrente_liquida.csv')
    df_rcl = ler_csv_siconfi(path_rcl)
    
    # Meta - 6º bimestre
    path_meta = os.path.join(pasta_ano, 'meta_primario', f'{ano}_6bim_meta_primario.csv')
    df_meta = ler_csv_siconfi(path_meta)
    
    return df_rp, df_rcl, df_meta


def processar_estado(cod: int, nome: str, reeleito: bool, 
                     anos: List[int], mandatos: Dict, debug: bool = False) -> dict:
    """
    Processa os dados fiscais de um estado.
    """
    resultados_primarios = {}
    metas_primarias = {}
    rcls = {}
    
    for ano in anos:
        df_rp, df_rcl, df_meta = carregar_dados_ano(ano)
        
        rp = extrair_resultado_primario_csv(df_rp, cod)
        rcl = extrair_rcl_csv(df_rcl, cod)
        meta = extrair_meta_primario_csv(df_meta, cod)
        
        if rp != 0.0:
            resultados_primarios[ano] = rp
        if rcl != 0.0:
            rcls[ano] = rcl
        if meta != 0.0:
            metas_primarias[ano] = meta
        
        if debug:
            print(f"    {ano}: RP={rp/1e9:+.2f}bi | RCL={rcl/1e9:.2f}bi | Meta={meta/1e9:+.2f}bi")
    
    # Calcular métricas
    anos_comuns = sorted(set(resultados_primarios.keys()) & set(rcls.keys()))
    
    if not anos_comuns:
        return criar_resultado_vazio(nome, cod, reeleito)
    
    # RP/RCL por ano
    rp_rcl_pct = {}
    for y in anos_comuns:
        rcl_y = rcls.get(y, 0)
        rp_y = resultados_primarios.get(y, 0)
        if rcl_y > 0:
            rp_rcl_pct[y] = (rp_y / rcl_y) * 100.0
    
    # Anos inicial e final
    ano_ini = anos_comuns[0]
    ano_fim = anos_comuns[-1]
    
    # Mandatos (se disponível)
    mandato_ini, mandato_fim = mandatos.get(limpar_nome_estado(nome), (None, None))
    if mandato_ini and mandato_ini in rp_rcl_pct:
        ano_ini = mandato_ini
    if mandato_fim and mandato_fim in rp_rcl_pct:
        ano_fim = mandato_fim
    
    rp_rcl_ini = rp_rcl_pct.get(ano_ini, 0.0)
    rp_rcl_fim = rp_rcl_pct.get(ano_fim, 0.0)
    delta_rp_rcl = rp_rcl_fim - rp_rcl_ini
    
    # Poupança Fiscal (ano mais recente)
    ano_poupanca = anos_comuns[-1]
    rp_ano_recente = resultados_primarios.get(ano_poupanca, 0.0)
    rcl_ano_recente = rcls.get(ano_poupanca, 0.0)
    poupanca_fiscal = (rp_ano_recente / rcl_ano_recente * 100.0) if rcl_ano_recente > 0 else 0.0
    
    # Diferença RP vs Meta
    meta_ano_recente = metas_primarias.get(ano_poupanca, 0.0)
    dif_rp_meta = rp_ano_recente - meta_ano_recente
    
    return {
        "Estado": nome,
        "Codigo_UF": cod,
        "Reeleito": bool(reeleito),
        
        # Placeholders para DCL e DTP (não calculados via CSV local ainda)
        "DCL_RCL_Pct_Inicial": 0.0,
        "DCL_RCL_Pct_Atual": 0.0,
        "Delta_DCL_pp": 0.0,
        
        "DTP_RCL_Pct_Inicial": 0.0,
        "DTP_RCL_Pct_Atual": 0.0,
        "Delta_DTP_pp": 0.0,
        
        # Evolução RP/RCL
        "RP_RCL_Pct_Inicial": round(rp_rcl_ini, 2),
        "RP_RCL_Pct_Atual": round(rp_rcl_fim, 2),
        "Delta_RP_RCL_pp": round(delta_rp_rcl, 2),
        "Ano_Inicial_RP": ano_ini,
        "Ano_Final_RP": ano_fim,
        
        # Poupança Fiscal
        "Poupanca_Fiscal_Pct": round(poupanca_fiscal, 2),
        "Ano_Poupanca_Fiscal": ano_poupanca,
        "Resultado_Primario_Ano_Recente_Bi": round(rp_ano_recente / 1e9, 2),
        "Meta_Primaria_Ano_Recente_Bi": round(meta_ano_recente / 1e9, 2),
        "RCL_Ano_Recente_Bi": round(rcl_ano_recente / 1e9, 2),
        "Diferenca_RP_Meta_Bi": round(dif_rp_meta / 1e9, 2),
        
        "Ano_Inicial": ano_ini,
        "Ano_Final_DCL": ano_fim,
        "Ano_Final_DTP": ano_fim,
    }


def criar_resultado_vazio(nome: str, cod: int, reeleito: bool) -> dict:
    """Cria resultado com valores zerados quando não há dados."""
    return {
        "Estado": nome,
        "Codigo_UF": cod,
        "Reeleito": bool(reeleito),
        "DCL_RCL_Pct_Inicial": 0.0,
        "DCL_RCL_Pct_Atual": 0.0,
        "Delta_DCL_pp": 0.0,
        "DTP_RCL_Pct_Inicial": 0.0,
        "DTP_RCL_Pct_Atual": 0.0,
        "Delta_DTP_pp": 0.0,
        "RP_RCL_Pct_Inicial": 0.0,
        "RP_RCL_Pct_Atual": 0.0,
        "Delta_RP_RCL_pp": 0.0,
        "Ano_Inicial_RP": None,
        "Ano_Final_RP": None,
        "Poupanca_Fiscal_Pct": 0.0,
        "Ano_Poupanca_Fiscal": None,
        "Resultado_Primario_Ano_Recente_Bi": 0.0,
        "Meta_Primaria_Ano_Recente_Bi": 0.0,
        "RCL_Ano_Recente_Bi": 0.0,
        "Diferenca_RP_Meta_Bi": 0.0,
        "Ano_Inicial": None,
        "Ano_Final_DCL": None,
        "Ano_Final_DTP": None,
    }


def main(debug: bool = False):
    """Função principal do ETL."""
    print("=" * 60)
    print("ETL Monitor Fiscal - Processamento de CSVs Locais")
    print("=" * 60)
    
    # Listar anos disponíveis
    anos = listar_anos_disponiveis()
    if not anos:
        print(f"❌ Nenhum ano encontrado em {PASTA_DADOS}/")
        return pd.DataFrame()
    
    print(f"📅 Anos disponíveis: {anos}")
    
    # Carregar mandatos
    mandatos = carregar_governadores(ARQ_GOV)
    print(f"👔 Governadores carregados: {len(mandatos)}")
    
    # Processar estados
    resultados = []
    print(f"\n🔄 Processando {len(CODIGOS_UF)} estados...")
    print("-" * 60)
    
    for idx, (cod, nome) in enumerate(CODIGOS_UF.items(), 1):
        print(f"[{idx:2d}/{len(CODIGOS_UF)}] {nome}...")
        
        try:
            resultado = processar_estado(
                cod, nome, 
                reeleito=(cod in UF_REELEITOS),
                anos=anos,
                mandatos=mandatos,
                debug=debug
            )
            resultados.append(resultado)
            
            if debug:
                pf = resultado.get('Poupanca_Fiscal_Pct', 0)
                print(f"       → Poupança Fiscal: {pf:+.2f}%")
                
        except Exception as e:
            print(f"  ⚠️ Erro: {e}")
            if debug:
                import traceback
                traceback.print_exc()
    
    # Criar DataFrame e salvar
    if not resultados:
        print("\n❌ Nenhum resultado gerado!")
        return pd.DataFrame()
    
    df = pd.DataFrame(resultados)
    df.to_csv(ARQ_SAIDA, index=False, encoding='utf-8-sig')
    
    print("\n" + "=" * 60)
    print(f"✅ Arquivo '{ARQ_SAIDA}' gerado com sucesso!")
    print(f"   Estados processados: {len(df)}")
    print("=" * 60)
    
    # Estatísticas
    print("\n📊 Estatísticas:")
    pf_pos = (df['Poupanca_Fiscal_Pct'] > 0).sum()
    pf_neg = (df['Poupanca_Fiscal_Pct'] < 0).sum()
    print(f"   Superávit (Poupança > 0): {pf_pos} estados")
    print(f"   Déficit (Poupança < 0): {pf_neg} estados")
    print(f"   Mediana Poupança Fiscal: {df['Poupanca_Fiscal_Pct'].median():.2f}%")
    
    # Top 5 melhores e piores
    df_sorted = df.sort_values('Poupanca_Fiscal_Pct', ascending=False)
    print("\n🏆 Top 5 - Melhor Poupança Fiscal:")
    for _, row in df_sorted.head(5).iterrows():
        print(f"   {row['Estado']}: {row['Poupanca_Fiscal_Pct']:+.2f}%")
    
    print("\n⚠️ Top 5 - Pior Poupança Fiscal:")
    for _, row in df_sorted.tail(5).iterrows():
        print(f"   {row['Estado']}: {row['Poupanca_Fiscal_Pct']:+.2f}%")
    
    return df


if __name__ == "__main__":
    import sys
    debug = "--debug" in sys.argv or "-d" in sys.argv
    df = main(debug=debug)
