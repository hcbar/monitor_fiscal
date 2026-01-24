# etl_divida.py
# -*- coding: utf-8 -*-
"""
MONITOR FISCAL 2.1 (Robusto / Cruzamento por id_ente)

Roda no CMD dentro da pasta do projeto:
    python etl_divida.py

Gera:
    dados_ranking_estados.csv  (na mesma pasta)

Requisitos:
    pip install pandas requests
"""

import re
import time
import requests
import pandas as pd

BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"
REQ_SLEEP_SEC = 1.05  # a documentação recomenda <= 1 req/s

# ============================
# CONFIG: REELEIÇÃO
# ============================
# Coloque aqui as UFs cujos governadores atuais são reeleitos (2º mandato consecutivo),
# para comparar 2018->2025 ao invés de 2022->2025.
#
# Exemplo:
# UF_REELEITOS = {"SP", "MG", "RJ"}
UF_REELEITOS = set()

# ============================
# PERÍODOS DO MVP
# ============================
ANO_ANTIGO = 2018
PERIODO_ANTIGO = 3  # Quad 3 = fechamento do ano (geralmente mais comparável)

ANO_BASE = 2022
PERIODO_BASE = 3

ANO_ATUAL = 2025
PERIODO_ATUAL = 2  # mais seguro: Q2 costuma estar preenchido para todos mais cedo


def print_header():
    print("=== MONITOR FISCAL 2.1 (Cruzamento por id_ente) ===\n")


def _safe_get(session, url, params=None, timeout=25):
    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    time.sleep(REQ_SLEEP_SEC)
    return r.json()


def fetch_all_items(session, endpoint, params):
    """
    Paginador ORDS (quando existir link rel=next).
    """
    url = f"{BASE}/{endpoint}"
    out = []

    while True:
        payload = _safe_get(session, url, params=params)

        items = payload.get("items", [])
        out.extend(items)

        next_link = None
        for link in payload.get("links", []):
            if link.get("rel") == "next" and link.get("href"):
                next_link = link["href"]
                break

        if not next_link:
            break

        # O 'next' já vem completo com query; não precisa manter params
        url = next_link
        params = None

    return out


def listar_estados(session):
    """
    Lista entes estaduais (inclui DF), com sg_uf e id_ente.
    """
    items = fetch_all_items(session, "entes", params={})
    df = pd.DataFrame(items)

    if df.empty:
        raise RuntimeError("Não consegui listar entes na API (/entes).")

    # Filtra esfera estadual se existir
    if "co_esfera" in df.columns:
        df = df[df["co_esfera"].astype(str).str.upper().eq("E")].copy()

    # Filtra UF 2 letras
    if "sg_uf" not in df.columns or "id_ente" not in df.columns:
        raise RuntimeError(f"Resposta de /entes não tem colunas esperadas. Colunas: {list(df.columns)}")

    df["sg_uf"] = df["sg_uf"].astype(str).str.upper()
    df = df[df["sg_uf"].str.match(r"^[A-Z]{2}$")].copy()

    # Dedup por UF
    df = df.sort_values("id_ente").drop_duplicates("sg_uf", keep="first")

    # Mantém apenas o necessário
    keep = ["id_ente", "sg_uf"]
    if "no_ente" in df.columns:
        keep.append("no_ente")
    if "instituicao" in df.columns:
        keep.append("instituicao")

    df = df[keep].copy()
    return df.reset_index(drop=True)


def baixar_rgf_por_ente(session, id_ente, ano, periodo):
    """
    Baixa RGF do ente/ano/período.

    Endpoint: /rgf (aceita parâmetros como: an_exercicio, in_periodicidade, nr_periodo, co_esfera, co_poder, id_ente etc.)
    """
    params = {
        "an_exercicio": ano,
        "nr_periodo": periodo,
        "in_periodicidade": "Q",  # quadrimestral
        "co_poder": "E",          # Executivo
        "co_esfera": "E",         # Estadual
        "id_ente": id_ente,
        # NÃO fixo 'conta' aqui porque varia por anexo; trazemos o pacote e extraímos por padrão textual
    }

    items = fetch_all_items(session, "rgf", params=params)
    return pd.DataFrame(items)


def extrair_rcl_e_dcl(df):
    """
    Extrai RCL e DCL (valores) de um dataframe do RGF.
    Usa busca por padrões (robusto a variações pequenas de texto).
    """
    if df is None or df.empty:
        return None, None

    # Colunas esperadas (mas podem variar)
    if "conta" not in df.columns or "valor" not in df.columns:
        # tenta inferir colunas equivalentes
        # se não houver, aborta
        return None, None

    dfx = df.copy()
    dfx["conta"] = dfx["conta"].astype(str)
    dfx["valor"] = pd.to_numeric(dfx["valor"], errors="coerce")

    # RCL: qualquer linha que contenha "RECEITA CORRENTE LÍQUIDA"
    rcl = dfx[dfx["conta"].str.contains(r"RECEITA CORRENTE L[ÍI]QUIDA", case=False, regex=True, na=False)].copy()
    # DCL: qualquer linha que contenha "DÍVIDA CONSOLIDADA LÍQUIDA"
    dcl = dfx[dfx["conta"].str.contains(r"D[ÍI]VIDA CONSOLIDADA L[ÍI]QUIDA", case=False, regex=True, na=False)].copy()

    if rcl.empty or dcl.empty:
        return None, None

    # Preferência: RCL "mais limpa" se existir
    rcl_exact = rcl[rcl["conta"].str.fullmatch(r"RECEITA CORRENTE L[ÍI]QUIDA", case=False, na=False)]
    rcl_use = rcl_exact if not rcl_exact.empty else rcl

    # Preferência: DCL com (I - II), se existir
    dcl_i_ii = dcl[dcl["conta"].str.contains(r"\(I\s*-\s*II\)", regex=True, na=False)]
    dcl_use = dcl_i_ii if not dcl_i_ii.empty else dcl

    # Para evitar pegar subtotal errado: pega o MAIOR valor (normalmente linha agregada)
    rcl_val = pd.to_numeric(rcl_use["valor"], errors="coerce").max()
    dcl_val = pd.to_numeric(dcl_use["valor"], errors="coerce").max()

    if pd.isna(rcl_val) or pd.isna(dcl_val) or rcl_val == 0:
        return None, None

    return float(rcl_val), float(dcl_val)


def montar_base_periodo(session, entes_df, ano, periodo):
    """
    Para cada UF:
      baixa RGF do período
      extrai RCL e DCL
      calcula DCL/RCL (%)
    Retorna DF com: id_ente, sg_uf, dcl_rcl_pct
    """
    registros = []
    total = len(entes_df)

    print(f"📥 Baixando {ano} (Quad {periodo})...")

    for idx, row in entes_df.iterrows():
        id_ente = int(row["id_ente"])
        uf = str(row["sg_uf"])

        print(f"Processando estado {id_ente} ({idx+1}/{total})...")

        try:
            df_rgf = baixar_rgf_por_ente(session, id_ente, ano, periodo)
        except Exception as e:
            print(f"⚠️ Falha ao baixar UF={uf} id_ente={id_ente}: {e}")
            continue

        rcl, dcl = extrair_rcl_e_dcl(df_rgf)
        if rcl is None or dcl is None:
            # não achou nesse período
            continue

        registros.append({
            "id_ente": id_ente,
            "sg_uf": uf,
            "RCL": rcl,
            "DCL": dcl,
            "DCL_RCL_Pct": (dcl / rcl) * 100.0,
            "ano": ano,
            "periodo": periodo,
        })

    return pd.DataFrame(registros)


def main():
    print_header()

    session = requests.Session()
    session.headers.update({"User-Agent": "monitor-fiscal/2.1"})

    # Lista entes estaduais
    entes = listar_estados(session)
    if entes.empty:
        print("❌ Não encontrei entes estaduais.")
        return

    # Normaliza UF_REELEITOS
    reeleitos_uf = {str(x).upper().strip() for x in UF_REELEITOS}

    # Baixa 3 bases
    base_2018 = montar_base_periodo(session, entes, ANO_ANTIGO, PERIODO_ANTIGO)
    base_2022 = montar_base_periodo(session, entes, ANO_BASE, PERIODO_BASE)
    base_2025 = montar_base_periodo(session, entes, ANO_ATUAL, PERIODO_ATUAL)

    print("\n📌 Shapes:")
    print("2018:", base_2018.shape)
    print("2022:", base_2022.shape)
    print("2025:", base_2025.shape)

    if base_2025.empty or base_2022.empty:
        print("❌ Falha ao processar os dados de um dos períodos (2025 ou 2022 vazio).")
        print("   Dica: verifique se a API retornou itens e se as contas de RCL/DCL aparecem no 'conta'.")
        return

    # Indexa por id_ente (chave estável)
    b2018 = base_2018.set_index("id_ente") if not base_2018.empty else pd.DataFrame()
    b2022 = base_2022.set_index("id_ente")
    b2025 = base_2025.set_index("id_ente")

    # Junta meta UF -> id_ente
    uf_to_id = {r["sg_uf"]: int(r["id_ente"]) for _, r in entes.iterrows()}

    # Monta ranking
    print("\n🔄 Cruzando dados e aplicando regra de reeleição...")
    rows = []

    for uf, id_ente in uf_to_id.items():
        if id_ente not in b2025.index:
            continue

        dcl_atual = float(b2025.loc[id_ente, "DCL_RCL_Pct"])

        # Se reeleito: tenta usar 2018; se não tiver, fallback 2022
        if uf in reeleitos_uf:
            if not b2018.empty and id_ente in b2018.index:
                dcl_ini = float(b2018.loc[id_ente, "DCL_RCL_Pct"])
                inicio_label = f"{ANO_ANTIGO}Q{PERIODO_ANTIGO}"
            elif id_ente in b2022.index:
                dcl_ini = float(b2022.loc[id_ente, "DCL_RCL_Pct"])
                inicio_label = f"{ANO_BASE}Q{PERIODO_BASE}"
            else:
                continue
        else:
            # Não reeleito: usa 2022
            if id_ente in b2022.index:
                dcl_ini = float(b2022.loc[id_ente, "DCL_RCL_Pct"])
                inicio_label = f"{ANO_BASE}Q{PERIODO_BASE}"
            else:
                continue

        rows.append({
            "UF": uf,
            "DCL_RCL_Pct_Inicial": dcl_ini,
            "DCL_RCL_Pct_Atual": dcl_atual,
            "Delta_pp": dcl_atual - dcl_ini,
            "Inicio_Avaliacao": inicio_label,
            "Atual_Avaliacao": f"{ANO_ATUAL}Q{PERIODO_ATUAL}",
            "id_ente": id_ente,
        })

    df_rank = pd.DataFrame(rows)

    if df_rank.empty:
        print("❌ Erro ao cruzar os dados: ranking vazio.")
        print("   Causas comuns:")
        print("   - A extração não encontrou RCL/DCL em algum período (bases vazias).")
        print("   - UF_REELEITOS está preenchida com UFs que não existem na lista.")
        return

    df_rank = df_rank.sort_values("Delta_pp", ascending=True)

    # Salva CSV na pasta atual (onde você roda o CMD)
    out_csv = "dados_ranking_estados.csv"
    df_rank.to_csv(out_csv, index=False, encoding="utf-8")

    print("\n✅ SUCESSO! Arquivo gerado:", out_csv)
    print("\nTop 10 (maior redução de Dívida/RCL):")
    print(df_rank[["UF", "Inicio_Avaliacao", "DCL_RCL_Pct_Inicial", "DCL_RCL_Pct_Atual", "Delta_pp"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
