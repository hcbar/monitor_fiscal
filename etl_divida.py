import pandas as pd
import requests
import urllib3

# Desabilitar avisos de certificado SSL (necessário para dados.gov.br)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
CODIGOS_UF = [
    11, 12, 13, 14, 15, 16, 17, # Norte
    21, 22, 23, 24, 25, 26, 27, 28, 29, # Nordeste
    31, 32, 33, 35, # Sudeste
    41, 42, 43, # Sul
    50, 51, 52, 53 # Centro-Oeste
]

# Governadores reeleitos (para usar base 2018)
UF_REELEITOS = [12, 27, 13, 53, 32, 52, 21, 51, 31, 15, 25, 41, 33, 24, 43, 11, 14, 17]

def get_rgf_data_estado(ano, periodo, codigo_ibge):
    url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json"
    }
    
    # AQUI ESTÁ A CORREÇÃO DESCOBERTA NO DIAGNÓSTICO:
    params = {
        'an_exercicio': ano,
        'nr_periodo': periodo,
        'in_periodicidade': 'Q',
        'co_poder': 'E',
        'co_esfera': 'E',
        'id_ente': codigo_ibge,
        'co_tipo_demonstrativo': 'RGF' # <--- O parâmetro que faltava!
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, verify=False, timeout=15)
        if response.status_code == 200:
            items = response.json()['items']
            return pd.DataFrame(items)
    except:
        pass
    return pd.DataFrame()

def coletar_todos_estados(ano, periodo):
    print(f"\n📥 Baixando {ano} (Quad {periodo})...")
    lista_dfs = []
    
    for i, cod in enumerate(CODIGOS_UF):
        # Feedback visual de progresso
        if i % 5 == 0: print(f"\r   Progresso: {int((i/len(CODIGOS_UF))*100)}%...", end="")
        
        df = get_rgf_data_estado(ano, periodo, cod)
        if not df.empty:
            df['id_ente_custom'] = cod 
            lista_dfs.append(df)
            
    print("\r   Progresso: 100% (Concluído).")
    
    if lista_dfs:
        return pd.concat(lista_dfs, ignore_index=True)
    return pd.DataFrame()

def processar_metricas(df):
    if df.empty: return pd.DataFrame()

    # Filtros de Conta (Case Insensitive e seguros)
    df_rcl = df[df['conta'].str.contains("RECEITA CORRENTE LÍQUIDA", case=False, na=False)].copy()
    
    # Regex para Dívida
    df_dcl = df[df['conta'].str.contains("DÍVIDA CONSOLIDADA LÍQUIDA", case=False, na=False)].copy()
    # Filtra linha específica do saldo ou título
    df_dcl = df_dcl[df_dcl['conta'].str.contains(r"\(I - II\)", regex=True, na=False) | 
                    (df_dcl['conta'].str.strip().str.upper() == 'DÍVIDA CONSOLIDADA LÍQUIDA')]

    for d in [df_rcl, df_dcl]:
        d['valor'] = pd.to_numeric(d['valor'], errors='coerce')
        
    # Agrupa pelo ID numérico
    rcl = df_rcl.groupby('id_ente_custom')['valor'].max().reset_index().rename(columns={'valor': 'RCL'})
    dcl = df_dcl.groupby('id_ente_custom').agg({'valor': 'max', 'instituicao': 'first'}).reset_index().rename(columns={'valor': 'DCL'})
    
    df_final = pd.merge(dcl, rcl, on='id_ente_custom')
    df_final['DCL_RCL_Pct'] = (df_final['DCL'] / df_final['RCL']) * 100
    
    return df_final

# --- EXECUÇÃO ---
print("=== MONITOR FISCAL V5 (Com Correção de Parâmetro) ===")

# 1. Baixar Bases Históricas
df_2018 = coletar_todos_estados(2018, 3) 
proc_2018 = processar_metricas(df_2018)
print(f" -> 2018: {len(proc_2018)} estados carregados.")

df_2022 = coletar_todos_estados(2022, 3)
proc_2022 = processar_metricas(df_2022)
print(f" -> 2022: {len(proc_2022)} estados carregados.")

# 2. Baixar Dados Recentes (Trocamos para Quad 2 de 2025 para garantir dados)
# (Alguns estados demoram para publicar o Quad 3, então Quad 2 é mais seguro agora)
df_2025 = coletar_todos_estados(2025, 2)
proc_2025 = processar_metricas(df_2025)
print(f" -> 2025: {len(proc_2025)} estados carregados.")

# 3. Cruzamento Final
if not proc_2025.empty:
    lista_final = []
    
    print("\nCruzando dados...")
    for idx, row in proc_2025.iterrows():
        cod_ibge = row['id_ente_custom']
        nome_sujo = row['instituicao']
        dcl_atual = row['DCL_RCL_Pct']
        
        # Limpar nome
        nome_limpo = nome_sujo.replace('Governo do Estado do ', '').replace('Governo do Estado de ', '').replace('Governo do Estado ', '').replace('Governo do ', '')
        
        dcl_inicial = None
        ano_inicio = 0
        
        # Regra Reeleição
        if cod_ibge in UF_REELEITOS:
            dados_antigos = proc_2018[proc_2018['id_ente_custom'] == cod_ibge]
            if not dados_antigos.empty:
                dcl_inicial = dados_antigos['DCL_RCL_Pct'].values[0]
                ano_inicio = 2019
        
        # Regra Novos Mandatos
        if dcl_inicial is None:
            dados_antigos = proc_2022[proc_2022['id_ente_custom'] == cod_ibge]
            if not dados_antigos.empty:
                dcl_inicial = dados_antigos['DCL_RCL_Pct'].values[0]
                ano_inicio = 2023
                
        if dcl_inicial is not None:
            lista_final.append({
                'Estado': nome_limpo,
                'Inicio_Avaliacao': f"Desde {ano_inicio}",
                'DCL_RCL_Pct_Inicial': dcl_inicial,
                'DCL_RCL_Pct_Atual': dcl_atual,
                'Delta_pp': dcl_atual - dcl_inicial
            })

    df_ranking = pd.DataFrame(lista_final)

    if not df_ranking.empty:
        df_ranking = df_ranking.sort_values(by='Delta_pp', ascending=True)
        df_ranking.to_csv('dados_ranking_estados.csv', index=False)
        print(f"\n✅ SUCESSO! Ranking gerado com {len(df_ranking)} estados.")
        print("Agora rode: streamlit run app.py")
    else:
        print("\n⚠️ Dados baixados, mas falha no cruzamento.")
else:
    print("\n❌ Sem dados recentes. Verifique conexão.")