import pandas as pd
import requests
import urllib3
import numpy as np
import time

# Desativa avisos de SSL inseguro (necessário para o Siconfi)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
CODIGOS_UF = [
    11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    31, 32, 33, 35, 41, 42, 43, 50, 51, 52, 53
]

# Estados onde o governador foi reeleito (Base 2018). Os demais são novos mandatos (Base 2022).
UF_REELEITOS = [12, 27, 13, 53, 32, 52, 21, 51, 31, 15, 25, 41, 33, 24, 43, 11, 14, 17]

def get_data(ano, periodo, codigo_ibge, relatorio, tentativas=3):
    """
    Busca dados na API do Siconfi com mecanismo de retry para falhas de conexão.
    """
    url = f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/{relatorio.lower()}"
    params = {
        'an_exercicio': ano, 
        'id_ente': codigo_ibge, 
        'co_poder': 'E', 
        'co_esfera': 'E', 
        'co_tipo_demonstrativo': relatorio,
        'in_periodicidade': 'Q' if relatorio == 'RGF' else 'B',
        'nr_periodo': periodo if relatorio == 'RGF' else periodo * 2
    }
    
    for i in range(tentativas):
        try:
            r = requests.get(url, params=params, verify=False, timeout=15)
            if r.status_code == 200:
                data = r.json()['items']
                return pd.DataFrame(data)
        except requests.exceptions.RequestException:
            time.sleep(1) # Espera 1 segundo antes de tentar novamente
            continue
            
    return pd.DataFrame()

def coletar_uf(cod):
    # --- LÓGICA DE DATA (FALLBACK) ---
    # Tenta pegar o fechamento de 2024. Se não existir, pega 2023.
    ano_atual = 2024
    rgf_fim = get_data(ano_atual, 3, cod, 'RGF')
    
    if rgf_fim.empty:
        ano_atual = 2023
        rgf_fim = get_data(ano_atual, 3, cod, 'RGF')
    
    # Define ano inicial com base na reeleição
    ano_ini = 2018 if cod in UF_REELEITOS else 2022
    rgf_ini = get_data(ano_ini, 3, cod, 'RGF')

    # Busca RREO (Relatório Resumido de Execução Orçamentária) para o mesmo ano definido acima
    rreo_fim = get_data(ano_atual, 3, cod, 'RREO')
    rreo_ini = get_data(ano_ini, 3, cod, 'RREO')

    # Dicionário base
    v = {
        'Nome': f"UF-{cod}", 
        'RCL_Ini': None, 
        'RCL_Fim': None,
        'Ano_Ref_Atual': ano_atual, # Importante para auditoria
        'Ano_Ref_Inicial': ano_ini
    }

    # --- EXTRAÇÃO DE NOME E RCL ---
    if not rgf_fim.empty:
        raw_nome = rgf_fim['instituicao'].iloc[0]
        # Limpeza aprimorada de nomes
        v['Nome'] = (raw_nome
                     .replace('Governo do Estado do ', '')
                     .replace('Governo do Estado da ', '') # Correção adicionada
                     .replace('Governo do Estado de ', '')
                     .replace('Governo do Estado ', '')
                     .replace('Governo do ', '')
                     .strip())
        
        # Pega RCL Final
        row = rgf_fim[rgf_fim['conta'].str.contains("RECEITA CORRENTE LÍQUIDA - RCL", case=False, na=False)]
        if not row.empty: 
            v['RCL_Fim'] = pd.to_numeric(row['valor'].iloc[0], errors='coerce')
            
    if not rgf_ini.empty:
        # Pega RCL Inicial
        row = rgf_ini[rgf_ini['conta'].str.contains("RECEITA CORRENTE LÍQUIDA - RCL", case=False, na=False)]
        if not row.empty: 
            v['RCL_Ini'] = pd.to_numeric(row['valor'].iloc[0], errors='coerce')

    # --- FUNÇÃO AUXILIAR DE VALORES ---
    def get_val(df, termo):
        if df.empty: return None
        # Filtra termo e exclui "META" para evitar pegar previsão orçamentária
        mask = df['conta'].str.contains(termo, case=False, na=False) & ~df['conta'].str.contains("META", case=False)
        temp = df[mask].copy()
        if temp.empty: return None
        
        # Pega o maior valor absoluto para evitar linhas de cabeçalho zeradas
        temp['abs'] = pd.to_numeric(temp['valor'], errors='coerce').abs()
        idx_max = temp['abs'].idxmax()
        return pd.to_numeric(temp.loc[idx_max, 'valor'], errors='coerce')

    # Extração dos Indicadores
    dcl_ini = get_val(rgf_ini, "DÍVIDA CONSOLIDADA LÍQUIDA")
    dcl_fim = get_val(rgf_fim, "DÍVIDA CONSOLIDADA LÍQUIDA")
    
    dtp_ini = get_val(rgf_ini, "DESPESA TOTAL COM PESSOAL - DTP")
    dtp_fim = get_val(rgf_fim, "DESPESA TOTAL COM PESSOAL - DTP")
    
    prim_ini = get_val(rreo_ini, "RESULTADO PRIMÁRIO")
    prim_fim = get_val(rreo_fim, "RESULTADO PRIMÁRIO")

    # Função segura para porcentagem
    def pct(val, rcl): 
        if val is None or rcl is None or rcl == 0:
            return None
        return (val / rcl) * 100
    
    # --- CÁLCULO DAS VARIAÇÕES ---
    res = {
        'Estado': v['Nome'],
        'Ano_Base': v['Ano_Ref_Inicial'],
        'Ano_Atual': v['Ano_Ref_Atual'],
        
        # Dívida (DCL)
        'DCL_RCL_Pct_Inicial': pct(dcl_ini, v['RCL_Ini']),
        'DCL_RCL_Pct_Atual': pct(dcl_fim, v['RCL_Fim']),
        
        # Pessoal (DTP)
        'DTP_RCL_Pct_Inicial': pct(dtp_ini, v['RCL_Ini']),
        'DTP_RCL_Pct_Atual': pct(dtp_fim, v['RCL_Fim']),
        
        # Primário
        'Primario_RCL_Pct_Inicial': pct(prim_ini, v['RCL_Ini']),
        'Primario_RCL_Pct_Atual': pct(prim_fim, v['RCL_Fim']),
    }

    # Calcula Deltas (Diferença em pontos percentuais)
    res['Delta_DCL_pp'] = res['DCL_RCL_Pct_Atual'] - res['DCL_RCL_Pct_Inicial'] if (res['DCL_RCL_Pct_Atual'] is not None and res['DCL_RCL_Pct_Inicial'] is not None) else None
    res['Delta_DTP_pp'] = res['DTP_RCL_Pct_Atual'] - res['DTP_RCL_Pct_Inicial'] if (res['DTP_RCL_Pct_Atual'] is not None and res['DTP_RCL_Pct_Inicial'] is not None) else None
    res['Delta_Primario_pp'] = res['Primario_RCL_Pct_Atual'] - res['Primario_RCL_Pct_Inicial'] if (res['Primario_RCL_Pct_Atual'] is not None and res['Primario_RCL_Pct_Inicial'] is not None) else None

    return res

def executar():
    print("=== ETL SICONFI: Ranking Fiscal dos Estados ===")
    print(f"Iniciando coleta para {len(CODIGOS_UF)} estados...")
    
    lista = []
    sucesso = 0
    erros = 0
    
    start_time = time.time()

    for i, cod in enumerate(CODIGOS_UF):
        print(f"\r[{i+1}/{len(CODIGOS_UF)}] Processando UF {cod}...", end="")
        try:
            dados = coletar_uf(cod)
            lista.append(dados)
            sucesso += 1
        except Exception as e:
            print(f"\nErro na UF {cod}: {str(e)}")
            erros += 1
    
    # Cria DataFrame e remove linhas vazias (onde não foi possível calcular nada)
    df = pd.DataFrame(lista)
    
    # Reordenar colunas para leitura mais fácil
    cols_order = [
        'Estado', 'Ano_Base', 'Ano_Atual',
        'DCL_RCL_Pct_Inicial', 'DCL_RCL_Pct_Atual', 'Delta_DCL_pp',
        'DTP_RCL_Pct_Inicial', 'DTP_RCL_Pct_Atual', 'Delta_DTP_pp',
        'Primario_RCL_Pct_Inicial', 'Primario_RCL_Pct_Atual', 'Delta_Primario_pp'
    ]
    # Garante que só reordena se as colunas existirem
    cols_final = [c for c in cols_order if c in df.columns]
    df = df[cols_final]

    # Salva arquivo
    filename = 'dados_ranking_estados_v2.csv'
    df.to_csv(filename, index=False, float_format='%.2f')
    
    tempo_total = time.time() - start_time
    print(f"\n\n✅ Concluído em {tempo_total:.1f} segundos!")
    print(f"📥 Arquivo salvo: {filename}")
    print(f"📊 Sucessos: {sucesso} | Falhas: {erros}")

if __name__ == "__main__":
    executar()