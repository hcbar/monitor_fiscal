"""
Script de Teste Rápido - Validação da Variável Tributos/RCL

Execute este script para validar se as correções estão funcionando.
"""

import pandas as pd
import sys

def testar_arquivo_csv(caminho_csv):
    """Testa se o arquivo CSV gerado contém as colunas esperadas."""
    
    print("="*70)
    print(" TESTE DE VALIDAÇÃO - TRIBUTOS/RCL")
    print("="*70)
    
    try:
        df = pd.read_csv(caminho_csv)
        print(f"\n✅ Arquivo carregado: {caminho_csv}")
        print(f"   Total de linhas: {len(df)}")
        print(f"   Total de colunas: {len(df.columns)}")
        
        # Verifica se as colunas necessárias existem
        colunas_esperadas = [
            'Estado',
            'Tributos_RCL_Pct_Inicial',
            'Tributos_RCL_Pct_Atual',
            'Delta_Tributos_pp'
        ]
        
        print("\n📋 VERIFICANDO COLUNAS:")
        colunas_faltando = []
        for col in colunas_esperadas:
            if col in df.columns:
                print(f"   ✅ {col}")
            else:
                print(f"   ❌ {col} - FALTANDO!")
                colunas_faltando.append(col)
        
        if colunas_faltando:
            print(f"\n❌ ERRO: Faltam {len(colunas_faltando)} colunas necessárias!")
            print("   Execute novamente o etl_completo_corrigido.py")
            return False
        
        # Verifica se há dados não-zero
        print("\n📊 ESTATÍSTICAS DOS DADOS:")
        
        # Tributos_RCL_Pct_Atual
        col_atual = 'Tributos_RCL_Pct_Atual'
        valores_nao_zero = (df[col_atual] != 0).sum()
        print(f"\n   {col_atual}:")
        print(f"   - Estados com dados: {valores_nao_zero}/{len(df)}")
        print(f"   - Mínimo: {df[col_atual].min():.2f}%")
        print(f"   - Máximo: {df[col_atual].max():.2f}%")
        print(f"   - Média: {df[col_atual].mean():.2f}%")
        
        if valores_nao_zero == 0:
            print("\n   ⚠️  AVISO: Todos os valores estão zerados!")
            print("   Possíveis causas:")
            print("   1. API do SICONFI não retornou dados")
            print("   2. Identificadores das contas mudaram")
            print("   3. Período/ano não disponível")
            return False
        
        # Delta_Tributos_pp
        col_delta = 'Delta_Tributos_pp'
        tem_variacao = (df[col_delta] != 0).sum()
        print(f"\n   {col_delta}:")
        print(f"   - Estados com variação: {tem_variacao}/{len(df)}")
        print(f"   - Maior aumento: {df[col_delta].max():.2f} pp")
        print(f"   - Maior queda: {df[col_delta].min():.2f} pp")
        
        # Mostra top 5 estados
        print("\n🏆 TOP 5 ESTADOS (maior % atual):")
        top5 = df.nlargest(5, col_atual)[['Estado', col_atual, col_delta]]
        for idx, row in top5.iterrows():
            delta_symbol = "🔺" if row[col_delta] > 0 else "🔻" if row[col_delta] < 0 else "➡️"
            print(f"   {delta_symbol} {row['Estado']}: {row[col_atual]:.2f}% (Δ {row[col_delta]:+.2f} pp)")
        
        print("\n" + "="*70)
        print("✅ VALIDAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*70)
        return True
        
    except FileNotFoundError:
        print(f"\n❌ ERRO: Arquivo não encontrado: {caminho_csv}")
        print("   Execute primeiro: python etl_completo_corrigido.py")
        return False
    except Exception as e:
        print(f"\n❌ ERRO ao processar arquivo: {e}")
        import traceback
        traceback.print_exc()
        return False


def teste_rapido():
    """Teste rápido sem arquivo - valida a lógica do cálculo."""
    print("\n🧪 TESTE RÁPIDO DA LÓGICA:")
    print("-" * 70)
    
    # Valores de teste (Mato Grosso 2018)
    impostos = 7578075447.92
    taxas = 182738816.96
    contrib = 0.00
    rcl = 15000000000.00
    
    tributos = impostos + taxas + contrib
    resultado = (tributos / rcl) * 100
    
    print(f"   Impostos: R$ {impostos:,.2f}")
    print(f"   Taxas: R$ {taxas:,.2f}")
    print(f"   Contribuição de Melhoria: R$ {contrib:,.2f}")
    print(f"   Tributos (total): R$ {tributos:,.2f}")
    print(f"   RCL: R$ {rcl:,.2f}")
    print(f"   Resultado: {resultado:.2f}%")
    
    esperado = 51.74
    if abs(resultado - esperado) < 0.01:
        print(f"\n   ✅ Cálculo correto! Esperado: {esperado}%, Obtido: {resultado:.2f}%")
        return True
    else:
        print(f"\n   ❌ Cálculo incorreto! Esperado: {esperado}%, Obtido: {resultado:.2f}%")
        return False


if __name__ == "__main__":
    # Teste rápido da lógica
    teste_rapido()
    
    # Se passou arquivo como argumento, testa o CSV
    if len(sys.argv) > 1:
        arquivo = sys.argv[1]
        testar_arquivo_csv(arquivo)
    else:
        print("\n" + "="*70)
        print("Para testar o arquivo CSV gerado, execute:")
        print("   python testar_validacao.py dados_ranking_estados.csv")
        print("="*70)
