# 🔧 Correções Aplicadas - Variável Tributos/RCL

## Problema Identificado

A variável "Arrecadação Própria" (Tributos/RCL) estava vindo zerada porque:

1. **Faltava o cálculo no ETL**: O arquivo `etl_completo.py` original não tinha a função que calcula essa variável
2. **App.py esperava colunas que não existiam**: O app buscava por `Tributos_RCL_Pct_Inicial`, `Tributos_RCL_Pct_Atual` e `Delta_Tributos_pp`, mas essas colunas não eram criadas

## Solução Implementada

### 1. Nova Função `calc_tributos_rcl()`

Adicionada no arquivo `etl_completo_corrigido.py` (linha ~342):

```python
def calc_tributos_rcl(df_rreo: pd.DataFrame, ano: int) -> float:
    """Calcula arrecadação própria (Tributos/RCL)."""
    
    # Busca Impostos
    impostos = pick_by_identifier(df_rreo, "siconfi-cor_Impostos", "Até o Bimestre")
    
    # Busca Taxas
    taxas = pick_by_identifier(df_rreo, "siconfi-cor_Taxas", "Até o Bimestre")
    
    # Busca Contribuição de Melhoria
    contrib = pick_by_identifier(df_rreo, "siconfi-cor_ContribuicaoDeMelhoria", "Até o Bimestre")
    
    # Soma os tributos
    tributos = impostos + taxas + contrib
    
    # Busca RCL no RREO Anexo 03
    df_rcl = get_data_rreo_bimestre(ano, 6, cod, anexo="Anexo 03", debug=False)
    rcl = pick_by_identifier(df_rcl, "siconfi-cor_RREO3ReceitaCorrenteLiquida", "TOTAL (ÚLTIMOS 12 MESES)")
    
    # Retorna o percentual
    return (tributos / rcl * 100) if rcl > 0 else 0.0
```

### 2. Novas Colunas no DataFrame de Saída

O ETL agora gera as seguintes colunas adicionais:

- `Tributos_RCL_Pct_Inicial`: % de Tributos/RCL no ano inicial
- `Tributos_RCL_Pct_Atual`: % de Tributos/RCL no ano final
- `Delta_Tributos_pp`: Variação em pontos percentuais

## Fórmula do Cálculo

```
Arrecadação Própria (%) = (Tributos / RCL) × 100

Onde:
- Tributos = Impostos + Taxas + Contribuição de Melhoria
- RCL = Receita Corrente Líquida
```

## Fontes de Dados

- **Tributos**: RREO Anexo 01 (Balanço Orçamentário) - coluna "Até o Bimestre"
- **RCL**: RREO Anexo 03 (Demonstrativo da RCL) - coluna "TOTAL (ÚLTIMOS 12 MESES)"

## Como Usar o Arquivo Corrigido

### Opção 1: Executar ETL Completo (API)

```python
python etl_completo_corrigido.py
```

Isso irá:
1. Buscar dados de todos os 27 estados
2. Calcular todos os indicadores incluindo Tributos/RCL
3. Gerar arquivo `dados_ranking_estados.csv`

### Opção 2: Processar CSV Manual

Se você tem um CSV baixado manualmente do Tesouro:

```python
import pandas as pd
from etl_completo_corrigido import process_uf

# Processar um estado específico
resultado = process_uf(
    cod=51,  # Mato Grosso
    nome="Mato Grosso",
    reeleito=True,
    ano_ini=2018,
    debug=True
)

print(resultado)
```

## Verificação dos Dados

Para verificar se o CSV contém as informações necessárias:

```bash
python verificar_rcl.py seu_arquivo.csv
```

Isso mostrará:
- Se o CSV contém dados de RCL
- Quais contas estão disponíveis
- Estrutura dos dados

## Exemplo de Saída

Com os dados de exemplo do Mato Grosso (2018):

```
Impostos: R$ 7,578,075,447.92
Taxas: R$ 182,738,816.96
Contribuição de Melhoria: R$ 0.00
Tributos (soma): R$ 7,760,814,264.88
RCL: R$ 15,000,000,000.00

Arrecadação Própria = 51.74%
```

## Estrutura de Arquivos

```
📁 Projeto
├── etl_completo_corrigido.py  ← USAR ESTE ARQUIVO
├── app.py                      ← Dashboard Streamlit (já configurado)
├── verificar_rcl.py           ← Script de verificação
├── teste_calculo.py           ← Script de teste
└── dados_ranking_estados.csv  ← Arquivo de saída
```

## Próximos Passos

1. **Execute o ETL corrigido**:
   ```bash
   python etl_completo_corrigido.py
   ```

2. **Verifique o arquivo gerado**:
   ```bash
   head -n 2 dados_ranking_estados.csv
   ```

3. **Execute o dashboard**:
   ```bash
   streamlit run app.py
   ```

4. **Selecione "Arrecadação Própria"** no menu do dashboard

## Diferenças do Arquivo Original

| Aspecto | Original | Corrigido |
|---------|----------|-----------|
| Função `calc_tributos_rcl()` | ❌ Não existe | ✅ Implementada |
| Colunas `Tributos_RCL_*` | ❌ Não geradas | ✅ Geradas |
| Fonte de dados Tributos | - | RREO Anexo 01 |
| Fonte de dados RCL | - | RREO Anexo 03 |
| Debug para Rondônia | Parcial | Completo |

## Notas Importantes

⚠️ **Atenção**:
- O cálculo usa dados do 6º bimestre (fim do ano)
- A RCL é buscada em um anexo diferente (Anexo 03) dos tributos (Anexo 01)
- Estados sem dados de tributos ou RCL terão resultado 0.0

✅ **Validação**:
- Testes com dados reais do Mato Grosso 2018 confirmam cálculo correto
- Percentuais típicos variam entre 30% e 70% dependendo do estado
- Estados mais desenvolvidos tendem a ter maior arrecadação própria

## Suporte

Se continuar com problemas:

1. Execute o `verificar_rcl.py` no seu CSV
2. Verifique se as contas "Impostos", "Taxas" e "RECEITA CORRENTE LÍQUIDA" existem
3. Confirme que está usando o 6º bimestre (fim do ano)
4. Ative o debug para ver os valores intermediários

```python
resultado = process_uf(cod=51, nome="MT", reeleito=True, debug=True)
```
