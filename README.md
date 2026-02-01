# Monitor Fiscal dos Estados Brasileiros

Dashboard interativo para análise da gestão fiscal dos governadores estaduais, baseado em dados do SICONFI (Sistema de Informações Contábeis e Fiscais do Setor Público Brasileiro).

## 📊 Indicadores Disponíveis

- **Poupança Fiscal (RP/RCL)**: Resultado Primário sobre Receita Corrente Líquida
- **Endividamento (DCL/RCL)**: Dívida Consolidada Líquida sobre RCL
- **Gastos com Pessoal (DTP/RCL)**: Despesa Total com Pessoal sobre RCL

## 🚀 Instalação

```bash
# Clonar repositório
git clone https://github.com/hcbar/monitor_fiscal.git
cd monitor_fiscal

# Instalar dependências
pip install -r requirements.txt
```

## 📁 Estrutura do Projeto

```
monitor_fiscal/
├── etl.py                    # ETL para processar CSVs locais
├── app.py                    # Dashboard Streamlit
├── governadores.csv          # Dados dos governadores
├── requirements.txt          # Dependências Python
├── dados_ranking_estados.csv # Output do ETL (gerado)
└── dados_brutos/
    └── 2024/
        ├── resultado_primario/
        │   └── 2024_*bim_resultado_primario_acima_da_linha.csv
        ├── receita_corrente_liquida/
        │   └── 2024_*bim_receita_corrente_liquida.csv
        └── meta_primario/
            └── 2024_*bim_meta_primario.csv
```

## 📥 Baixando os Dados do SICONFI

Os dados são baixados manualmente do [SICONFI](https://siconfi.tesouro.gov.br/siconfi/index.jsf):

1. Acesse SICONFI → Consultas → Consultar Relatório
2. Selecione:
   - **Tipo**: RREO
   - **Escopo**: Estados/DF
   - **Exercício**: Ano desejado
   - **Período**: 6º Bimestre (dados anuais consolidados)
3. Baixe os anexos:
   - **Resultado Primário - Acima da Linha** (Anexo 06)
   - **Previsão Atualizada / RCL** (Anexo 03)
   - **Meta Fiscal para o Resultado Primário** (Anexo 06)

4. Salve os CSVs na estrutura de pastas:
   ```
   dados_brutos/{ano}/resultado_primario/{ano}_{bim}bim_resultado_primario_acima_da_linha.csv
   dados_brutos/{ano}/receita_corrente_liquida/{ano}_{bim}bim_receita_corrente_liquida.csv
   dados_brutos/{ano}/meta_primario/{ano}_{bim}bim_meta_primario.csv
   ```

## 🔄 Executando o ETL

```bash
# Processar CSVs e gerar dados_ranking_estados.csv
python etl.py

# Com debug (mostra valores por estado/ano)
python etl.py --debug
```

## 🖥️ Executando o Dashboard

```bash
streamlit run app.py
```

Acesse http://localhost:8501 no navegador.

## 📈 Sobre os Indicadores

### Poupança Fiscal (Resultado Primário / RCL)

O **Resultado Primário** mostra se o estado arrecadou mais do que gastou (excluindo juros da dívida):

- **Positivo** 🟢: Superávit primário (estado poupou, pode pagar dívidas)
- **Negativo** 🔴: Déficit primário (estado gastou mais do que arrecadou)

### Nota sobre RPPS

A partir de 2024, o SICONFI passou a separar os resultados:
- **COM RPPS**: Inclui o Regime Próprio de Previdência Social
- **SEM RPPS**: Exclui o RPPS

Este ETL utiliza a versão **COM RPPS** para manter consistência histórica.

## 📋 Fonte dos Dados

- [SICONFI - Tesouro Nacional](https://siconfi.tesouro.gov.br)
- RREO - Relatório Resumido de Execução Orçamentária
- RGF - Relatório de Gestão Fiscal

## 📄 Licença

MIT License
