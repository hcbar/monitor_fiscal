"""
Baixa dados PNAD usando pacotes R especializados

Requisitos:
    - R instalado
    - Pacotes R: PNADcIBGE, dplyr, tidyr
    
Instalar:
    R -e "install.packages(c('PNADcIBGE', 'dplyr', 'tidyr'))"
"""

import subprocess
import json
import os
import math
from datetime import datetime
from typing import Dict, Optional


def instalar_pacotes_r():
    """Instala pacotes R necessários."""
    print("📦 Instalando pacotes R...")
    script = """
    if (!require("PNADcIBGE")) install.packages("PNADcIBGE", repos="https://cloud.r-project.org")
    if (!require("dplyr")) install.packages("dplyr", repos="https://cloud.r-project.org")
    if (!require("tidyr")) install.packages("tidyr", repos="https://cloud.r-project.org")
    """
    try:
        subprocess.run(["Rscript", "-e", script], check=True, capture_output=True)
        print("✅ Pacotes R instalados")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao instalar pacotes R: {e}")
        return False


def baixar_pnad_via_r(anos: list = None, salvar_em: str = "pnad_dados.json", auto_anos: bool = True) -> bool:
    """
    Baixa dados PNAD usando R e salva em JSON.
    
    Args:
        anos: Lista de anos para baixar. Se None e auto_anos=True, baixa de 2018 até ano atual.
        salvar_em: Caminho do arquivo JSON para salvar
        auto_anos: Se True e anos=None, detecta automaticamente intervalo de anos
        
    Returns:
        True se sucesso, False caso contrário
    """
    
    # Detecta anos automaticamente se não especificado
    if anos is None and auto_anos:
        ano_atual = datetime.now().year
        anos = list(range(2018, ano_atual + 1))
        print(f"📅 Anos detectados automaticamente: 2018-{ano_atual}")
        print(f"   Nota: Ano {ano_atual} pode ter dados parciais (PNAD tem ~3 meses defasagem)")
    
    if not anos:
        print("❌ Nenhum ano especificado")
        return False
    
    # Script R que baixa os dados
    script_r = f"""
library(PNADcIBGE)
library(dplyr)
library(tidyr)
library(jsonlite)

# Anos para baixar
anos <- c({','.join(map(str, anos))})

# Inicializa lista de resultados
resultado <- list()

for (ano in anos) {{
    cat(sprintf("\\n📊 Baixando PNAD %d...\\n", ano))
    
    tryCatch({{
        # Baixa dados anuais da PNAD
        # Variáveis:
        # - V403312: Rendimento mensal habitual do trabalho principal
        # - VD4020: Rendimento mensal habitual de todos os trabalhos
        
        pnad <- get_pnadc(year = ano, interview = 1, vars = c("V403312", "VD4020", "UF"))
        
        # Calcula massa salarial por UF
        # Massa = soma(rendimento × peso da pessoa)
        
        dados_uf <- pnad %>%
            filter(!is.na(VD4020) & VD4020 > 0) %>%
            mutate(
                massa_individual = VD4020 * V1028  # rendimento × peso
            ) %>%
            group_by(UF) %>%
            summarise(
                massa_total_mensal = sum(massa_individual, na.rm = TRUE),
                pop_ocupada = n(),
                rendimento_medio = mean(VD4020, na.rm = TRUE)
            ) %>%
            mutate(
                ano = ano,
                massa_total_anual = massa_total_mensal * 12  # Anualiza
            )
        
        # Adiciona ao resultado
        resultado[[as.character(ano)]] <- dados_uf
        
        cat(sprintf("✅ Ano %d concluído\\n", ano))
        
    }}, error = function(e) {{
        cat(sprintf("❌ Erro no ano %d: %s\\n", ano, e$message))
    }})
    
    # Pausa entre requests (respeito à API)
    Sys.sleep(2)
}}

# Converte para formato JSON
cat("\\n💾 Salvando dados...\\n")

# Combina todos os anos
dados_completos <- bind_rows(resultado)

# Converte códigos UF para numéricos
dados_completos <- dados_completos %>%
    mutate(cod_uf = as.integer(UF))

# Salva como JSON
write_json(dados_completos, "{salvar_em}", pretty = TRUE)

cat(sprintf("\\n✅ Dados salvos em: {salvar_em}\\n"))
    """
    
    # Salva script temporário
    script_path = "/tmp/baixar_pnad.R"
    with open(script_path, "w") as f:
        f.write(script_r)
    
    print(f"🔄 Executando script R...")
    print(f"   Baixando anos: {anos}")
    print(f"   Isso pode levar alguns minutos...")
    
    try:
        # Executa R
        result = subprocess.run(
            ["Rscript", script_path],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutos
        )
        
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"❌ Erro no R:")
            print(result.stderr)
            return False
        
        # Verifica se arquivo foi criado
        if os.path.exists(salvar_em):
            print(f"✅ Dados PNAD salvos com sucesso!")
            return True
        else:
            print(f"❌ Arquivo {salvar_em} não foi criado")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout: Download demorou muito (>10min)")
        return False
    except Exception as e:
        print(f"❌ Erro ao executar R: {e}")
        return False
    finally:
        # Remove script temporário
        if os.path.exists(script_path):
            os.remove(script_path)


def carregar_dados_pnad(arquivo_json: str = "pnad_dados.json") -> Dict[int, Dict[int, float]]:
    """
    Carrega dados PNAD do JSON e converte para dicionário Python.
    
    Returns:
        {cod_uf: {ano: massa_salarial_anual}}
    """
    
    if not os.path.exists(arquivo_json):
        print(f"❌ Arquivo {arquivo_json} não encontrado")
        return {}
    
    try:
        with open(arquivo_json, 'r') as f:
            dados = json.load(f)
        
        # Reorganiza para formato {uf: {ano: massa}}
        resultado = {}
        
        for registro in dados:
            cod_uf = int(registro['cod_uf'])
            ano = int(registro['ano'])
            massa = float(registro['massa_total_anual'])
            
            if cod_uf not in resultado:
                resultado[cod_uf] = {}
            
            resultado[cod_uf][ano] = massa
        
        print(f"✅ Dados carregados: {len(resultado)} UFs, {len(dados)} registros")
        return resultado
        
    except Exception as e:
        print(f"❌ Erro ao carregar JSON: {e}")
        return {}


def calcular_delta_ln_com_pnad_real(
    cod_uf: int,
    tributos_ini: float,
    tributos_fim: float,
    ano_ini: int,
    ano_fim: int,
    dados_pnad: Optional[Dict] = None,
    arquivo_pnad: str = "pnad_dados.json",
    debug: bool = False
) -> float:
    """
    Calcula Δln(Arrecadação) - Δln(Massa Salarial) usando dados PNAD reais.
    
    Se dados_pnad não fornecido, tenta carregar de arquivo.
    Se arquivo não existe, baixa automaticamente via R.
    """
    
    # Carrega dados se necessário
    if dados_pnad is None:
        if not os.path.exists(arquivo_pnad):
            print(f"📥 Dados PNAD não encontrados, baixando...")
            anos = list(range(ano_ini, ano_fim + 1))
            if not baixar_pnad_via_r(anos, arquivo_pnad):
                print("❌ Falha ao baixar PNAD")
                return 0.0
        
        dados_pnad = carregar_dados_pnad(arquivo_pnad)
    
    # Busca dados da UF
    if cod_uf not in dados_pnad:
        if debug:
            print(f"⚠️  UF {cod_uf} não encontrada nos dados PNAD")
        return 0.0
    
    dados_uf = dados_pnad[cod_uf]
    
    if ano_ini not in dados_uf or ano_fim not in dados_uf:
        if debug:
            print(f"⚠️  Anos {ano_ini} ou {ano_fim} não disponíveis para UF {cod_uf}")
        return 0.0
    
    massa_ini = dados_uf[ano_ini]
    massa_fim = dados_uf[ano_fim]
    
    # Valida
    if any(x <= 0 for x in [tributos_ini, tributos_fim, massa_ini, massa_fim]):
        if debug:
            print(f"❌ Valores inválidos")
        return 0.0
    
    try:
        # Cálculo logarítmico
        delta_ln_trib = math.log(tributos_fim) - math.log(tributos_ini)
        delta_ln_massa = math.log(massa_fim) - math.log(massa_ini)
        diferenca = delta_ln_trib - delta_ln_massa
        
        if debug:
            cresc_trib = (math.exp(delta_ln_trib) - 1) * 100
            cresc_massa = (math.exp(delta_ln_massa) - 1) * 100
            
            print(f"\n📊 UF {cod_uf} ({ano_ini}-{ano_fim}) - DADOS REAIS PNAD:")
            print(f"  Tributos: R$ {tributos_ini/1e9:.2f}bi → R$ {tributos_fim/1e9:.2f}bi ({cresc_trib:+.1f}%)")
            print(f"  Massa PNAD: R$ {massa_ini/1e9:.2f}bi → R$ {massa_fim/1e9:.2f}bi ({cresc_massa:+.1f}%)")
            print(f"  Δln: {diferenca:+.4f}")
            
            if diferenca > 0.10:
                print(f"  🔴 Carga tributária AUMENTOU")
            elif diferenca < -0.10:
                print(f"  🟢 Carga tributária DIMINUIU")
            else:
                print(f"  ⚪ Carga tributária ESTÁVEL")
        
        return diferenca
        
    except Exception as e:
        if debug:
            print(f"❌ Erro no cálculo: {e}")
        return 0.0


# Teste
if __name__ == "__main__":
    print("="*70)
    print("PNAD via R - Dados REAIS")
    print("="*70)
    
    # Verifica se R está instalado
    try:
        result = subprocess.run(["Rscript", "--version"], capture_output=True, timeout=5)
        print(f"✅ R instalado: {result.stdout.decode()[:50]}")
    except:
        print("❌ R não encontrado! Instale R primeiro:")
        print("   Ubuntu: sudo apt install r-base")
        print("   Mac: brew install r")
        print("   Windows: https://cran.r-project.org/bin/windows/base/")
        exit(1)
    
    # Instala pacotes se necessário
    instalar_pacotes_r()
    
    print("\n" + "="*70)
    print(f"TESTE: Baixando dados PNAD 2018-{datetime.now().year}")
    print("="*70)
    
    # Baixa dados até ano atual
    # PNAD geralmente tem 3-4 meses de defasagem, então ano atual pode estar incompleto
    ano_atual = datetime.now().year
    anos = list(range(2018, ano_atual + 1))
    
    print(f"📊 Tentando baixar: {anos}")
    print(f"   Nota: Ano {ano_atual} pode estar incompleto (PNAD tem ~3 meses de defasagem)")
    
    if baixar_pnad_via_r(anos, "pnad_teste.json"):
        
        # Carrega e testa
        dados = carregar_dados_pnad("pnad_teste.json")
        
        if dados:
            print("\n" + "="*70)
            print("EXEMPLO: Mato Grosso")
            print("="*70)
            
            resultado = calcular_delta_ln_com_pnad_real(
                cod_uf=51,
                tributos_ini=7_760_814_265,
                tributos_fim=11_000_000_000,
                ano_ini=2018,
                ano_fim=2023,
                dados_pnad=dados,
                debug=True
            )
            
            print(f"\n{'='*70}")
            print(f"✅ SUCESSO! Δln = {resultado:+.4f}")
            print(f"{'='*70}")
