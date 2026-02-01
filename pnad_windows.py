r"""
PNAD via R (Windows) — Massa salarial por UF (microdados PNAD Contínua) - versão corrigida

O que este script faz (de forma robusta):
- Auto-detecta o Rscript.exe em C:\Program Files\R\R-*\bin\Rscript.exe
- Instala pacotes R necessários (inclui survey)
- Baixa PNAD Contínua TRIMESTRAL via PNADcIBGE (get_pnadc(year, quarter=...))
- Calcula MASSA SALARIAL por UF usando:
    * Ocupados (VD4002 == 1)  [se existir no layout]
    * Rendimento habitual do trabalho (VD4019)  [âncora recomendada]
    * Pesos amostrais do objeto survey (weights(design))
- Salva JSON "pnad_massa_uf.json" com: UF, ano, trimestre, massa_mensal_nominal, massa_trimestral_nominal

Notas importantes:
- PNAD é trimestral: use média móvel 4T ou compare início/fim do mandato.
- Se alguma variável mudar em layout futuro, o script vai falhar com mensagem clara
  (não vai “engolir” e retornar 0 silenciosamente).
"""

import subprocess
import json
import os
import math
from typing import Dict, Optional, List, Tuple
import glob


# -----------------------------
# Windows: achar Rscript.exe
# -----------------------------
def encontrar_rscript() -> Optional[str]:
    r_base = r"C:\Program Files\R"

    if not os.path.exists(r_base):
        print(f"❌ Pasta {r_base} não encontrada")
        return None

    pastas_r = glob.glob(os.path.join(r_base, "R-*"))
    if not pastas_r:
        print(f"❌ Nenhuma instalação do R encontrada em {r_base}")
        return None

    pasta_r = sorted(pastas_r)[-1]
    rscript_path = os.path.join(pasta_r, "bin", "Rscript.exe")

    if os.path.exists(rscript_path):
        print(f"✅ R encontrado: {rscript_path}")
        return rscript_path

    print(f"❌ Rscript.exe não encontrado em {rscript_path}")
    return None


# -----------------------------
# Instalar pacotes R
# -----------------------------
def instalar_pacotes_r(rscript_path: str) -> bool:
    print("\n📦 Instalando pacotes R (pode demorar na primeira vez)...")

    # Inclui 'survey' (estava faltando no seu código)
    script = r"""
    pkgs <- c("PNADcIBGE","survey","dplyr","tidyr","jsonlite")
    repos <- "https://cloud.r-project.org"
    for (p in pkgs) {
      if (!require(p, character.only=TRUE)) install.packages(p, repos=repos)
    }
    cat("\n✅ Pacotes instalados!\n")
    """

    try:
        result = subprocess.run(
            [rscript_path, "-e", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=900,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(f"⚠️ Avisos/Erros: {result.stderr}")
        return True
    except Exception as e:
        print(f"❌ Erro ao instalar pacotes: {e}")
        return False


# -----------------------------
# Baixar & calcular massa salarial por UF (TRIMESTRAL)
# -----------------------------
def baixar_massa_salarial_pnad_trimestral_via_r(
    anos: Optional[List[int]] = None,
    trimestres: Optional[List[int]] = None,
    salvar_em: str = "pnad_massa_uf.json",
    rscript_path: Optional[str] = None,
) -> bool:
    """
    Baixa PNAD Contínua trimestral via R/PNADcIBGE e salva massa salarial por UF.

    anos: lista de anos (ex.: [2018,2019,2022,2023,2024])
    trimestres: lista de trimestres (1..4). Ex.: [1,2,3,4]
    """

    if rscript_path is None:
        rscript_path = encontrar_rscript()
        if rscript_path is None:
            print("\n❌ ERRO: Não consegui encontrar o R!")
            print("Verifique se está instalado em: C:\\Program Files\\R")
            return False

    if anos is None:
        # escolha default: você pode ajustar
        anos = [2018, 2019, 2022, 2023, 2024]
        print(f"\n📅 Anos default: {anos}")

    if trimestres is None:
        trimestres = [1, 2, 3, 4]

    # Cache
    cache_dir = os.path.abspath("./pnad_cache")
    os.makedirs(cache_dir, exist_ok=True)
    print(f"\n📁 Cache PNAD: {cache_dir}")
    print(f"📦 Saída: {salvar_em}")
    print(f"📅 Períodos: anos={anos}, trimestres={trimestres}\n")

    # R script (corrigido)
    # - usa get_pnadc(year, quarter=...)
    # - usa VD4019 (rendimento habitual) e VD4002 (ocupado) se existir
    # - pesos: weights(design)
    # - falha explicitamente se variáveis não existirem
    script_r = f"""
cache_dir <- "{cache_dir.replace(chr(92), '/')}"
dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
options(PNADcIBGE.cache = cache_dir)
Sys.setenv(PNADC_CACHE_DIR = cache_dir)

library(PNADcIBGE)
library(survey)
library(dplyr)
library(tidyr)
library(jsonlite)

anos <- c({','.join(map(str, anos))})
trimestres <- c({','.join(map(str, trimestres))})

# Variáveis âncora
# VD4019: rendimento mensal habitual do trabalho principal (muito usada como "salário habitual")
# VD4002: condição na ocupação (ocupado=1) — pode variar conforme layout, mas costuma existir
vars <- c("UF","VD4019","VD4002")

resultado <- list()

for (ano in anos) {{
  for (tri in trimestres) {{
    cat(sprintf("\\n📊 Baixando PNAD %d T%d...\\n", ano, tri))

    tryCatch({{
      des <- get_pnadc(year = ano, quarter = tri, vars = vars, design = TRUE, labels = FALSE, savedir = cache_dir)

      # Checagens de variáveis
      vnames <- names(des$variables)
      needed <- c("UF","VD4019","VD4002")
      missing <- setdiff(needed, vnames)
      if (length(missing) > 0) {{
        stop(paste0("Variáveis ausentes no layout: ", paste(missing, collapse=", ")))
      }}

      # Filtra ocupados (VD4002 == 1) e renda habitual válida
      des_f <- subset(des, !is.na(UF) & !is.na(VD4019) & VD4019 > 0 & VD4002 == 1)

      df <- des_f$variables
      df$peso <- weights(des_f)

      dados_uf <- df %>%
        mutate(
          uf = as.integer(as.character(UF)),
          renda_hab = as.numeric(VD4019),
          peso = as.numeric(peso),
          renda_pond = pmax(renda_hab, 0) * pmax(peso, 0)
        ) %>%
        group_by(uf) %>%
        summarise(
          massa_mensal_nominal = sum(renda_pond, na.rm = TRUE),
          soma_pesos = sum(peso, na.rm = TRUE),
          renda_media_hab = ifelse(soma_pesos > 0, sum(renda_hab * peso, na.rm = TRUE)/soma_pesos, NA_real_),
          .groups = "drop"
        ) %>%
        mutate(
          ano = ano,
          trimestre = tri,
          massa_trimestral_nominal = massa_mensal_nominal * 3
        )

      key <- paste0(ano, "_T", tri)
      resultado[[key]] <- dados_uf
      cat(sprintf("✅ %s: %d UFs\\n", key, nrow(dados_uf)))

    }}, error = function(e) {{
      cat(sprintf("❌ Erro em %d T%d: %s\\n", ano, tri, e$message))
    }})

    Sys.sleep(1)
  }}
}}

if (length(resultado) > 0) {{
  dados <- bind_rows(resultado)
  write_json(dados, "{salvar_em.replace(chr(92), '/')}", pretty = TRUE)
  cat(sprintf("\\n✅ Salvo: %d linhas em {salvar_em}\\n", nrow(dados)))
}} else {{
  cat("\\n❌ Nenhum dado processado\\n")
}}
"""

    script_path = "temp_pnad_massa_trimestral.R"
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(script_r)

    try:
        result = subprocess.run(
            [rscript_path, script_path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=7200,  # até 2h dependendo do período e conexão
        )

        print(result.stdout)
        if result.returncode != 0:
            print("\n⚠️ STDERR do R (pode conter warnings úteis):")
            print(result.stderr)

        if os.path.exists(salvar_em):
            mb = os.path.getsize(salvar_em) / 1024 / 1024
            print(f"\n✅ SUCESSO: {salvar_em} ({mb:.1f} MB)")
            return True

        print(f"\n❌ Arquivo {salvar_em} não foi criado")
        return False

    except subprocess.TimeoutExpired:
        print("\n❌ Timeout: demorou mais que o limite")
        return False
    except Exception as e:
        print(f"\n❌ Erro ao executar R: {e}")
        return False
    finally:
        if os.path.exists(script_path):
            os.remove(script_path)


# -----------------------------
# Carregar JSON em estrutura fácil
# -----------------------------
def carregar_massa_trimestral_por_uf(arquivo_json: str = "pnad_massa_uf.json") -> Dict[int, Dict[Tuple[int, int], float]]:
    """
    Retorna:
      { uf: {(ano,trimestre): massa_mensal_nominal} }
    """
    if not os.path.exists(arquivo_json):
        raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_json}")

    with open(arquivo_json, "r", encoding="utf-8") as f:
        dados = json.load(f)

    out: Dict[int, Dict[Tuple[int, int], float]] = {}
    for r in dados:
        uf = int(r["uf"])
        ano = int(r["ano"])
        tri = int(r["trimestre"])
        massa = float(r["massa_mensal_nominal"])
        out.setdefault(uf, {})[(ano, tri)] = massa

    return out


# -----------------------------
# Índice taxador: própria vs massa salarial (crescimento proporcional)
# -----------------------------
def indice_taxador(
    arrec_ini: float,
    arrec_fim: float,
    massa_ini: float,
    massa_fim: float,
    usar_log: bool = True,
) -> float:
    """
    Retorna:
      Δln(arrec) - Δln(massa)  (se usar_log=True)
    ou
      (arrec_fim/arrec_ini - 1) - (massa_fim/massa_ini - 1)  (se usar_log=False)
    """
    if min(arrec_ini, arrec_fim, massa_ini, massa_fim) <= 0:
        raise ValueError("Todos os valores precisam ser > 0")

    if usar_log:
        return (math.log(arrec_fim) - math.log(arrec_ini)) - (math.log(massa_fim) - math.log(massa_ini))

    return (arrec_fim / arrec_ini - 1.0) - (massa_fim / massa_ini - 1.0)


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("=" * 70)
    print("PNAD via R - WINDOWS (TRIMESTRAL) — Massa Salarial por UF")
    print("=" * 70)

    rscript = encontrar_rscript()
    if not rscript:
        print("\n❌ Não encontrei o R em C:\\Program Files\\R")
        input("\nPressione Enter para sair...")
        raise SystemExit(1)

    if not instalar_pacotes_r(rscript):
        print("\n❌ Falha ao instalar pacotes")
        input("\nPressione Enter para sair...")
        raise SystemExit(1)

    ok = baixar_massa_salarial_pnad_trimestral_via_r(
        anos=[2018, 2019, 2022, 2023, 2024],  # ajuste aqui
        trimestres=[1, 2, 3, 4],
        salvar_em="pnad_massa_uf.json",
        rscript_path=rscript,
    )

    if ok:
        print("\n✅ Pronto. Arquivo pnad_massa_uf.json gerado.")
    else:
        print("\n❌ Falhou. Veja logs acima (geralmente variável ausente ou conexão).")

    input("\nPressione Enter para sair...")
