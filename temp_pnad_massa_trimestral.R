
cache_dir <- "C:/Users/cerqu/Documents/monitor_fiscal/pnad_cache"
dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
options(PNADcIBGE.cache = cache_dir)
Sys.setenv(PNADC_CACHE_DIR = cache_dir)

library(PNADcIBGE)
library(survey)
library(dplyr)
library(tidyr)
library(jsonlite)

anos <- c(2018,2019,2022,2023,2024)
trimestres <- c(1,2,3,4)

# Variáveis âncora
# VD4019: rendimento mensal habitual do trabalho principal (muito usada como "salário habitual")
# VD4002: condição na ocupação (ocupado=1) — pode variar conforme layout, mas costuma existir
vars <- c("UF","VD4019","VD4002")

resultado <- list()

for (ano in anos) {
  for (tri in trimestres) {
    cat(sprintf("\n📊 Baixando PNAD %d T%d...\n", ano, tri))

    tryCatch({
      des <- get_pnadc(year = ano, quarter = tri, vars = vars, design = TRUE, labels = FALSE, savedir = cache_dir)

      # Checagens de variáveis
      vnames <- names(des$variables)
      needed <- c("UF","VD4019","VD4002")
      missing <- setdiff(needed, vnames)
      if (length(missing) > 0) {
        stop(paste0("Variáveis ausentes no layout: ", paste(missing, collapse=", ")))
      }

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
      cat(sprintf("✅ %s: %d UFs\n", key, nrow(dados_uf)))

    }, error = function(e) {
      cat(sprintf("❌ Erro em %d T%d: %s\n", ano, tri, e$message))
    })

    Sys.sleep(1)
  }
}

if (length(resultado) > 0) {
  dados <- bind_rows(resultado)
  write_json(dados, "pnad_massa_uf.json", pretty = TRUE)
  cat(sprintf("\n✅ Salvo: %d linhas em pnad_massa_uf.json\n", nrow(dados)))
} else {
  cat("\n❌ Nenhum dado processado\n")
}
