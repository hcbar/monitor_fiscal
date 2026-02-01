#!/bin/bash
# limpar_repo.sh
# Script para limpar arquivos desnecessários do repositório
# Execute com: bash limpar_repo.sh

echo "🧹 Limpando repositório monitor_fiscal..."
echo ""

# Arquivos Python legados
ARQUIVOS_REMOVER=(
    "app_antigo.py"
    "etl_completo.py"
    "etl_completo_2.py"
    "etl_divida.py"
    "etl_divida_2.py"
    "diagnostico.py"
    "pnad_via_r.py"
    "pnad_windows.py"
    "pnad_dados.json"
    "temp_pnad_massa_trimestral.R"
    "estrutura.txt"
    "estrutura_projeto.txt"
    "guia.pdf"
    "grafico_trajetoria_divida.png"
    "dados_ranking_estados_final.csv"
    "dados_ranking_estados_v2.csv"
    "etl_csv_local.py"
)

# Pastas para remover
PASTAS_REMOVER=(
    "claude"
    "auditoria"
    ".github"
)

echo "📄 Removendo arquivos legados..."
for arquivo in "${ARQUIVOS_REMOVER[@]}"; do
    if [ -f "$arquivo" ]; then
        echo "   Removendo: $arquivo"
        rm -f "$arquivo"
    fi
done

echo ""
echo "📁 Removendo pastas legadas..."
for pasta in "${PASTAS_REMOVER[@]}"; do
    if [ -d "$pasta" ]; then
        echo "   Removendo: $pasta/"
        rm -rf "$pasta"
    fi
done

echo ""
echo "🗜️ Removendo arquivos ZIP em dados_brutos..."
find dados_brutos -name "*.zip" -type f -delete 2>/dev/null

echo ""
echo "📦 Estrutura final:"
echo ""
tree -L 3 --dirsfirst 2>/dev/null || find . -maxdepth 3 -type f | head -30

echo ""
echo "✅ Limpeza concluída!"
echo ""
echo "Próximos passos:"
echo "  1. Revise as mudanças: git status"
echo "  2. Adicione os arquivos: git add ."
echo "  3. Commit: git commit -m 'Limpeza do repositório - estrutura simplificada'"
echo "  4. Push: git push origin main"
