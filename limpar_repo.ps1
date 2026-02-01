# limpar_repo.ps1
# Script PowerShell para limpar o repositório monitor_fiscal no Windows
# Execute com: powershell -ExecutionPolicy Bypass -File limpar_repo.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Limpando repositorio monitor_fiscal" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Arquivos Python legados para remover
$arquivosRemover = @(
    "app_antigo.py",
    "etl_completo.py",
    "etl_completo_2.py",
    "etl_divida.py",
    "etl_divida_2.py",
    "diagnostico.py",
    "pnad_via_r.py",
    "pnad_windows.py",
    "pnad_dados.json",
    "temp_pnad_massa_trimestral.R",
    "estrutura.txt",
    "estrutura_projeto.txt",
    "guia.pdf",
    "grafico_trajetoria_divida.png",
    "dados_ranking_estados_final.csv",
    "dados_ranking_estados_v2.csv"
)

# Pastas para remover
$pastasRemover = @(
    "claude",
    "auditoria",
    ".github",
    "dados_brutos\2018",
    "dados_brutos\2022",
    "dados_brutos\2025",
    "dados_brutos\2026"
)

Write-Host "[1/4] Removendo arquivos legados..." -ForegroundColor Yellow
foreach ($arquivo in $arquivosRemover) {
    if (Test-Path $arquivo) {
        Remove-Item $arquivo -Force
        Write-Host "   Removido: $arquivo" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "[2/4] Removendo pastas legadas..." -ForegroundColor Yellow
foreach ($pasta in $pastasRemover) {
    if (Test-Path $pasta) {
        Remove-Item $pasta -Recurse -Force
        Write-Host "   Removido: $pasta\" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "[3/4] Removendo arquivos ZIP em dados_brutos..." -ForegroundColor Yellow
Get-ChildItem -Path "dados_brutos" -Recurse -Filter "*.zip" -ErrorAction SilentlyContinue | ForEach-Object {
    Remove-Item $_.FullName -Force
    Write-Host "   Removido: $($_.FullName)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "[4/4] Removendo arquivos duplicados de meta_primario..." -ForegroundColor Yellow
# Remove arquivos com (2) no nome
Get-ChildItem -Path "dados_brutos" -Recurse -Filter "*(*)*" -ErrorAction SilentlyContinue | ForEach-Object {
    Remove-Item $_.FullName -Force
    Write-Host "   Removido: $($_.FullName)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Limpeza concluida!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Estrutura final esperada:" -ForegroundColor Cyan
Write-Host "  monitor_fiscal\"
Write-Host "  |-- etl.py"
Write-Host "  |-- app.py"
Write-Host "  |-- governadores.csv"
Write-Host "  |-- requirements.txt"
Write-Host "  |-- README.md"
Write-Host "  |-- .gitignore"
Write-Host "  |-- dados_ranking_estados.csv"
Write-Host "  \-- dados_brutos\"
Write-Host "      \-- 2024\"
Write-Host "          |-- resultado_primario\"
Write-Host "          |-- receita_corrente_liquida\"
Write-Host "          \-- meta_primario\"
Write-Host ""
Write-Host "Proximos passos:" -ForegroundColor Yellow
Write-Host "  1. Salve o novo etl.py (vou te enviar)"
Write-Host "  2. Teste: python etl.py"
Write-Host "  3. Commit: git add . && git commit -m 'Limpeza do repositorio'"
Write-Host "  4. Push: git push origin main"
Write-Host ""
