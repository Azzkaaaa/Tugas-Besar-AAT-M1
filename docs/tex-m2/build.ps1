param(
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$ReportName = 'IF4031_M2_nasigoreng'
)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    New-Item -ItemType Directory -Path 'build' -Force | Out-Null
    for ($pass = 1; $pass -le 3; $pass++) {
        & pdflatex -interaction=nonstopmode -halt-on-error -file-line-error -output-directory=build "-jobname=$ReportName" main.tex
        if ($LASTEXITCODE -ne 0) {
            throw "Kompilasi gagal pada putaran $pass. Lihat build/$ReportName.log."
        }
    }
    Copy-Item -LiteralPath "build/$ReportName.pdf" -Destination "../$ReportName.pdf" -Force
    Write-Output "PDF siap: $(Join-Path (Split-Path $PSScriptRoot -Parent) "$ReportName.pdf")"
}
finally { Pop-Location }
