# GCS Bronze Layer Cleanup Commands (PowerShell)
# This script removes duplicate extractions, keeping only the latest file for each table

# Ensure gcloud is in PATH
$env:PATH += ";C:\Users\$env:USERNAME\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin"

Write-Host "Starting GCS Bronze Layer cleanup..." -ForegroundColor Green

# Remove older extractions for agenda_usuarios (keep 11:29:05, remove 11:24:31)
Write-Host "Cleaning agenda_usuarios..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/agenda_usuarios/date=2025/08/18/agenda_usuarios_full_20250818_112431.parquet"

# Remove older extractions for alb_cli (keep 11:29:02, remove 11:24:32)
Write-Host "Cleaning alb_cli..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/alb_cli/date=2025/08/18/alb_cli_full_20250818_112432.parquet"

# Remove older extractions for alb_cli_lin (keep 11:29:02, remove 11:24:31)
Write-Host "Cleaning alb_cli_lin..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/alb_cli_lin/date=2025/08/18/alb_cli_lin_full_20250818_112431.parquet"

# Remove older extractions for alb_pro (keep 11:29:05, remove 11:24:52)
Write-Host "Cleaning alb_pro..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/alb_pro/date=2025/08/18/alb_pro_full_20250818_112452.parquet"

# Remove older extractions for alb_pro_lin (keep 11:29:10, remove 11:24:37)
Write-Host "Cleaning alb_pro_lin..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/alb_pro_lin/date=2025/08/18/alb_pro_lin_full_20250818_112437.parquet"

# Remove older extractions for Pie_Fac (keep 11:29:14, remove 11:18:07 and 11:24:42)
Write-Host "Cleaning Pie_Fac..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/Pie_Fac/date=2025/08/18/Pie_Fac_full_20250818_111807.parquet"
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/Pie_Fac/date=2025/08/18/Pie_Fac_full_20250818_112442.parquet"

# Remove older extractions for Pie_Fac_ML (keep 11:29:19, remove 11:24:47)
Write-Host "Cleaning Pie_Fac_ML..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/Pie_Fac_ML/date=2025/08/18/Pie_Fac_ML_full_20250818_112447.parquet"

# Remove older extractions for Recambios (keep 11:29:05, remove 11:24:32)
Write-Host "Cleaning Recambios..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/Recambios/date=2025/08/18/Recambios_full_20250818_112432.parquet"

# Also remove the standalone Pie_Fac file from earlier test
Write-Host "Cleaning standalone files..." -ForegroundColor Yellow
gcloud storage rm "gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/Pie_Fac/date=2025/08/18/Pie_Fac_20250818_110412.parquet"

Write-Host "Cleanup completed - only the latest extraction files remain for each table" -ForegroundColor Green

# Verify cleanup
Write-Host "`nVerifying cleanup results..." -ForegroundColor Cyan
gcloud storage ls gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/*/date=2025/08/18/ | findstr "_full_" | Measure-Object -Line
