# Incremental Loading Strategy Guide

Generated on: 2025-08-19 08:29:38

## Summary

- **Total Tables Analyzed**: 220
- **Incremental Loading Candidates**: 63
- **Full Load Recommended**: 157

### Strategy Breakdown

- **FULL_REPLACE**: 116 tables
- **INCREMENTAL_PREFERRED**: 26 tables
- **INCREMENTAL_POSSIBLE**: 37 tables
- **INCREMENTAL_CHALLENGING**: 39 tables
- **ERROR**: 2 tables

## High Priority Incremental Candidates

These tables are excellent candidates for incremental loading:

### accesos_presencia

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 10,946
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 3 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)

### audit_errores

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 0
- **Watermark Column**: `f_fecha`
- **Implementation**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'f_fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic
  - Historical/audit table - good incremental candidate

### audit_ia

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 15,371
- **Watermark Column**: `timestamp`
- **Implementation**: Implement incremental loading using 'timestamp' as watermark column. Query: SELECT * FROM table WHERE timestamp > last_run_timestamp
- **Analysis Reasons**:
  - Found 2 timestamp column(s)
  - Using 'timestamp' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic
  - Historical/audit table - good incremental candidate

### avi_comen_rec_his_dev

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 173
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)
  - Historical/audit table - good incremental candidate

### avi_comen_rec_his_est

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 23,392
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)
  - Historical/audit table - good incremental candidate

### avi_his_est

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 163,049
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)
  - Historical/audit table - good incremental candidate

### avi_his_porcentajes

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 3,561
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)
  - Historical/audit table - good incremental candidate

### avi_his_revisiones

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 74,403
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)
  - Historical/audit table - good incremental candidate

### contratos_log

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 3,724
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic
  - Historical/audit table - good incremental candidate

### errores_ia

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 622
- **Watermark Column**: `timestamp`
- **Implementation**: Implement incremental loading using 'timestamp' as watermark column. Query: SELECT * FROM table WHERE timestamp > last_run_timestamp
- **Analysis Reasons**:
  - Found 2 timestamp column(s)
  - Using 'timestamp' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic

### fac_cli_firmas

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 26,366
- **Watermark Column**: `f_fecha`
- **Implementation**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 3 timestamp column(s)
  - Using 'f_fecha' as fallback watermark
  - Composite primary key (manageable for incremental)
  - Transaction table - likely benefits from incremental loading

### gh_historico_acciones

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 2,002
- **Watermark Column**: `f_fecha`
- **Implementation**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'f_fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic
  - Historical/audit table - good incremental candidate

### mostrar_pdf

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 2,543
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 2 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic

### mostrar_pdf_alb

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 55
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 2 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic
  - Transaction table - likely benefits from incremental loading

### mostrar_pdf_con

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 62
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 2 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic

### ped_pro

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 64,678
- **Watermark Column**: `f_fecha_enviado`
- **Implementation**: Implement incremental loading using 'f_fecha_enviado' as watermark column. Query: SELECT * FROM table WHERE f_fecha_enviado > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'f_fecha_enviado' as fallback watermark
  - Composite primary key (manageable for incremental)
  - Transaction table - likely benefits from incremental loading

### piezas_1

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 378,719
- **Watermark Column**: `fecha_pmp`
- **Implementation**: Implement incremental loading using 'fecha_pmp' as watermark column. Query: SELECT * FROM table WHERE fecha_pmp > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha_pmp' as fallback watermark
  - Single primary key simplifies incremental logic

### piezas_2

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 389,345
- **Watermark Column**: `fecha_pmp`
- **Implementation**: Implement incremental loading using 'fecha_pmp' as watermark column. Query: SELECT * FROM table WHERE fecha_pmp > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha_pmp' as fallback watermark
  - Single primary key simplifies incremental logic

### piezas_4

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 318,588
- **Watermark Column**: `fecha_pmp`
- **Implementation**: Implement incremental loading using 'fecha_pmp' as watermark column. Query: SELECT * FROM table WHERE fecha_pmp > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha_pmp' as fallback watermark
  - Single primary key simplifies incremental logic

### programacion

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 5,630
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 5 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)

### seg_token

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 3,135
- **Watermark Column**: `f_fecha_in`
- **Implementation**: Implement incremental loading using 'f_fecha_in' as watermark column. Query: SELECT * FROM table WHERE f_fecha_in > last_run_timestamp
- **Analysis Reasons**:
  - Found 2 timestamp column(s)
  - Using 'f_fecha_in' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic

### soap_price_list

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 440,657
- **Watermark Column**: `f_fecha`
- **Implementation**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'f_fecha' as fallback watermark
  - Single primary key simplifies incremental logic

### soap_price_list_versiones

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 12
- **Watermark Column**: `f_fecha`
- **Implementation**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'f_fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Single primary key simplifies incremental logic

### token_byVIN

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 761
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Auto-increment column available for incremental loading
  - Composite primary key (manageable for incremental)

### vista_movimientos_historicos

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 165,018
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - No primary key found (complicates incremental loading)
  - Historical/audit table - good incremental candidate
  - Transaction table - likely benefits from incremental loading

### vto_cli_his_agru

- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH
- **Row Count**: 11,995
- **Watermark Column**: `fecha`
- **Implementation**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp
- **Analysis Reasons**:
  - Found 1 timestamp column(s)
  - Using 'fecha' as fallback watermark
  - Composite primary key (manageable for incremental)
  - Historical/audit table - good incremental candidate

## Implementation Examples

### Timestamp-based Incremental Loading

Example with table `accesos_presencia`:

```sql
-- Initial load
SELECT * FROM accesos_presencia
WHERE fecha >= '2024-01-01'

-- Incremental load (subsequent runs)
SELECT * FROM accesos_presencia
WHERE fecha > '{last_run_timestamp}'
```

## Complete Analysis Results

### Pie_Fac
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -5)
- **Rows**: 107,634
- **Notes**: Full table replacement recommended. Simple and reliable approach

### Pie_Fac_ML
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -5)
- **Rows**: 107,634
- **Notes**: Full table replacement recommended. Simple and reliable approach

### Recambios
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -20)
- **Rows**: 22
- **Notes**: Full table replacement recommended. Simple and reliable approach

### accesos_presencia
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 90)
- **Rows**: 10,946
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo, ic_cod_usu, ic_cod_ofi
- **Timestamp Columns**: fecha, f_fec_mod, fecha_crea
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### agenda
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 301
- **Watermark**: `fecha_ini`
- **Primary Keys**: ic_ano, ic_cod_age, ic_cod_ofi
- **Timestamp Columns**: fecha_ini, fecha_fin
- **Notes**: Incremental loading feasible with 'fecha_ini'. Consider data volume and update patterns to decide vs full replace

### agenda_usuarios
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 820
- **Primary Keys**: ic_ano, ic_cod_age, ic_cod_ofi, ic_cod_usu
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alb_cli
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 20,607
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_alb
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alb_cli_lin
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 44,027
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_alb, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alb_pro
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 85,624
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_alb
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alb_pro_lin
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 124,338
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_alb, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alm
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 40
- **Primary Keys**: ic_cod_alm
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_coste
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 16,149
- **Primary Keys**: ic_cod_alm, cc_cod_pie, ic_orden
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_his_1
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 306,612
- **Primary Keys**: ic_ano, ic_cod_alm, cc_cod_pie, ic_cod_ofi, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alm_his_2
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 164,979
- **Primary Keys**: ic_ano, ic_cod_alm, cc_cod_pie, ic_cod_ofi, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alm_his_4
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 0
- **Primary Keys**: ic_ano, ic_cod_alm, cc_cod_pie, ic_cod_ofi, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alm_lug_alt
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 278
- **Primary Keys**: ic_cod_lug, ic_cod_ofi, ic_cod_alm
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_lug_caj
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 201
- **Primary Keys**: ic_cod_lug, ic_cod_ofi, ic_cod_alm
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_lug_est
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 278
- **Primary Keys**: ic_cod_lug, ic_cod_ofi, ic_cod_alm
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_lug_mod
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 402
- **Primary Keys**: ic_cod_lug, ic_cod_ofi, ic_cod_alm
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_mov
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 7
- **Primary Keys**: ic_cod_mov
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### alm_ofi
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 160
- **Primary Keys**: ic_cod_ofi, ic_cod_alm
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_pie_1
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 96,240
- **Primary Keys**: ic_cod_alm, cc_cod_pie, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_pie_2
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 40,509
- **Primary Keys**: ic_cod_alm, cc_cod_pie, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### alm_pie_4
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 0
- **Primary Keys**: ic_cod_alm, cc_cod_pie, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### audit_errores
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 120)
- **Rows**: 0
- **Watermark**: `f_fecha`
- **Primary Keys**: id
- **Timestamp Columns**: f_fecha
- **Notes**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp

### audit_ia
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 120)
- **Rows**: 15,371
- **Watermark**: `timestamp`
- **Primary Keys**: ic_cod_log
- **Timestamp Columns**: timestamp, fecha
- **Notes**: Implement incremental loading using 'timestamp' as watermark column. Query: SELECT * FROM table WHERE timestamp > last_run_timestamp

### audit_ia_tipo
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 60)
- **Rows**: 2
- **Primary Keys**: ic_cod_tipo
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### avi_comen
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 67,288
- **Watermark**: `f_fecha`
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_cod_usu, ic_linea
- **Timestamp Columns**: f_fecha
- **Notes**: Incremental loading feasible with 'f_fecha'. Consider data volume and update patterns to decide vs full replace

### avi_comen_rec
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 8,525
- **Watermark**: `f_fecha`
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_cod_usu, ic_linea
- **Timestamp Columns**: f_fecha, f_fecha_prev, f_fecha_ent
- **Notes**: Incremental loading feasible with 'f_fecha'. Consider data volume and update patterns to decide vs full replace

### avi_comen_rec_est
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 13
- **Primary Keys**: ic_cod_est
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_comen_rec_his_dev
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 110)
- **Rows**: 173
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo, ic_ano, ic_cod_avi, ic_cod_ofi, ic_linea
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### avi_comen_rec_his_est
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 110)
- **Rows**: 23,392
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo, ic_ano, ic_cod_avi, ic_cod_ofi, ic_linea
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### avi_comen_rec_pri
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 3
- **Primary Keys**: ic_cod_pri
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_comen_rec_tip
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 2
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_estados
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 9
- **Primary Keys**: ic_cod_est
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### avi_gar
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 0
- **Primary Keys**: ic_ano, ic_cod_avi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_his_est
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 110)
- **Rows**: 163,049
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo, ic_ano, ic_cod_avi, ic_cod_ofi
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### avi_his_porcentajes
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 110)
- **Rows**: 3,561
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo, ic_ano, ic_cod_avi, ic_cod_ofi
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### avi_his_revisiones
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 110)
- **Rows**: 74,403
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo, ic_ano, ic_cod_avi, ic_cod_ofi
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### avi_hor_fac
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 81,455
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_cod_usu, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### avi_horas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 308,104
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_jobs
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 19
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_cod_job
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_lineas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 373,446
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_lineas_
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 29,755
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_meta
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 57,796
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_motivos_parada
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 4
- **Primary Keys**: ic_cod_mot
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### avi_obs
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 252,756
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_cod_tip, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_obs_
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 5,596
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi, ic_cod_tip, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_tip_fic
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 18
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avi_tipos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 20
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### avisos
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 76,202
- **Watermark**: `f_fec_rev_rec`
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi
- **Timestamp Columns**: f_fec_rev_rec
- **Notes**: Incremental loading feasible with 'f_fec_rev_rec'. Consider data volume and update patterns to decide vs full replace

### avisos_
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 4,362
- **Primary Keys**: ic_ano, ic_cod_avi, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### bancos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 12
- **Primary Keys**: ic_cod_ban, ic_cod_suc
- **Notes**: Full table replacement recommended. Simple and reliable approach

### bd_config_planificador
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -10)
- **Rows**: 4
- **Primary Keys**: ic_codigo, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### bd_config_tareas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -10)
- **Rows**: 4
- **Primary Keys**: ic_codigo, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### campanas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 3
- **Primary Keys**: ic_cod_cam
- **Notes**: Full table replacement recommended. Simple and reliable approach

### campanas_lineas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 2
- **Primary Keys**: ic_cod_cam, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### categorias
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 27
- **Primary Keys**: cc_cod_cat
- **Notes**: Full table replacement recommended. Simple and reliable approach

### con_diario
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 302,060
- **Primary Keys**: ic_ano, ic_cod_asi, ic_cod_apu, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### con_letras
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 15
- **Primary Keys**: ic_cod_let
- **Notes**: Full table replacement recommended. Simple and reliable approach

### con_subctas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 172
- **Primary Keys**: cc_cod_sub, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### con_tipos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 0
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### con_tmp
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 58
- **Primary Keys**: ic_cod_asi, ic_cod_apu, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### contratos
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 15,299
- **Watermark**: `fecha`
- **Primary Keys**: ic_ano, ic_cod_con, ic_cod_ofi
- **Timestamp Columns**: fecha, fecha_fir1, fecha_fir2, fecha_fir3, fecha_fir_sal, fecha_fir_ract, fecha_prevista_entrada, fecha_prevista_salida, fecha_entrada, fecha_salida, f_last_service
- **Notes**: Incremental loading feasible with 'fecha'. Consider data volume and update patterns to decide vs full replace

### contratos_estados
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 6
- **Primary Keys**: ic_cod_est
- **Notes**: Full table replacement recommended. Simple and reliable approach

### contratos_insertar
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 918
- **Primary Keys**: ic_ano, ic_cod_con
- **Notes**: Full table replacement recommended. Simple and reliable approach

### contratos_log
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 120)
- **Rows**: 3,724
- **Watermark**: `fecha`
- **Primary Keys**: id
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### contratos_ra
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 0
- **Watermark**: `f_fecha_matriculacion`
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_con
- **Timestamp Columns**: f_fecha_matriculacion, f_fecha_recepcion
- **Notes**: Incremental loading feasible with 'f_fecha_matriculacion'. Consider data volume and update patterns to decide vs full replace

### contratos_ra_lineas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 139,581
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_con, id
- **Notes**: Full table replacement recommended. Simple and reliable approach

### danos_jobs
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 9
- **Primary Keys**: ic_cod_job
- **Notes**: Full table replacement recommended. Simple and reliable approach

### danos_subzonas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 406
- **Primary Keys**: ic_cod_zon, ic_cod_sub
- **Notes**: Full table replacement recommended. Simple and reliable approach

### danos_zonas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 80
- **Primary Keys**: ic_cod_zon
- **Notes**: Full table replacement recommended. Simple and reliable approach

### danos_zonas_lineas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 1,220
- **Primary Keys**: ic_cod_zon, ic_cod_sub, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### docs
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 5,359
- **Primary Keys**: ic_cod_doc, ic_cod_ofi, ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### docs_albcli
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 168
- **Primary Keys**: ic_cod_doc, ic_cod_ofi, ic_cod_tip
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### docs_albpro
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 2
- **Primary Keys**: ic_cod_doc, ic_cod_ofi, ic_cod_tip
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### docs_comu
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 4
- **Primary Keys**: ic_cod_doc, ic_cod_ofi, ic_cod_tip, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### docs_pro
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 61
- **Primary Keys**: ic_cod_doc, ic_cod_ofi, ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### docs_tipos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 6
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_fam
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 581
- **Primary Keys**: ic_cod_ofi, ic_cod_cli, ic_cod_fam
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_pie
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 228
- **Primary Keys**: ic_cod_ofi, ic_cod_cli, cc_cod_pie
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_sub_fam
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 2
- **Primary Keys**: ic_cod_ofi, ic_cod_cli, ic_cod_fam, ic_cod_sub
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_tipos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 18
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_tipos_fam
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -10)
- **Rows**: 0
- **Primary Keys**: ic_cod_tip, ic_cod_fam
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_tipos_margen
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -10)
- **Rows**: 0
- **Primary Keys**: ic_cod_tip, ic_cod_tra
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_tipos_pie
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -10)
- **Rows**: 0
- **Primary Keys**: ic_cod_tip, cc_cod_pie
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_tipos_sub_fam
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -10)
- **Rows**: 491
- **Primary Keys**: ic_cod_tip, ic_cod_fam, ic_cod_sub
- **Notes**: Full table replacement recommended. Simple and reliable approach

### dto_cli_tramos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 0
- **Primary Keys**: ic_cod_ofi, ic_cod_cli, ic_cod_tra
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 5,340
- **Primary Keys**: ic_cod_cli, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_ban
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 0
- **Primary Keys**: ic_cod_cli, ic_cod_ofi, ic_cod_ban, ic_cod_suc, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### ent_cli_blo
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 2,390
- **Primary Keys**: ic_cod_cli, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_com
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 1,160
- **Primary Keys**: ic_cod_cli, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_con
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 763
- **Primary Keys**: ic_cod_cli, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_des
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -20)
- **Rows**: 102
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_doc
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 2,777
- **Primary Keys**: ic_cod_cli, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_doc_tip
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 7
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_fam
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 8
- **Primary Keys**: ic_cod_fam
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_fam_des
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 8
- **Primary Keys**: ic_cod_cla, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_cli_origen
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 11
- **Primary Keys**: ic_cod_ori
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### ent_cli_rie
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 5,215
- **Primary Keys**: ic_cod_cli, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_pro
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 1,540
- **Primary Keys**: ic_cod_pro, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_pro_blo
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 14
- **Primary Keys**: ic_cod_pro, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ent_pro_fam
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 1
- **Primary Keys**: ic_cod_fam
- **Notes**: Full table replacement recommended. Simple and reliable approach

### errores_ia
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 100)
- **Rows**: 622
- **Watermark**: `timestamp`
- **Primary Keys**: ic_cod_err
- **Timestamp Columns**: timestamp, fecha
- **Notes**: Implement incremental loading using 'timestamp' as watermark column. Query: SELECT * FROM table WHERE timestamp > last_run_timestamp

### fac_cli
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 54,189
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_fac, ic_cod_let
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### fac_cli_aut
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 36,553
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_fac, ic_cod_let, ic_fac, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### fac_cli_est
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 3
- **Primary Keys**: ic_cod_est
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### fac_cli_firmas
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 80)
- **Rows**: 26,366
- **Watermark**: `f_fecha`
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_fac, ic_cod_let, ic_linea
- **Timestamp Columns**: f_fecha, f_fecha_firma, f_fecha_envio
- **Notes**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp

### fac_cli_lin
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 60,103
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_fac, ic_cod_let, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### fac_pro
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 26,594
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_fac, ic_cod_let
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### fac_pro_est
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 3
- **Primary Keys**: ic_cod_est
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### fac_pro_lin
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 68,097
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_fac, ic_cod_let, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### fields_gauges
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 10
- **Primary Keys**: ic_cod_gau
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### fields_typ_operators
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 10
- **Primary Keys**: ic_typ_ope
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### fields_types
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 8
- **Primary Keys**: ic_cod_typ
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### firmas
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 48
- **Primary Keys**: ic_ano, ic_cod_avi, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### for_pago
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 36
- **Primary Keys**: ic_cod_pag
- **Notes**: Full table replacement recommended. Simple and reliable approach

### for_pago2
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 24
- **Primary Keys**: ic_cod_pag
- **Notes**: Full table replacement recommended. Simple and reliable approach

### garantias
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 3,113
- **Primary Keys**: ic_ano, ic_cod_gar, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### gh_acciones
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 0
- **Primary Keys**: ic_cod_acc
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### gh_estados
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 3
- **Primary Keys**: ic_cod_est
- **Notes**: Full table replacement recommended. Simple and reliable approach

### gh_estados_devolucion
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: -10)
- **Rows**: 3
- **Primary Keys**: ic_cod_est, c_nombre
- **Notes**: Full table replacement recommended. Simple and reliable approach

### gh_historico_acciones
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 120)
- **Rows**: 2,002
- **Watermark**: `f_fecha`
- **Primary Keys**: id
- **Timestamp Columns**: f_fecha
- **Notes**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp

### gh_lista_espera
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 1
- **Watermark**: `f_fecha`
- **Primary Keys**: ic_cod_ofi, ic_cod_pie, ic_cod_usu
- **Timestamp Columns**: f_fecha
- **Notes**: Incremental loading feasible with 'f_fecha'. Consider data volume and update patterns to decide vs full replace

### gh_piezas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 128
- **Primary Keys**: ic_cod_ofi, ic_cod_pie
- **Notes**: Full table replacement recommended. Simple and reliable approach

### gh_piezas_lineas
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 128
- **Watermark**: `f_fecha_inicio`
- **Primary Keys**: ic_cod_ofi, ic_cod_pie, ic_linea
- **Timestamp Columns**: f_fecha_inicio, f_fecha_final
- **Notes**: Incremental loading feasible with 'f_fecha_inicio'. Consider data volume and update patterns to decide vs full replace

### jobs_carpetas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 100
- **Primary Keys**: ic_cod_job
- **Notes**: Full table replacement recommended. Simple and reliable approach

### jobs_maestras
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 2
- **Primary Keys**: ic_cod_job
- **Notes**: Full table replacement recommended. Simple and reliable approach

### mae_carpetas
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 4
- **Primary Keys**: ic_cod_car
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### mae_ficheros
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 27
- **Primary Keys**: ic_cod_fic
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### mae_tables
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 18
- **Primary Keys**: ic_cod_tab
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### mae_tables_family
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 9
- **Primary Keys**: ic_cod_fam
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### mae_tables_fields
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 203
- **Primary Keys**: ic_cod_tab, cc_cod_fie
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### mae_tables_fields_groupby
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 0
- **Primary Keys**: ic_cod_tab, cc_cod_fie
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### mae_tables_fields_usu_field
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 136
- **Primary Keys**: ic_cod_tab, cc_cod_fie, ic_cod_usu
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### mae_tables_fields_usu_order
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 14
- **Primary Keys**: ic_cod_tab, cc_cod_fie, ic_cod_usu
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### mae_views
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 0
- **Primary Keys**: ic_cod_vie
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### mae_views_fields
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 0
- **Primary Keys**: ic_cod_vie, ic_cod_tab, cc_cod_fie, ic_cod_usu
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### mae_views_where
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 0
- **Primary Keys**: ic_cod_vie, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### marcas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 74
- **Primary Keys**: ic_cod_mar
- **Notes**: Full table replacement recommended. Simple and reliable approach

### margen_cliente
- **Strategy**: ERROR
- **Confidence**: LOW (Score: 0)
- **Rows**: 0
- **Notes**: None

### margen_cliente_alb_avi
- **Strategy**: ERROR
- **Confidence**: LOW (Score: 0)
- **Rows**: 0
- **Notes**: None

### metacode_actt
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 705
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_cont
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 683
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_loct
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 489
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_objt
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 23,811
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_ran
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 3,185
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_rea
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 51,879
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_text
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 647,283
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_val
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 817,778
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### metacode_vrst
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 709
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### mostrar_pdf
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 100)
- **Rows**: 2,543
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo
- **Timestamp Columns**: fecha, fecha2
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### mostrar_pdf_alb
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 115)
- **Rows**: 55
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo
- **Timestamp Columns**: fecha, fecha2
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### mostrar_pdf_con
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 100)
- **Rows**: 62
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo
- **Timestamp Columns**: fecha, fecha2
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### nam_ban
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 20
- **Primary Keys**: ic_cod_ban, ic_cod_suc, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ped_pro
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 80)
- **Rows**: 64,678
- **Watermark**: `f_fecha_enviado`
- **Primary Keys**: ic_ano, ic_cod_ofi, ic_cod_ped
- **Timestamp Columns**: f_fecha_enviado
- **Notes**: Implement incremental loading using 'f_fecha_enviado' as watermark column. Query: SELECT * FROM table WHERE f_fecha_enviado > last_run_timestamp

### ped_pro_des
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 0
- **Primary Keys**: ic_cod_pro, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### ped_pro_est
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 5
- **Primary Keys**: ic_cod_est
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### ped_pro_lin
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 108,173
- **Primary Keys**: ic_cod_ofi, ic_ano, ic_cod_ped, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### ped_pro_lin_his
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 4,334
- **Primary Keys**: ic_codigo, ic_cod_ofi, ic_ano, ic_cod_ped, ic_linea
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### ped_pro_tip
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 30)
- **Rows**: 6
- **Primary Keys**: ic_cod_tip
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### pie_descuentos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 1
- **Primary Keys**: ic_cod_des
- **Notes**: Full table replacement recommended. Simple and reliable approach

### pie_familias
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 53
- **Primary Keys**: ic_cod_fam
- **Notes**: Full table replacement recommended. Simple and reliable approach

### pie_subfamilias
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 45
- **Primary Keys**: ic_cod_fam, ic_cod_sub
- **Notes**: Full table replacement recommended. Simple and reliable approach

### pie_subfamilias_dto
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 88
- **Primary Keys**: ic_cod_ofi, ic_cod_fam, ic_cod_sub
- **Notes**: Full table replacement recommended. Simple and reliable approach

### piezas_1
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 75)
- **Rows**: 378,719
- **Watermark**: `fecha_pmp`
- **Primary Keys**: cc_cod_pie
- **Timestamp Columns**: fecha_pmp
- **Notes**: Implement incremental loading using 'fecha_pmp' as watermark column. Query: SELECT * FROM table WHERE fecha_pmp > last_run_timestamp

### piezas_2
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 75)
- **Rows**: 389,345
- **Watermark**: `fecha_pmp`
- **Primary Keys**: cc_cod_pie
- **Timestamp Columns**: fecha_pmp
- **Notes**: Implement incremental loading using 'fecha_pmp' as watermark column. Query: SELECT * FROM table WHERE fecha_pmp > last_run_timestamp

### piezas_4
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 75)
- **Rows**: 318,588
- **Watermark**: `fecha_pmp`
- **Primary Keys**: cc_cod_pie
- **Timestamp Columns**: fecha_pmp
- **Notes**: Implement incremental loading using 'fecha_pmp' as watermark column. Query: SELECT * FROM table WHERE fecha_pmp > last_run_timestamp

### piezas_com
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 2,688
- **Primary Keys**: cc_cod_pie, cc_pie_com
- **Notes**: Full table replacement recommended. Simple and reliable approach

### piezas_ent_pro
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 3
- **Primary Keys**: cc_cod_pie, ic_cod_pro, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### piezas_lin
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 58
- **Primary Keys**: cc_cod_pie, ic_cod_ofi, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### pro
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 52
- **Primary Keys**: ic_cod_pro
- **Notes**: Full table replacement recommended. Simple and reliable approach

### programacion
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 90)
- **Rows**: 5,630
- **Watermark**: `fecha`
- **Primary Keys**: ic_cod_pro, ic_cod_ofi
- **Timestamp Columns**: fecha, fecha1, fecha2, fecha_prevista_entrada, fecha_prevista_salida
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### prueba
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 0
- **Primary Keys**: cc_code
- **Notes**: Full table replacement recommended. Simple and reliable approach

### ra_definicion
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 93
- **Primary Keys**: id
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### rea
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 1,043
- **Primary Keys**: ic_ano, ic_cod_rea, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### rem_lin_his
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 0
- **Primary Keys**: ic_cod_cli, fc_fecha
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### rem_lin_his_
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 0
- **Primary Keys**: ic_cod_cli, fc_fecha
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### rem_lineas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 42,450
- **Primary Keys**: ic_ano, ic_cod_rem, ic_cod_ofi, ic_vto_ano, ic_vto_cod, ic_vto_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### rem_lineas_
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 5,831
- **Primary Keys**: ic_ano, ic_cod_rem, ic_vto_ano, ic_vto_cod, ic_vto_ofi, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### remesas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 9,031
- **Primary Keys**: ic_ano, ic_cod_rem, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### remesas_
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 305
- **Primary Keys**: ic_ano, ic_cod_rem, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### repair_history
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 60)
- **Rows**: 154
- **Primary Keys**: ic_code
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### repair_history_lin
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 1,159
- **Primary Keys**: ic_code, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### seg_accesos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 137,631
- **Primary Keys**: ic_cod_ofi, ic_cod_usu, ic_linea, f_fecha
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_accesos_fichar
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 185
- **Primary Keys**: ic_cod_ofi, ic_cod_usu, ic_linea, f_fecha
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_accesos_planificador
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 9,452
- **Primary Keys**: ic_cod_ofi, ic_cod_usu, ic_linea, f_fecha
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_accesos_tareas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 6,626
- **Primary Keys**: ic_cod_ofi, ic_cod_usu, ic_linea, f_fecha
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_acciones
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 11
- **Primary Keys**: ic_code
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_com_fam
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 14
- **Primary Keys**: ic_cod_fam
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_com_gru
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 230
- **Primary Keys**: ic_cod_com, ic_cod_gru
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_comandos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 69
- **Primary Keys**: ic_cod_com
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_grupos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 7
- **Primary Keys**: ic_cod_gru
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_modulos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 14
- **Primary Keys**: ic_code
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_ofi_tip
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 1
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_oficina
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 4
- **Primary Keys**: ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_token
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 100)
- **Rows**: 3,135
- **Watermark**: `f_fecha_in`
- **Primary Keys**: ic_codigo
- **Timestamp Columns**: f_fecha_in, f_fecha_expired
- **Notes**: Implement incremental loading using 'f_fecha_in' as watermark column. Query: SELECT * FROM table WHERE f_fecha_in > last_run_timestamp

### seg_usu_permisos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 1,741
- **Primary Keys**: ic_tipo, ic_cod_usu, ic_cod_ofi, ic_code
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_usuarios
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 176
- **Primary Keys**: ic_cod_usu, i_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_usuarios_ausencias
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 7
- **Watermark**: `fecha`
- **Primary Keys**: ic_code, ic_cod_usu, ic_cod_ofi
- **Timestamp Columns**: fecha, fecha1, fecha2
- **Notes**: Incremental loading feasible with 'fecha'. Consider data volume and update patterns to decide vs full replace

### seg_usuarios_ausencias_tipos
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 2
- **Primary Keys**: ic_cod_tip
- **Notes**: Full table replacement recommended. Simple and reliable approach

### seg_usuarios_horarios
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 32,724
- **Watermark**: `fecha`
- **Primary Keys**: ic_code, ic_cod_usu, ic_cod_ofi
- **Timestamp Columns**: fecha, fechaini1, fechafin1, fechaini2, fechafin2
- **Notes**: Incremental loading feasible with 'fecha'. Consider data volume and update patterns to decide vs full replace

### soap_price_list
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 75)
- **Rows**: 440,657
- **Watermark**: `f_fecha`
- **Primary Keys**: cc_MaterialNo
- **Timestamp Columns**: f_fecha
- **Notes**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp

### soap_price_list_versiones
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 100)
- **Rows**: 12
- **Watermark**: `f_fecha`
- **Primary Keys**: id
- **Timestamp Columns**: f_fecha
- **Notes**: Implement incremental loading using 'f_fecha' as watermark column. Query: SELECT * FROM table WHERE f_fecha > last_run_timestamp

### soap_service
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 55
- **Primary Keys**: ic_cod_ofi, cc_field
- **Notes**: Full table replacement recommended. Simple and reliable approach

### tareas
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 22,879
- **Watermark**: `fecha`
- **Primary Keys**: ic_cod_tar, ic_cod_ofi, ic_linea
- **Timestamp Columns**: fecha, f_fec_ini, f_fec_fin, f_fec_cierre
- **Notes**: Incremental loading feasible with 'fecha'. Consider data volume and update patterns to decide vs full replace

### tareas_estados
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 0)
- **Rows**: 3
- **Primary Keys**: ic_cod_est
- **Notes**: Full table replacement recommended. Simple and reliable approach

### tareas_tipos
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 49
- **Primary Keys**: ic_cod_tar
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### tareas_tipos_lineas
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 6
- **Primary Keys**: ic_cod_tar, ic_linea
- **Notes**: Full table replacement recommended. Simple and reliable approach

### token_byVIN
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 90)
- **Rows**: 761
- **Watermark**: `fecha`
- **Primary Keys**: ic_codigo, ic_tipo, cc_bastidor
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### tramos_margen
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 15)
- **Rows**: 5
- **Primary Keys**: ic_cod_tra
- **Notes**: Full table replacement recommended. Simple and reliable approach

### veh_tip_con
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 40)
- **Rows**: 4
- **Primary Keys**: ic_cod_tip
- **Notes**: Incremental loading possible but may require custom logic or composite keys

### vehiculos
- **Strategy**: INCREMENTAL_POSSIBLE
- **Confidence**: MEDIUM (Score: 65)
- **Rows**: 13,969
- **Watermark**: `f_last_leido`
- **Primary Keys**: cc_cod_mat, ic_cod_cli, ic_cod_ofi
- **Timestamp Columns**: f_last_leido
- **Notes**: Incremental loading feasible with 'f_last_leido'. Consider data volume and update patterns to decide vs full replace

### vista_movimientos_historicos
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 75)
- **Rows**: 165,018
- **Watermark**: `fecha`
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### vto_cli
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 50,270
- **Primary Keys**: ic_ano, ic_cod_vto, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### vto_cli_fac
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 68,190
- **Primary Keys**: ic_vto_ano, ic_vto_cod, ic_vto_ofi, ic_fac_ano, ic_fac_ofi, ic_fac_let, ic_fac_cod
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### vto_cli_his
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 25)
- **Rows**: 819
- **Primary Keys**: ic_ano, ic_cod_vto, ic_cod_ofi, ic_linea
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

### vto_cli_his_agru
- **Strategy**: INCREMENTAL_PREFERRED
- **Confidence**: HIGH (Score: 85)
- **Rows**: 11,995
- **Watermark**: `fecha`
- **Primary Keys**: ic_ano, ic_cod_vto, ic_cod_ofi, ic_fac_ano, ic_fac_cod, ic_fac_let, ic_fac_ofi, f_fecha, f_fec_ini
- **Timestamp Columns**: fecha
- **Notes**: Implement incremental loading using 'fecha' as watermark column. Query: SELECT * FROM table WHERE fecha > last_run_timestamp

### vto_pro
- **Strategy**: FULL_REPLACE
- **Confidence**: HIGH (Score: 5)
- **Rows**: 16,461
- **Primary Keys**: ic_ano, ic_cod_vto, ic_cod_ofi
- **Notes**: Full table replacement recommended. Simple and reliable approach

### vto_pro_fac
- **Strategy**: INCREMENTAL_CHALLENGING
- **Confidence**: MEDIUM (Score: 20)
- **Rows**: 25,530
- **Primary Keys**: ic_vto_ano, ic_vto_cod, ic_vto_ofi, ic_fac_ano, ic_fac_ofi, ic_fac_let, ic_fac_cod
- **Notes**: Incremental loading technically possible but may be complex. Consider full replace unless table is very large

