import dlt
from pyspark.sql import functions as F

# -------------------------------------------------------------------------
# 1. CREDIT RISK ANALYSIS (SCR DATA)
# -------------------------------------------------------------------------

@dlt.view(name="scr_raw_data") 
def scr_raw_data():
    return spark.read.parquet('/Volumes/banco-central-dados/bronze/raw_data/scr_data (1).parquet')

@dlt.table(name="total_por_uf_inadimplencia", comment="Aggregated credit metrics by State")
def total_por_uf():
    return (
        dlt.read("scr_raw_data")
        .withColumn("esta_devendo", F.col("carteira_vencida") != 0)
        .groupBy("uf")
        .agg(
            F.sum("carteira_inadimplencia").alias("total_devendo"),
            F.mean("carteira_inadimplencia").alias("media_saldo"),
            F.max("carteira_inadimplencia").alias("max_saldo"),
            F.sum(F.col("esta_devendo").cast("int")).alias("total_devedores")
        )
    )

@dlt.table(name="total_por_porte_inadimplencia", comment="Aggregated credit metrics by Company Size")
def total_por_porte():
    # Reading from view to maintain isolation
    return (
        dlt.read("scr_raw_data")
        .withColumn("esta_devendo", F.col("carteira_vencida") != 0)
        .groupBy("porte")
        .agg(
            F.sum("carteira_inadimplencia").alias("total_devendo"),
            F.avg("carteira_inadimplencia").alias("media_saldo"),
            F.max("carteira_inadimplencia").alias("max_saldo"),
            F.min('carteira_inadimplencia').alias("min_saldo"),
            F.sum(F.col("esta_devendo").cast("int")).alias("total_devedores"),
            F.avg(F.col("esta_devendo").cast("int")).alias("media_devedores"),
            F.min(F.col("esta_devendo").cast("int")).alias("min_devedores"),
            F.max(F.col("esta_devendo").cast("int")).alias("max_devedores"),
        )

    )

# -------------------------------------------------------------------------
# 2. PIX MATURITY ANALYSIS
# -------------------------------------------------------------------------

@dlt.view(name="raw_pix_data")
def raw_pix_data():
    return spark.read.parquet('/Volumes/banco-central-dados/bronze/raw_data/pix_transacoes_municipio.parquet')

@dlt.table(name="total_per_uf_pix", comment="Pix transactions grouped by State")
def total_per_uf_pix():
    return (
        dlt.read("raw_pix_data")
        .groupBy("Regiao", "Estado_Ibge", "Estado") # Keep Regiao here for hierarchical use
        .agg(
            F.sum("VL_PagadorPF").alias("total_pagador"),
            F.sum("VL_RecebedorPF").alias("total_recebedor"),
            F.max('VL_PagadorPF').alias("max_pagador"),
            F.max('VL_RecebedorPF').alias("max_recebedor"),
            F.min('VL_PagadorPF').alias("min_pagador"),
            F.count("VL_PagadorPF").alias("total_transacoes"),
            F.sum("QT_PES_PagadorPF").alias("total_pessoas_pagadoras"),
            F.sum("QT_PES_RecebedorPF").alias("total_pessoas_recebedoras")
        )
    )

@dlt.table(name="total_per_region_pix", comment="Pix transactions aggregated by Region")
def total_per_region():
    # Read from UF table (faster) instead of raw data
    return (
        dlt.read("total_per_uf_pix")
        .groupBy("Regiao")
        .agg(
            F.sum("total_pagador").alias("total_pagador"),
            F.sum("total_recebedor").alias("total_recebedor"),
            F.sum("total_transacoes").alias("total_transacoes"),
            F.sum("total_pessoas_pagadoras").alias("total_pessoas_pagadoras"),
            F.sum("total_pessoas_recebedoras").alias("total_pessoas_recebedoras")
        )
    )

# -------------------------------------------------------------------------
# 3. EDUCATION SCORING ANALYSIS
# -------------------------------------------------------------------------

@dlt.view(name="raw_escolarizacao_data")
def raw_escolarizacao_data():
    return spark.read.parquet('/Volumes/banco-central-dados/bronze/raw_data/taxa_escolarizacao.parquet')

@dlt.table(name="por_estado_escolarizacao", comment="Education indices by State/Level")
def por_estado():
    return (
        dlt.read("raw_escolarizacao_data")
        .groupBy("NN", "D3N") # Assuming D3N or similar is 'Year', adjust if column name is different
        .agg(
            F.sum("V").alias("total_indice"),
            F.mean("V").alias("media_indice"),
            F.max("V").alias("max_indice")
        )
    )

@dlt.table(name="por_ano_escolarizacao", comment="Education indices aggregated by Year")
def por_ano():
    # Aggregating from the State table for performance
    return (
        dlt.read("por_estado_escolarizacao")
        .groupBy("D3N") # Added the likely 'Year' column name
        .agg(
            F.sum("total_indice").alias("total_indice"),
            F.mean("media_indice").alias("media_indice_global")
        
        )
    )




