#!/usr/bin/env python3

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import sys

from src.extract import (
    get_population_sidrapy,
    clean_dataframe
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("="*70)
    logger.info("ETL COM SIDRAPY - DADOS REAIS DO IBGE")
    logger.info("="*70)
    
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/output", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("ETL-Sidrapy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    try:
        # 1. EXTRACAO - SIDRAPY
        logger.info("\nPASSO 1: EXTRAÇÃO COM SIDRAPY")
        logger.info("-"*70)
        
        df_pop = get_population_sidrapy()
        
        if df_pop is None or len(df_pop) == 0:
            logger.error("\nFALHA: sidrapy não conseguiu extrair dados")
            sys.exit(1)
        
        df_pop = clean_dataframe(df_pop)
        
        logger.info(f"\n{len(df_pop)} registros REAIS do IBGE")
        logger.info(f"Colunas: {df_pop.columns.tolist()}")
        logger.info(f"\n{df_pop.head()}")
        
        # Salvar raw
        df_pop.to_csv("data/raw/ibge_sidrapy.csv", index=False)
        logger.info(f"Salvo: data/raw/ibge_sidrapy.csv")
        
        # 2. TRANSFORMAÇÃO COM SPARK
        logger.info("\nPASSO 2: TRANSFORMAÇÃO COM SPARK")
        logger.info("-"*70)
        
        df_spark = spark.createDataFrame(df_pop)
        logger.info(f"DataFrame: {df_spark.count()} linhas")
        
        df_spark.show(5)
        
        # 3. CARREGAMENTO
        logger.info("\nPASSO 3: CARREGAMENTO")
        logger.info("-"*70)
        
        # Parquet
        df_spark.write.mode("overwrite").parquet("data/processed/ibge.parquet")
        logger.info("Salvo: data/processed/ibge.parquet")
        
        # CSV
        df_spark.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("data/output/ibge.csv")
        logger.info("Salvo: data/output/ibge.csv")
        
        logger.info("\n" + "="*70)
        logger.info("Dados reais do IBGE processados")
        logger.info("="*70)
        
    except Exception as e:
        logger.error(f"\nERRO: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
