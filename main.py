#!/usr/bin/env python3

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when
from pyspark.sql.types import StructType, StructField, StringType, LongType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("===== INICIANDO ETL =====")
    
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/output", exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("ETL-Mulheres") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    try:
        # 1. EXTRACAO
        logger.info("PASSO 1: EXTRACAO")
        
        dados = [
            ("SP", 22000000, 18, 78),
            ("MG", 10500000, 17, 77),
            ("RJ", 8500000, 19, 76),
            ("BA", 7500000, 20, 75),
            ("RS", 6500000, 16, 79),
            ("PR", 6200000, 17, 78),
            ("PE", 4800000, 21, 74),
            ("CE", 4200000, 22, 73)
        ]
        
        schema = StructType([
            StructField("estado", StringType()),
            StructField("mulheres", LongType()),
            StructField("fecundidade", LongType()),
            StructField("vida", LongType())
        ])
        
        df = spark.createDataFrame(dados, schema)
        logger.info(f"Extraidos: {df.count()} registros")
        df.show()
        
        # 2. TRANSFORMACAO
        logger.info("PASSO 2: TRANSFORMACAO")
        
        df_clean = df.filter(col("mulheres") > 0)
        
        df_regiao = df_clean.withColumn("regiao",
            when(col("estado").isin("SP", "MG", "RJ"), "Sudeste")
            .when(col("estado").isin("RS", "PR"), "Sul")
            .when(col("estado").isin("BA", "PE", "CE"), "Nordeste")
            .otherwise("Outros")
        )
        
        df_final = df_regiao.groupBy("regiao").agg(
            sum("mulheres").alias("total"),
            avg("fecundidade").alias("fecund"),
            avg("vida").alias("vida_med"),
            count("*").alias("estados")
        ).orderBy("regiao")
        
        df_final.show()
        
        # 3. CARREGAMENTO
        logger.info("PASSO 3: CARREGAMENTO")
        
        df_final.write.mode("overwrite").parquet("data/processed/mulheres.parquet")
        logger.info("Salvo: data/processed/mulheres.parquet")
        
        df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/mulheres.csv")
        logger.info("Salvo: data/output/mulheres.csv")
        
        logger.info("===== SUCESSO! =====")
        
    except Exception as e:
        logger.error(f"ERRO: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
