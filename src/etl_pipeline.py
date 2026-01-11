import logging
import pandas as pd
from pyspark.sql import SparkSession
from src.extract import Extractor
from typing import Optional

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.extrator = Extractor()
        self.spark = None

    def extract_all(self):
        logger.info("\nPASSO 1: EXTRAÇÃO COM SIDRAPY")
        logger.info("-"*70)
        df_pop = self.extrator.get_population_sidrapy()
        df_fec = self.extrator.get_fecundity_rate()
        df_life = self.extrator.get_life_expectancy()
        df_edu = self.extrator.get_education()
        return {
            'pop': self.extrator.clean_dataframe(df_pop),
            'fec': self.extrator.clean_dataframe(df_fec),
            'life': self.extrator.clean_dataframe(df_life),
            'edu': self.extrator.clean_dataframe(df_edu)
        }

    def save_raw(self, dfs: dict):
        logger.info("\nPASSO 1B: SALVANDO RAW")
        if dfs['pop'] is not None:
            dfs['pop'].to_csv("data/raw/ibge_sidrapy.csv", index=False)
            logger.info(f"Salvo: data/raw/ibge_sidrapy.csv")
        if dfs['fec'] is not None:
            dfs['fec'].to_csv("data/raw/ibge_fecundidade.csv", index=False)
            logger.info(f"Salvo: data/raw/ibge_fecundidade.csv")
        if dfs['life'] is not None:
            dfs['life'].to_csv("data/raw/ibge_esperanca_vida.csv", index=False)
            logger.info(f"Salvo: data/raw/ibge_esperanca_vida.csv")
        if dfs['edu'] is not None:
            dfs['edu'].to_csv("data/raw/ibge_escolaridade.csv", index=False)
            logger.info(f"Salvo: data/raw/ibge_escolaridade.csv")

    def start_spark(self):
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("ETL-Sidrapy") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
        return self.spark

    def transform(self, dfs: dict):
        logger.info("\nPASSO 2: TRANSFORMAÇÃO COM SPARK")
        logger.info("-"*70)
        spark = self.start_spark()
        spark_dfs = {}
        for key, df in dfs.items():
            if df is not None:
                spark_df = spark.createDataFrame(df)
                logger.info(f"DataFrame {key}: {spark_df.count()} linhas")
                spark_df.show(5)
                spark_dfs[key] = spark_df
            else:
                spark_dfs[key] = None
        return spark_dfs

    def load(self, spark_dfs: dict):
        logger.info("\nPASSO 3: CARREGAMENTO")
        logger.info("-"*70)
        output_map = {
            'pop': ('ibge', 'população'),
            'fec': ('ibge_fecundidade', 'fecundidade'),
            'life': ('ibge_esperanca_vida', 'esperança de vida'),
            'edu': ('ibge_escolaridade', 'escolaridade')
        }
        for key, spark_df in spark_dfs.items():
            if spark_df is not None:
                base, desc = output_map[key]
                spark_df.write.mode("overwrite").parquet(f"data/processed/{base}.parquet")
                logger.info(f"Salvo: data/processed/{base}.parquet")
                spark_df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .csv(f"data/output/{base}.csv")
                logger.info(f"Salvo: data/output/{base}.csv")

    def stop_spark(self):
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
