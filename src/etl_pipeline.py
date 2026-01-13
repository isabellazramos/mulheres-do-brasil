import logging
import os
import pandas as pd
from pyspark.sql import SparkSession
from src.extract import Extractor
from typing import Optional

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.extrator = Extractor()
        self.spark = None
        # Garante as pastas de saída
        for p in ["data/raw", "data/processed", "data/output"]:
            os.makedirs(p, exist_ok=True)

    def extract_all(self):
        logger.info("\nPASSO 1: EXTRAÇÃO COM SIDRAPY")
        logger.info("-" * 70)
        
        # Mapeamento direto com os nomes do Extractor
        return {
            'pop': self.extrator.clean_dataframe(self.extrator.get_population_sidrapy()),
            'fec': self.extrator.clean_dataframe(self.extrator.get_fecundity_rate()),
            'life': self.extrator.clean_dataframe(self.extrator.get_life_expectancy()),
            'edu': self.extrator.clean_dataframe(self.extrator.get_education_illiteracy())
        }

    def save_raw(self, dfs: dict):
        """Salva CSVs brutos separando por sexo quando disponível"""
        logger.info("\nPASSO 1B: SALVANDO DADOS BRUTOS")
        for key, df in dfs.items():
            if df is not None:
                # Se houver coluna Sexo, separa os arquivos
                if 'Sexo' in df.columns:
                    for genero in ['Mulheres', 'Homens']:
                        temp_df = df[df['Sexo'] == genero]
                        if not temp_df.empty:
                            path = f"data/raw/{key}_{genero.lower()}.csv"
                            temp_df.to_csv(path, index=False, encoding='utf-8-sig')
                            logger.info(f"Salvo: {path}")
                else:
                    path = f"data/raw/{key}_geral.csv"
                    df.to_csv(path, index=False, encoding='utf-8-sig')
                    logger.info(f"Salvo: {path}")

    def start_spark(self):
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("ETL-Mulheres-Brasil") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
        return self.spark

    def transform(self, dfs: dict):
        logger.info("\nPASSO 2: TRANSFORMAÇÃO SPARK")
        spark = self.start_spark()
        spark_dfs = {}
        for key, df in dfs.items():
            if df is not None:
                # 1. Converte para string para evitar erros de tipo
                # 2. Resolve colunas duplicadas antes de criar o Spark DataFrame
                df_temp = df.copy()
                cols = pd.Series(df_temp.columns)
                for dup in cols[cols.duplicated()].unique(): 
                    cols[cols[cols == dup].index.values.tolist()] = [
                        dup + '_' + str(i) if i != 0 else dup 
                        for i in range(sum(cols == dup))
                    ]
                df_temp.columns = cols

                # Cria o Spark DataFrame
                sdf = spark.createDataFrame(df_temp.astype(str))
                
                # Opcional: Padronizar nomes de colunas para minúsculas (evita conflitos SQL)
                for col_name in sdf.columns:
                    sdf = sdf.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))

                spark_dfs[key] = sdf
                logger.info(f"Spark DF {key} criado com {sdf.count()} linhas")
        return spark_dfs

    def load(self, spark_dfs: dict):
        logger.info("\nPASSO 3: CARREGAMENTO FINAL")
        
        for key, sdf in spark_dfs.items():
            if sdf is not None:
                base_path = f"data/processed/{key}.parquet"
                # Agora checamos por 'sexo' em minúsculo
                if "sexo" in sdf.columns:
                    sdf.write.mode("overwrite").partitionBy("sexo").parquet(base_path)
                    logger.info(f"Parquet particionado por sexo: {base_path}")
                else:
                    sdf.write.mode("overwrite").parquet(base_path)
                    logger.info(f"Parquet processado: {base_path}")

    def stop_spark(self):
        if self.spark:
            self.spark.stop()
            self.spark = None