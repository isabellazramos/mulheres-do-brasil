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
        logger.info("\nPASSO 1: EXTRAÇÃO COM SIDRAPY E DATASUS")
        logger.info("-" * 70)
        
        return {
            'pop': self.extrator.clean_dataframe(self.extrator.get_population_sidrapy()),
            'pop_race': self.extrator.clean_dataframe(self.extrator.get_population_by_race_gender()),
            'fec': self.extrator.clean_dataframe(self.extrator.get_fecundity_rate()),
            'life': self.extrator.clean_dataframe(self.extrator.get_life_expectancy()),  # Apenas sexo
            'edu': self.extrator.clean_dataframe(self.extrator.get_education_illiteracy()),
        }

    def save_raw(self, dfs: dict):
        """Salva CSVs brutos separando por sexo e cor/raça quando disponível"""
        logger.info("\nPASSO 1B: SALVANDO DADOS BRUTOS")
        
        for key, df in dfs.items():
            if df is None or df.empty:
                logger.warning(f"⚠ Pulando {key}: DataFrame vazio")
                continue
                
            # Verifica quais colunas de classificação existem
            has_sexo = 'Sexo' in df.columns
            has_cor_raca = 'Cor ou raça' in df.columns
            
            if has_sexo and has_cor_raca:
                # Separa por sexo e cor/raça
                sexos = df['Sexo'].unique()
                racas = df['Cor ou raça'].unique()
                
                logger.info(f"  {key}: {len(sexos)} sexos × {len(racas)} raças/cores")
                
                for genero in sexos:
                    for raca in racas:
                        temp_df = df[(df['Sexo'] == genero) & (df['Cor ou raça'] == raca)]
                        if not temp_df.empty:
                            genero_clean = genero.lower().replace(' ', '_')
                            raca_clean = raca.lower().replace(' ', '_').replace('ú', 'u')
                            path = f"data/raw/{key}_{genero_clean}_{raca_clean}.csv"
                            temp_df.to_csv(path, index=False, encoding='utf-8-sig')
                            logger.info(f"  ✓ {path} ({len(temp_df)} registros)")
                            
            elif has_sexo:
                # Apenas por sexo
                for genero in df['Sexo'].unique():
                    temp_df = df[df['Sexo'] == genero]
                    if not temp_df.empty:
                        genero_clean = genero.lower().replace(' ', '_')
                        path = f"data/raw/{key}_{genero_clean}.csv"
                        temp_df.to_csv(path, index=False, encoding='utf-8-sig')
                        logger.info(f"  ✓ {path} ({len(temp_df)} registros)")
            else:
                # Sem classificações
                path = f"data/raw/{key}_geral.csv"
                df.to_csv(path, index=False, encoding='utf-8-sig')
                logger.info(f"  ✓ {path} ({len(df)} registros)")

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
            if sdf is None:
                continue
                
            base_path = f"data/processed/{key}.parquet"
            
            # Identifica colunas de partição disponíveis
            partition_cols = []
            if "sexo" in sdf.columns:
                partition_cols.append("sexo")
            if "cor_ou_raça" in sdf.columns:
                partition_cols.append("cor_ou_raça")
            
            # Escreve particionado ou não
            if partition_cols:
                sdf.write.mode("overwrite").partitionBy(*partition_cols).parquet(base_path)
                logger.info(f"  ✓ Parquet particionado por {', '.join(partition_cols)}: {base_path}")
            else:
                sdf.write.mode("overwrite").parquet(base_path)
                logger.info(f"  ✓ Parquet processado: {base_path}")

    def stop_spark(self):
        if self.spark:
            self.spark.stop()
            self.spark = None