import logging
import pandas as pd
from typing import Optional

class Extractor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def extract_sidrapy(self, table_code: str, territorial_level: str = '1', 
                        ibge_territorial_code: str = '1', descricao: str = "", 
                        classification: str = None, variable: str = None, 
                        period: str = None) -> Optional[pd.DataFrame]:
        """Função base para chamadas ao SIDRA"""
        try:
            import sidrapy as sidra
            df = sidra.get_table(
                table_code=table_code,
                territorial_level=territorial_level,
                ibge_territorial_code=ibge_territorial_code,
                classification=classification,
                variable=variable,
                period=period
            )
            if df is not None and len(df) > 1:
                # Ajusta o cabeçalho: a primeira linha da API contém os nomes das colunas
                df.columns = df.iloc[0]
                df = df.drop(df.index[0])
                return df
            return None
        except Exception as e:
            self.logger.error(f"Erro ao extrair {descricao}: {e}")
            return None

    def clean_dataframe(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Limpeza básica de nulos e duplicatas"""
        if df is None: return None
        return df.drop_duplicates().dropna(how='all')

    # --- Métodos chamados pelo ETLPipeline ---

    def get_population_sidrapy(self) -> Optional[pd.DataFrame]:
        """População residente por sexo (Censo 2022)"""
        return self.extract_sidrapy(
            table_code='9514',
            territorial_level='3',
            ibge_territorial_code='all',
            period="2022",
            classification="2/all", 
            descricao="população por sexo"
        )

    def get_fecundity_rate(self) -> Optional[pd.DataFrame]:
        """Taxa de fecundidade (Projeções)"""
        return self.extract_sidrapy(
            table_code='7362',
            territorial_level='3',
            ibge_territorial_code='all',
            period="last",
            descricao="taxa de fecundidade"
        )

    def get_life_expectancy(self) -> Optional[pd.DataFrame]:
        """Esperança de vida ao nascer (Projeções)"""
        return self.extract_sidrapy(
            table_code='7362',
            territorial_level='3',
            variable='2501',
            ibge_territorial_code='all',
            period="last",
            classification="2/all",
            descricao="esperança de vida"
        )

    def get_education_illiteracy(self) -> Optional[pd.DataFrame]:
        """Taxa de alfabetização por sexo (Censo 2022)"""
        return self.extract_sidrapy(
            table_code='9543',
            territorial_level='3',
            ibge_territorial_code='all',
            period="2022",
            classification="2/all",
            descricao="alfabetização"
        )