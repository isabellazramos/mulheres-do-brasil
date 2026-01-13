import logging
import pandas as pd
from typing import Optional

class Extractor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def extract_sidrapy(self, table_code: str, territorial_level: str = '1',
                        ibge_territorial_code: str = '1', descricao: str = "",
                        classifications: dict = None, variable: str = None,
                        period: str = None) -> Optional[pd.DataFrame]:
        """Função base para chamadas ao SIDRA com suporte a múltiplas classificações"""
        try:
            import sidrapy as sidra
            
            params = {
                'table_code': table_code,
                'territorial_level': territorial_level,
                'ibge_territorial_code': ibge_territorial_code,
                'period': period
            }
            
            # Adiciona parâmetros opcionais apenas se fornecidos
            if classifications:
                params['classifications'] = classifications
            if variable:
                params['variable'] = variable
            
            df = sidra.get_table(**params)
            
            if df is not None and len(df) > 1:
                df.columns = df.iloc[0]
                df = df.drop(df.index[0])
                self.logger.info(f"✓ {descricao}: {len(df)} registros extraídos")
                return df
            
            self.logger.warning(f"⚠ {descricao}: retornou vazio")
            return None
            
        except Exception as e:
            self.logger.error(f"✗ Erro ao extrair {descricao}: {e}")
            return None
    
    def clean_dataframe(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Limpeza básica de nulos e duplicatas"""
        if df is None: 
            return None
        return df.drop_duplicates().dropna(how='all')
    
    # --- Métodos chamados pelo ETLPipeline ---
    
    def get_population_by_race_gender(self) -> Optional[pd.DataFrame]:
        """População residente por sexo e cor/raça (Censo 2022) - Tabela 9606"""
        return self.extract_sidrapy(
            table_code='9606',
            territorial_level='3',  # Municípios
            ibge_territorial_code='all',
            period="2022",
            classifications={
                "2": "all",    # Sexo
                "86": "all"    # Cor ou raça
            },
            descricao="população por sexo e cor/raça"
        )
    
    def get_population_sidrapy(self) -> Optional[pd.DataFrame]:
        """População residente por sexo (Censo 2022) - Tabela 9514"""
        return self.extract_sidrapy(
            table_code='9514',
            territorial_level='3',
            ibge_territorial_code='all',
            period="2022",
            classifications={"2": "all"},  # Apenas sexo
            descricao="população por sexo"
        )
    
    def get_fecundity_rate(self) -> Optional[pd.DataFrame]:
        """Taxa de fecundidade (Projeções) - Tabela 7362"""
        return self.extract_sidrapy(
            table_code='7362',
            territorial_level='3',
            ibge_territorial_code='all',
            period="last",
            descricao="taxa de fecundidade"
        )
    
    def get_life_expectancy(self) -> Optional[pd.DataFrame]:
        """Esperança de vida ao nascer por sexo (Projeções) - Tabela 7362"""
        return self.extract_sidrapy(
            table_code='7362',
            territorial_level='3',
            ibge_territorial_code='all',
            period="last",
            classifications={"2": "all"}, 
            descricao="esperança de vida por sexo"
        )


    def get_education_illiteracy(self) -> Optional[pd.DataFrame]:
        """Taxa de alfabetização por sexo (Censo 2022) - Tabela 9543"""
        return self.extract_sidrapy(
            table_code='9543',
            territorial_level='3',
            ibge_territorial_code='all',
            period="2022",
            classifications={"2": "all"},  # Apenas sexo
            descricao="alfabetização"
        )