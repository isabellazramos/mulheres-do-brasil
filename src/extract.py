import logging
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)


def extract_sidrapy(table_code: str, territorial_level: str = '1', ibge_territorial_code: str = '1', descricao: str = "") -> Optional[pd.DataFrame]:
    """
    Função genérica para extrair dados do IBGE via sidrapy
    """
    try:
        import sidrapy as sidra
        if descricao:
            logger.info(f"📦 Extraindo {descricao} (IBGE)...")
        df = sidra.get_table(
            table_code=table_code,
            territorial_level=territorial_level,
            ibge_territorial_code=ibge_territorial_code
        )
        if df is not None and len(df) > 0:
            logger.info(f"✅ {descricao if descricao else table_code}: {len(df)} registros")
            return df
        logger.warning(f"Nenhum dado encontrado para {descricao if descricao else table_code}")
        return None
    except Exception as e:
        logger.error(f"❌ Erro ao extrair {descricao if descricao else table_code}: {e}")
        return None

def get_population_sidrapy() -> Optional[pd.DataFrame]:
    """População por estado (IBGE real)"""
    return extract_sidrapy(
        table_code='6579',
        territorial_level='3',
        ibge_territorial_code='all',
        descricao="população por estado"
    )

def get_fecundity_rate() -> Optional[pd.DataFrame]:
    """Taxa de fecundidade por mulher (dados reais IBGE)"""
    return extract_sidrapy(
        table_code='8276',
        territorial_level='1',
        ibge_territorial_code='1',
        descricao="taxa de fecundidade"
    )

def get_life_expectancy() -> Optional[pd.DataFrame]:
    """Esperança de vida ao nascer (dados reais IBGE)"""
    return extract_sidrapy(
        table_code='7362',
        territorial_level='1',
        ibge_territorial_code='1',
        descricao="esperança de vida"
    )

def get_education() -> Optional[pd.DataFrame]:
    """Escolaridade (dados reais IBGE)"""
    return extract_sidrapy(
        table_code='9074',
        territorial_level='1',
        ibge_territorial_code='1',
        descricao="escolaridade"
    )

def clean_dataframe(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Remove duplicatas e linhas inválidas"""
    if df is None:
        return None
    
    df_clean = df.drop_duplicates()
    logger.info(f"Limpeza: {len(df)} → {len(df_clean)} registros")
    return df_clean if len(df_clean) > 0 else None
