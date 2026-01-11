#!/usr/bin/env python3


import os
import sys
import logging
from src.etl_pipeline import ETLPipeline


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    pipeline = ETLPipeline()
    try:
        dfs = pipeline.extract_all()
        if dfs['pop'] is None or len(dfs['pop']) == 0:
            logger.error("\nFALHA: sidrapy não conseguiu extrair dados de população")
            sys.exit(1)
        if dfs['fec'] is None or len(dfs['fec']) == 0:
            logger.warning("\nAviso: Não foi possível extrair taxa de fecundidade")
        if dfs['life'] is None or len(dfs['life']) == 0:
            logger.warning("\nAviso: Não foi possível extrair esperança de vida")
        if dfs['edu'] is None or len(dfs['edu']) == 0:
            logger.warning("\nAviso: Não foi possível extrair escolaridade")

        logger.info(f"\n{len(dfs['pop'])} registros REAIS do IBGE (população)")
        logger.info(f"Colunas: {dfs['pop'].columns.tolist()}")
        logger.info(f"\n{dfs['pop'].head()}")

        pipeline.save_raw(dfs)
        spark_dfs = pipeline.transform(dfs)
        pipeline.load(spark_dfs)

        logger.info("\n" + "="*70)
        logger.info("Dados reais do IBGE processados")
        logger.info("="*70)
    except Exception as e:
        logger.error(f"\nERRO: {e}", exc_info=True)
        sys.exit(1)
    finally:
        pipeline.stop_spark()

if __name__ == "__main__":
    main()
