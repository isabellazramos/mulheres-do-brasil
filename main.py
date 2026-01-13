#!/usr/bin/env python3

import sys
import logging
from src.etl_pipeline import ETLPipeline

# Configuração de Log aprimorada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    pipeline = ETLPipeline()
    try:
        logger.info("="*70)
        logger.info("INICIANDO PIPELINE ETL: DADOS DEMOGRÁFICOS POR GÊNERO")
        logger.info("="*70)

        # 1. Extração
        dfs = pipeline.extract_all()

        # Verificação da tabela principal (População)
        if dfs['pop'] is None or len(dfs['pop']) == 0:
            logger.error("\nFALHA CRÍTICA: sidrapy não conseguiu extrair dados de população")
            sys.exit(1)
            
        # Logs de status para as demais tabelas
        for key in ['fec', 'life', 'edu']:
            if dfs[key] is None or len(dfs[key]) == 0:
                logger.warning(f"Aviso: Tabela '{key}' retornou vazia ou falhou.")

        # 2. Exibição de Metadados (Audit)
        logger.info(f"\nDados Extraídos com Sucesso:")
        logger.info(f"- População: {len(dfs['pop'])} registros")
        logger.info(f"- Colunas encontradas: {dfs['pop'].columns.tolist()}")
        
        # 3. Salvamento dos arquivos RAW (Aqui ocorre a separação inicial por gênero em CSV)
        pipeline.save_raw(dfs)

        # 4. Transformação com PySpark
        # O Spark tratará os dados e criará as partições de gênero no Parquet
        spark_dfs = pipeline.transform(dfs)

        # 5. Carregamento (Escrita final em Parquet e CSV processado)
        pipeline.load(spark_dfs)

        logger.info("\n" + "="*70)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO")
        logger.info("Localização dos dados:")
        logger.info("- Brutos: data/raw/ (Arquivos separados por gênero)")
        logger.info("- Processados: data/processed/ (Parquet particionado)")
        logger.info("- Entrega: data/output/ (CSVs finais)")
        logger.info("="*70)

    except Exception as e:
        logger.error(f"\nERRO NO PROCESSO PRINCIPAL: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Garante que a sessão do Spark seja encerrada para liberar memória
        pipeline.stop_spark()
        logger.info("Sessão Spark encerrada.")

if __name__ == "__main__":
    main()