import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import logging as log 

log.basicConfig(
    level= log.INFO,  
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"

)

logger = log.getLogger(__name__)

def selecionar_colunas(df: DataFrame) -> DataFrame:

    logger.info("Selecionando colunas")

    try:

        df_colunas = (
            df.select(
                "cnpj_completo",
                "cnpj_basico",
                "municipio",	
                "uf",	
                "cnae_fiscal_principal",	
                "situacao_cadastral",	
                "identificador_matriz_filial",
                "data_inicio_atividade",
                "data_situacao_cadastral"
            )
        )

        logger.info("Colunas selecionadas")
    
    except Exception:
        logger.exception("Erro ao selecionar colunas")
        raise

    return df_colunas

def criar_indicadores(df: DataFrame) -> DataFrame:

    logger.info("Criando indicadores")

    try:

        df_indicadores = (
            df.withColumn("qtd_estabelecimentos", F.lit(1))
            .withColumn("flag_ativo", F.when(F.col("situacao_cadastral") == "02", F.lit(1)).otherwise(F.lit(0)))
            .withColumn("flag_matriz", F.when(F.col("identificador_matriz_filial") == "1", F.lit(1)).otherwise(F.lit(0)))
        )

        logger.info("Indicadores criados")
    
    except Exception:
        logger.exception("Erro ao criar indicadores")
        raise

    return df_indicadores

def main(): 

    logger.info("Lendo a tabela silver_estabelecimentos e iniciando criações das metricas")

    df_silver_estabelecimentos = spark.read.table("workspace.businessradar.silver_estabelecimentos")

    try:

        df_selecionar_colunas = selecionar_colunas(df_silver_estabelecimentos)
        df_fato_estabelecimentos = criar_indicadores(df_selecionar_colunas)

        logger.info("Iniciando gravação da fato_estabelecimentos")

        df_fato_estabelecimentos.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.fato_estabelecimentos")

        logger.info("Métricas criadas e tabela salva")
    
    except Exception:
        logger.exception("Falha na criação da fato_estabelecimentos")
        raise

if __name__ == "__main__":
    main()









