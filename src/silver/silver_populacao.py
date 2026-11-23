import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def converter_populacao(df: DataFrame) -> DataFrame: 

    logger.info("Convertendo registro populacionais")

    try:

        df_conversao = (
            df.withColumn("populacao_estimada", F.col("populacao_estimada").cast("long"))
        )

        logger.info("Registros convertidos")
    
    except Exception:
        logger.exception("Erro ao converter registros populacionais")
        raise

    return df_conversao

def criar_codigo_ibge(df: DataFrame) -> DataFrame: 

    logger.info("Criando codigos ibge")

    try:

        df_codigo_ibge = (
            df.withColumn("codigo_ibge", F.concat(F.col("codigo_uf"), F.col("codigo_municipio")))
        )

        logger.info("Codigos criados")
    
    except Exception:
        logger.exception("Erro ao criar codigos ibge")
        raise

    return df_codigo_ibge

def main():

    logger.info("Lendo a tabela bronze_populacao e iniciando transformações")

    df_bronze_estabelecimentos = spark.read.table("workspace.businessradar.bronze_populacao")

    try:

        df_converter_populacao = converter_populacao(df_bronze_estabelecimentos)
        df_silver_populacao = criar_codigo_ibge(df_converter_populacao)

        logger.info("Iniciando gravação da silver_populacao")

        df_silver_populacao.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.silver_populacao")

        logger.info("Transformações concluidas e tabela salva")

    except Exception:
        logger.exception("Falha na tranformação silver população")
        raise

if __name__ == "__main__":
    main()









