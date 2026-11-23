from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def silver_cnaes(df_bronze: DataFrame) -> DataFrame:

    logger.info("Iniciando transformações da camada bronze_cnaes")

    try:
        df_silver = (df_bronze.withColumn("descricao_cnae", F.trim(F.col("descricao_cnae"))))

        total_registros = df_silver.count()
        total_colunas = len(df_silver.columns)

        logger.info(f"Transformações realizadas! / Colunas: %s / Linhas: %s", total_colunas, total_registros)

    except Exception:
        logger.exception("Erro na transformação")
        raise

    return df_silver

def main():    

    logger.info("Lendo tabela bronze_cnaes")

    df_bronze_cnaes = spark.read.table("workspace.businessradar.bronze_cnaes")

    try:
        
        df_silver_cnaes = silver_cnaes(df_bronze_cnaes)

        logger.info("Iniciando gravação da silver_cnaes")
        
        df_silver_cnaes.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.silver_cnaes")
        
        df_validacao = spark.read.table("workspace.businessradar.silver_cnaes")

        total_registros = df_validacao.count()
        total_colunas = len(df_validacao.columns)

        logger.info("Validação concluída | Registros: %s | Colunas: %s", total_registros,total_colunas)
        

    except Exception:
        logger.exception("Falha na tranformação silver cnaes")
        raise

if __name__ == "__main__":
    main()



    