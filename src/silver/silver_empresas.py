from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)
 

def padronizar_empresas(df: DataFrame) -> DataFrame:

    logger.info("Padronizando os dados das empresas")

    try:
        df_padronizado = (
            df.withColumn("razao_social", F.regexp_replace(F.trim(F.col("razao_social")), r"\\$", ""))
        )

    except Exception:
        logger.exception("Erro ao corrigir registros")
        raise 

    return df_padronizado 

def converter_capital_social(df: DataFrame) -> DataFrame:

    logger.info("Convertando tipagem dos dados das empresas")

    try:

        df_conversao = (
            df.withColumn("capital_social", F.regexp_replace(F.col("capital_social"), ',', '.').cast("decimal(18,2)"))
        )

        logger.info("Conversões realizadas")

    except Exception:
        logger.exception("Erro ao converter dados")
        raise 

    return df_conversao


def deduplicar_empresas(df: DataFrame) -> DataFrame:

    logger.info("Iniciando deduplicação de empresas")

    registro_completo = (
        (F.col("razao_social").isNotNull()) &
        (F.col("porte_empresa").isNotNull()) &
        (F.col("natureza_juridica") != "0000")
    )

    try:

        df_deduplicado = (
            df.withColumn("registro_completo", registro_completo)
        )

        janela_empresas = (
            Window
            .partitionBy("cnpj_basico")
            .orderBy(F.col("registro_completo").desc())
        )

        df_deduplicado = (
            df_deduplicado.withColumn("numero_linha", F.row_number().over(janela_empresas))
            .filter(F.col("numero_linha") == 1)
            .drop("registro_completo", "numero_linha")
        )

        logger.info("Deduplicações concluidas")

    except Exception:
        logger.exception("Erro ao deduplicar dados")
        raise     

    return df_deduplicado 

def main(): 
    
    logger.info("Lendo tabela bronze_empresas e iniciando transformações")

    df_bronze_empresas = spark.read.table("workspace.businessradar.bronze_empresas")

    try:

        df_padronizar_empresas = padronizar_empresas(df_bronze_empresas)
        df_converter_capital = converter_capital_social(df_padronizar_empresas)
        df_silver_empresas = deduplicar_empresas(df_converter_capital)
         
        logger.info("Iniciando gravação da silver_empresas")

        df_silver_empresas.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.silver_empresas")

        logger.info("Transformações concluidas e tabela salva")

    except Exception:
        logger.exception("Falha na tranformação silver empresas")
        raise

if __name__ == "__main__":
    main()















