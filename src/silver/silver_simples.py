import pyspark.sql.functions as F
from pyspark.sql import DataFrame 
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def converter_datas(df: DataFrame) -> DataFrame:

    logger.info("Convertendo datas")

    datas = ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"]

    try:

        df_datas_convertidas = df

        for data in datas:
            df_datas_convertidas = (
                df_datas_convertidas.withColumn(data, F.when(F.col(data) != "00000000", F.to_date(data, "yyyyMMdd")).otherwise(F.lit(None)))
            )
        
        logger.info("Datas convertidas")

    except Exception:
        logger.exception("Erro ao converter datas")
        raise 

    return df_datas_convertidas 

def main():

    logger.info("Lendo a tabela bronze_simples e iniciando transformações")

    df_bronze_estabelecimentos = spark.read.table("workspace.businessradar.bronze_simples")

    try:

        df_silver_simples = converter_datas(df_bronze_estabelecimentos)

        logger.info("Iniciando gravação da silver_simples")

        df_silver_simples.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.silver_simples")

        logger.info("Transformações concluidas e tabela salva")

    except Exception:
        logger.exception("Falha na tranformação silver simples")
        raise

if __name__ == "__main__":
    main()

