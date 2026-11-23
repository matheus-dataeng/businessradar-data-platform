import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def converter_datas(df: DataFrame) -> DataFrame: 

    logger.info("Iniciando conversão de datas")

    datas = ["data_inicio_atividade", "data_situacao_cadastral", "data_situacao_especial"]

    try:
        df_datas_convertidas = df
        for data in datas:
            df_datas_convertidas = (
                df_datas_convertidas.withColumn(data, F.when(F.col(data) != "0", F.to_date(F.col(data), "yyyyMMdd")).otherwise(F.lit(None)))
            )

        logger.info("Datas convertidas")

    except Exception:
        logger.exception("Erro ao converter datas")
        raise 

    return df_datas_convertidas 

def padronizar_estabelecimentos(df: DataFrame) -> DataFrame: 

    logger.info("Padronizando colunas dos estabelecimentos")

    colunas = [
        "nome_fantasia",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "correio_eletronico"
    ]
    
    try:

        df_padronizado = df

        for coluna in colunas:
            col_limpa = F.regexp_replace(F.trim(F.col(coluna)), r"\\+$", "")

            df_padronizado = (
                df_padronizado.withColumn(
                    coluna,
                    F.when(
                        (col_limpa.isNull()) | (col_limpa == ""), F.lit(None)
                    ).otherwise(col_limpa)
                )
            )

        logger.info("Colunas padronizadas")

    except Exception:
        logger.exception("Erro ao padronizar colunas")
        raise 

    return df_padronizado 

def criar_cnpj_completo(df: DataFrame) -> DataFrame: 

    logger.info("Criando cnpj's completos")

    try:

        df_cnpjs = (
            df.withColumn("cnpj_completo", F.concat(F.col("cnpj_basico"), F.col("cnpj_ordem"), F.col("cnpj_dv")))
        )

        logger.info("cnpj's criados")

    except Exception:
        logger.exception("Erro ao concatenar cnpj's")
        raise 

    return df_cnpjs

def main():

    logger.info("Lendo a tabela bronze_estabelecimentos e iniciando transformações")

    df_bronze_estabelecimentos = spark.read.table("workspace.businessradar.bronze_estabelecimentos")

    try:

        df_conversao_datas = converter_datas(df_bronze_estabelecimentos)
        df_padronizar_estabelecimentos = padronizar_estabelecimentos(df_conversao_datas)
        df_silver_estabelecimentos = criar_cnpj_completo(df_padronizar_estabelecimentos)

        logger.info("Iniciando gravação da silver_estabelecimentos")

        df_silver_estabelecimentos.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.silver_estabelecimentos")

        logger.info("Transformações concluidas e tabela salva")

    except Exception:
        logger.exception("Falha na tranformação silver empresas")
        raise

if __name__ == "__main__":
    main()




































