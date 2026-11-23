import pyspark.sql.functions as F
from pyspark.sql import DataFrame 
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def padronizar_codigo_municipio(df: DataFrame) -> DataFrame:

    logger.info("Padronizando codigo dos municipios")

    try:

        df_codigos = (
            df.withColumn("codigo_municipio", F.lpad(F.col("codigo_municipio"), 4, "0"))
        )

        logger.info("Codigos padronizados")       
    
    except Exception:
        logger.exception("Erro ao padronizar codigos")
        raise

    return df_codigos

def tratar_inconsistencias(df: DataFrame) -> DataFrame:

    logger.info("Tratando inconsistências dos municípios")

    try:

        df_tratado = (
            df.withColumn("codigo_ibge",
            F.when((F.col("codigo_municipio") == "9707") & (F.col("codigo_ibge") == "0"), F.lit(None)).otherwise(F.col("codigo_ibge")))
        )

        dados = [
            ("1182", "5101837", "BOA ESPERANÇA DO NORTE", "Boa Esperança do Norte", "MT", "complemento_manual_validado")
        ]
        
        schema = ["codigo_municipio", "codigo_ibge", "nome_municipio_tom", "nome_municipio_ibge", "uf", "arquivo_origem"]

        df_novo_municipio = spark.createDataFrame(dados, schema= schema)

        df_final = df_tratado.unionByName(df_novo_municipio)

        logger.info("Inconsistencias tratadas")       
    
    except Exception:
        logger.exception("Erro ao tratar inconsistencias")
        raise

    return df_final

def selecionar_colunas(df: DataFrame) -> DataFrame:

    logger.info("Selecionando colunas")

    try:

        df_colunas = (
            df.select(
                "codigo_municipio",
                "codigo_ibge",
                F.coalesce(
                    F.col("nome_municipio_ibge"),
                    F.col("nome_municipio_tom")
                ).alias("nome_municipio"),		
                "uf",	
                "arquivo_origem"	
            )
            
        )

        logger.info("Colunas selecionadas")       
    
    except Exception:
        logger.exception("Erro ao selecionar colunas")
        raise

    return df_colunas

def main():

    logger.info("Lendo a tabela bronze_municipios e iniciando transformações")

    try:

        df_bronze_municipios = spark.read.table("workspace.businessradar.bronze_municipios")

        df_padronizar_codigo_municipio = padronizar_codigo_municipio(df_bronze_municipios)
        df_tratar_inconsistencias = tratar_inconsistencias(df_padronizar_codigo_municipio)
        df_silver_municipios = selecionar_colunas(df_tratar_inconsistencias)

        logger.info("Iniciando gravação da silver_municipios")

        df_silver_municipios.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.silver_municipios")

        logger.info("Transformações concluídas e silver_municipios salva com sucesso")

    except Exception:
        logger.exception("Falha na tranformação silver municipios")
        raise

if __name__ == "__main__":
    main()










