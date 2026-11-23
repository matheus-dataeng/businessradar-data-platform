import pyspark.sql.functions as F
from pyspark.sql import DataFrame 
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def main():

    logger.info("Iniciando pipeline de criação da dim_localizacao")

    try:

        logger.info("Lendo tabela silver_municipios")

        df_silver_localizacao = spark.read.table("workspace.businessradar.silver_municipios")

        logger.info("Selecionando colunas da dimensão localização")

        df_dim_localizacao = df_silver_localizacao.select("codigo_municipio", "codigo_ibge", "nome_municipio", "uf")

        logger.info("Colunas selecionadas")

        logger.info("Iniciando gravação da dim_localizacao")

        df_dim_localizacao.write.format("delta").mode("overwrite").saveAsTable( "workspace.businessradar.dim_localizacao")

        logger.info("Dimensão localização criada e salva com sucesso")

    except Exception:
        logger.exception("Falha na criação da dim_localizacao")
        raise

if __name__ == "__main__":
    main()
