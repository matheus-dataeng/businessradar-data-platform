import pyspark.sql.functions as F
from pyspark.sql import DataFrame 
import logging as log 

log.basicConfig(
    level= log.INFO,
    format= "%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def relacionar_empresas_simples(df_empresas: DataFrame, df_simples: DataFrame) -> DataFrame:

    logger.info("Relacionando tabelas silver_empresas e silver_simples")

    try:

        df_join_empresas_simples = df_empresas.join(
            df_simples,
            on= "cnpj_basico",
            how= "left"
        )
    
    
        logger.info("Relacionamento concluido")
    
    except Exception:
        logger.exception("Erro ao relacionar tabelas")
        raise

    return df_join_empresas_simples

def selecionar_colunas(df: DataFrame) -> DataFrame:

    logger.info("Selecionando colunas")

    try:

        df_colunas = (
            df.select(
                "cnpj_basico", 
                "razao_social", 
                "natureza_juridica", 
                "qualificacao_responsavel", 
                "capital_social", 
                "porte_empresa", 
                "opcao_pelo_simples", 
                "data_opcao_simples", 
                "data_exclusao_simples", 
                "opcao_pelo_mei", 
                "data_opcao_mei", 
                "data_exclusao_mei"

            )
        )
        
        logger.info("Colunas selecionadas")
    
    except Exception:
        logger.exception("Erro ao selecionar colunas")
        raise

    return df_colunas

def main():

    logger.info("Lendo tabelas silver_empresas, silver_simples e iniciando criações das metricas")

    try:
        
        df_silver_empresas = spark.read.table("workspace.businessradar.silver_empresas")
        df_silver_simples = spark.read.table("workspace.businessradar.silver_simples")

        df_relacionamento = relacionar_empresas_simples(df_silver_empresas, df_silver_simples)
        df_dim_empresas = selecionar_colunas(df_relacionamento)

        logger.info("Iniciando gravação da dim_empresas")

        df_dim_empresas.write.format("delta").mode("overwrite").saveAsTable("workspace.businessradar.dim_empresas")

        logger.info("Dimensão empresas criada e salva com sucesso")
    
    except Exception:
        logger.exception("Falha na criação da dim_empresas")
        raise

if __name__ == "__main__":
    main()
















