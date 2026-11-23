import logging as log

log.basicConfig(
    level=log.INFO,
    format="%(asctime)s / %(levelname)s / %(message)s / %(name)s"
)

logger = log.getLogger(__name__)

def main():

    logger.info("Iniciando pipeline de criação da dim_cnaes")

    try:

        logger.info("Lendo tabela silver_cnaes")

        df_silver_cnaes = spark.read.table("workspace.businessradar.silver_cnaes")

        logger.info("Selecionando colunas da dimensão CNAEs")

        df_dim_cnaes = df_silver_cnaes.select( "codigo_cnae", "descricao_cnae")

        logger.info("Colunas selecionadas")

        logger.info("Iniciando gravação da dim_cnaes")

        df_dim_cnaes.write.format("delta").mode("overwrite").saveAsTable( "workspace.businessradar.dim_cnaes")

        logger.info("Dimensão CNAEs criada e salva com sucesso")

    except Exception:
        logger.exception("Falha na criação da dim_cnaes")
        raise

if __name__ == "__main__":
    main()
