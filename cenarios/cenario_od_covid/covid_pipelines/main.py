import logging

import typer

from pipeline import (
    pipeline_covid_raw_ingest,
    pipeline_cleaned_covid_with_filter,
    pipeline_enrich_cleaned_covid_data
)
from spark_conf import setup_spark, ENV_CONFIG

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s',
    level='DEBUG'
)

app = typer.Typer()

@app.command()
def covid_raw(
    source_data: str = typer.Option(
        ...,
        help="Google Storage URL to get covid csv files",
    ),
    table: str = typer.Option(
        ...,
        help="Destiny table of covid raw data",
    ),
):
    spark_session = setup_spark(
        app_name='covid_raw',
        cassandra_user=ENV_CONFIG['cassandra_user'],
        cassandra_password=ENV_CONFIG['cassandra_password'],
        cassandra_host=ENV_CONFIG['cassandra_host'],
        cassandra_port=ENV_CONFIG['cassandra_port'],
    )

    pipeline_covid_raw_ingest(
        spark_session=spark_session,
        bucket=ENV_CONFIG['google_bucket'],
        warehouse=ENV_CONFIG['warehouse'],
        source_data=source_data,
        destiny_table=table
    )
    return 0

@app.command()
def cleaned_and_agg_covid(
    source_data: str = typer.Option(
        ...,
        help="Google Storage URL to get covid raw data",
    ),
    table: str = typer.Option(
        ...,
        help="Destiny table of covid raw data",
    ),
    have_datafrag: bool = typer.Option(
        False,
        '--have-datafrag',
        help="Save datafragment",
    ),
    filter: str = typer.Option(
        ...,
        help="Filter the data of covid",
    ),
):
    spark_session = setup_spark(
        app_name='covid_cleaned_agg',
        cassandra_user=ENV_CONFIG['cassandra_user'],
        cassandra_password=ENV_CONFIG['cassandra_password'],
        cassandra_host=ENV_CONFIG['cassandra_host'],
        cassandra_port=ENV_CONFIG['cassandra_port'],
    )

    pipeline_cleaned_covid_with_filter(
        spark_session=spark_session,
        bucket=ENV_CONFIG['google_bucket'],
        warehouse=ENV_CONFIG['warehouse'],
        source_data=source_data,
        destiny_table=table,
        have_datafrag=have_datafrag,
        filter=filter,
        datafrag_keysapce=ENV_CONFIG['datafrag_keyspace'],
        datafrag_metatable=ENV_CONFIG['datafrag_metatable'],
        datafrag_warehouse=ENV_CONFIG['datafrag_warehouse']
    )
    return 0

# @app.command()
# def cor_precipitation_refined(
#     table_raw: str = typer.Option(
#         ...,
#         help="Table cor precipitation table raw data",
#     ),
#     metadata_table: str = typer.Option(
#         ...,
#         help="Table of metadata cor estations",
#     ),
# ):
#     spark_session = setup_spark(
#         app_name="corprepicitationrefined",   
#     )

#     pipeline_cor_precipitation_enrich_ingest(
#         spark_session=spark_session,
#         warehouse=ENV_CONFIG['warehouse'],
#         table_raw=table_raw,
#         metadata_table=metadata_table
#     )
#     return 0

if __name__ == "__main__":
    app()
