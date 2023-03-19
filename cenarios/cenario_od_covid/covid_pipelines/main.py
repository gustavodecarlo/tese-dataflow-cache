import logging

import typer

from pipeline import (
    pipeline_covid_raw_ingest,
    pipeline_containment_dummy_raw_ingest,
    pipeline_cleaned_covid_with_filter,
    pipeline_cleaned_containment_dummy_with_filter,
    pipeline_check_containment
)
from spark_conf import setup_spark, setup_spark_only_cassandra, ENV_CONFIG

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
    print(f'env vars: {ENV_CONFIG}')
    spark_session = setup_spark(
        app_name='covid_raw',
        cassandra_user=ENV_CONFIG.get('cassandra_user'),
        cassandra_password=ENV_CONFIG.get('cassandra_password'),
        cassandra_host=ENV_CONFIG.get('cassandra_host'),
        cassandra_port=ENV_CONFIG.get('cassandra_port'),
    )

    pipeline_covid_raw_ingest(
        spark_session=spark_session,
        bucket=ENV_CONFIG.get('google_bucket'),
        warehouse=ENV_CONFIG.get('warehouse'),
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
    read_datafrag: str = typer.Option(
        "",
        help="Google Storage URL to get covid raw data",
    )
):
    spark_session = setup_spark(
        app_name='covid_cleaned_agg',
        cassandra_user=ENV_CONFIG.get('cassandra_user'),
        cassandra_password=ENV_CONFIG.get('cassandra_password'),
        cassandra_host=ENV_CONFIG.get('cassandra_host'),
        cassandra_port=ENV_CONFIG.get('cassandra_port'),
    )

    pipeline_cleaned_covid_with_filter(
        spark_session=spark_session,
        bucket=ENV_CONFIG.get('google_bucket'),
        warehouse=ENV_CONFIG.get('warehouse'),
        source_data=source_data,
        destiny_table=table,
        have_datafrag=have_datafrag,
        filter=filter,
        datafrag_keysapce=ENV_CONFIG.get('datafrag_keyspace'),
        datafrag_metatable=ENV_CONFIG.get('datafrag_metatable'),
        datafrag_tc_metatable=ENV_CONFIG.get('datafrag_tc_metatable'),
        datafrag_warehouse=ENV_CONFIG.get('datafrag_warehouse'),
        read_datafrag=read_datafrag
    )
    return 0

@app.command()
def containment_dummy_raw(
    source_data: str = typer.Option(
        ...,
        help= 'URL to get dummy csv files',
    ),
    table: str = typer.Option(
        ...,
        help='Destiny table of dummy raw data',
    ),
):
    print(f'env vars: {ENV_CONFIG}')
    spark_session = setup_spark(
        app_name='covid_raw',
        cassandra_user=ENV_CONFIG.get('cassandra_user'),
        cassandra_password=ENV_CONFIG.get('cassandra_password'),
        cassandra_host=ENV_CONFIG.get('cassandra_host'),
        cassandra_port=ENV_CONFIG.get('cassandra_port'),
    )

    pipeline_containment_dummy_raw_ingest(
        spark_session=spark_session,
        bucket=ENV_CONFIG.get('google_bucket'),
        warehouse=ENV_CONFIG.get('warehouse'),
        source_data=source_data,
        destiny_table=table
    )
    return 0

@app.command()
def cleaned_and_filter_containment(
    source_data: str = typer.Option(
        ...,
        help="URL to get dummy csv files",
    ),
    table: str = typer.Option(
        ...,
        help="Destiny table of dummy cleaned and filtered data",
    ),
    have_datafrag: bool = typer.Option(
        False,
        '--have-datafrag',
        help="Save datafragment",
    ),
    filter: str = typer.Option(
        ...,
        help="Filter the data",
    ),
    read_datafrag: str = typer.Option(
        "",
        help="Google Storage URL to get covid raw data",
    )
):
    spark_session = setup_spark(
        app_name='cleaned_and_filter_containment',
        cassandra_user=ENV_CONFIG.get('cassandra_user'),
        cassandra_password=ENV_CONFIG.get('cassandra_password'),
        cassandra_host=ENV_CONFIG.get('cassandra_host'),
        cassandra_port=ENV_CONFIG.get('cassandra_port'),
    )

    pipeline_cleaned_containment_dummy_with_filter(
        spark_session=spark_session,
        bucket=ENV_CONFIG.get('google_bucket'),
        warehouse=ENV_CONFIG.get('warehouse'),
        source_data=source_data,
        destiny_table=table,
        have_datafrag=have_datafrag,
        filter=filter,
        datafrag_keysapce=ENV_CONFIG.get('datafrag_keyspace'),
        datafrag_metatable=ENV_CONFIG.get('datafrag_metatable'),
        datafrag_tc_metatable=ENV_CONFIG.get('datafrag_tc_metatable'),
        datafrag_warehouse=ENV_CONFIG.get('datafrag_warehouse'),
        read_datafrag=read_datafrag
    )
    return 0

@app.command()
def test_containment(
    source_data: str = typer.Option(
        ...,
        help="URL to get dummy csv files",
    ),
    filter: str = typer.Option(
        ...,
        help="Filter the data",
    ),
):
    spark_session = setup_spark_only_cassandra(
        app_name='covid_cleaned_agg',
        cassandra_user=ENV_CONFIG.get('cassandra_user'),
        cassandra_password=ENV_CONFIG.get('cassandra_password'),
        cassandra_host=ENV_CONFIG.get('cassandra_host'),
        cassandra_port=ENV_CONFIG.get('cassandra_port'),
    )

    pipeline_check_containment(
        spark_session=spark_session,
        source_data=source_data,
        filter=filter,
        datafrag_keysapce=ENV_CONFIG.get('datafrag_keyspace'),
        datafrag_metatable=ENV_CONFIG.get('datafrag_metatable'),
        datafrag_tc_metatable=ENV_CONFIG.get('datafrag_tc_metatable'),
        datafrag_warehouse=ENV_CONFIG.get('datafrag_warehouse'),
    )
    return 0



if __name__ == "__main__":
    app()
