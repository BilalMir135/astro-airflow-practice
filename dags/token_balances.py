from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryExecuteQueryOperator
from datetime import datetime
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig


from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from include.utils.gcp_connection import add_gcp_connection

DATASET='token_balances'

@dag(
    dag_id='token_balances',
    start_date=datetime(2023, 11, 1),
    schedule=None,
    catchup=False,
)
def token_balances():
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    query = """
    CREATE OR REPLACE VIEW token_balances.blocks AS
    SELECT *
    FROM `bigquery-public-data.crypto_ethereum.blocks`;
    """

    activate_GCP = PythonOperator(
        task_id='add_gcp_connection_python',
        python_callable=add_gcp_connection,
        provide_context=True,
    )

    create_token_balances_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_token_balances_dataset',
        dataset_id=DATASET,
        gcp_conn_id='gcp',
    )

    create_ref_tabels = BigQueryExecuteQueryOperator(
        task_id ='create_ref_tabels',
        sql = query,
        use_legacy_sql = False,
        gcp_conn_id = 'gcp',
    )

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    chain(
        begin,
        activate_GCP,
        create_token_balances_dataset,
        create_ref_tabels,
        transform,
        end
    )


token_balances()