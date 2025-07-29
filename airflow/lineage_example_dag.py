from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_provider_openmetadata.lineage.operator import OpenMetadataLineageOperator
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig

default_args = {
    'owner': 'openmetadata',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

openmetadata_config = OpenMetadataConnection(
    hostPort="http://openmetadata.openmetadata.svc.cluster.local:8585/api",
    authProvider="openmetadata",
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken="{{ env('OPENMETADATA_JWT_TOKEN') }}"  # Use environment variable
    )
)

with DAG(
    'lineage_example_dag',
    default_args=default_args,
    description='A DAG to demonstrate OpenMetadataLineageOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['openmetadata', 'lineage'],
) as dag:
    process_data = BashOperator(
        task_id='process_data',
        bash_command='echo "Processing data"',
        outlets={"tables": ["sample_data.ecommerce_db.shopify.dim_address"]},
    )
    lineage_task = OpenMetadataLineageOperator(
        task_id='metadata_lineage',
        server_config=openmetadata_config,
        service_name='my_postgres_service',
        only_keep_dag_lineage=False,
    )
    process_data >> lineage_task