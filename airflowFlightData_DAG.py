from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

my_DAG = DAG(
    dag_id = 'FlightData_DAG',
    default_args = {'retries': 1}
)

variable_dict = Variable.get("Env_vars", deserialize_json=True)
PROJECT_ID = variable_dict["PROJECT_ID"] # 'smart-arc-459310-g9'
REGION = variable_dict["REGION"] # 'us-east1'
BUCKET_NAME = variable_dict["BUCKET_NAME"]
ENV_NAME = variable_dict["ENV_NAME"]
PYSPARK_JOB = f'gs://{BUCKET_NAME}/spark-jobs/spark-job-flight.py'

DATASET_NAME = variable_dict["DATASET_NAME"]
TABLE_NAME = variable_dict["TABLE_NAME"]

wait_for_gcs_file = GCSObjectExistenceSensor(
    dag=my_DAG,
    task_id='wait_for_gcs_file',
    bucket=BUCKET_NAME,
    object=f'data/source/{ENV_NAME}/flight_booking.csv',
    poke_interval=30,
    timeout=300,
    mode='poke'    
)

BATCH = {
    "pyspark_batch": {
        "main_python_file_uri": PYSPARK_JOB,
        "args": [
                f"--PROJECT_ID={PROJECT_ID}",
                f"--BUCKET_NAME={BUCKET_NAME}",
                f"--ENV_NAME={ENV_NAME}",
                f"--DATASET_NAME={DATASET_NAME}",
                f"--TABLE_NAME={TABLE_NAME}"
            ]
    },
    "runtime_config": {
        "version": "2.1"
    },
    "environment_config": {
        "execution_config": {
            "service_account": "268628543886-compute@developer.gserviceaccount.com",
            "network_uri": f"projects/{PROJECT_ID}/global/networks/default",
            "subnetwork_uri": f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
        }
    }
}

pyspark_task = DataprocCreateBatchOperator(
    dag = my_DAG,
    task_id="pyspark_task", 
    batch = BATCH,
    batch_id = 'dataprocserverless',
    region = REGION, 
    project_id = PROJECT_ID
)

wait_for_gcs_file >> pyspark_task