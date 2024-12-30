from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, 
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

cluster_config = {
    "Name": "crime_analysis_cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
    "LogUri": "s3://crime-data/logs/",
    "VisibleToAllUsers": False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "MasterNode",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "CoreNode",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        
        "Ec2SubnetId": "subnet-1234567890abcdef",
        "Ec2KeyName": "crime-analysis-keypair",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, 
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

extraction_steps = [
    {
        "Name": "Data Extraction",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://us-east-2.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": [
                "s3://crime-data/scripts/data_ingestion.sh",
            ],
        },
    },
]

transformation_steps = [
    {
        "Name": "Data Transformation",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
            "s3://crime-data/scripts/data_transformation.py",
            ],
        },
    },
]

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 26), 
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

with DAG('crime_data_emr_pipeline',
        default_args=default_args,
        catchup=False) as dag:

        start_task = DummyOperator(task_id="start_pipeline")
        
        create_cluster = EmrCreateJobFlowOperator(
            task_id="create_emr_cluster",
            job_flow_overrides=cluster_config,
        )

        cluster_creation_sensor = EmrJobFlowSensor(
        task_id="wait_for_cluster", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states={"WAITING"},  
        timeout=3600,
        poke_interval=5,
        mode='poke',
        )

        add_extraction = EmrAddStepsOperator(
        task_id="add_extraction_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=extraction_steps,
        )

        extraction_sensor = EmrStepSensor(
        task_id="wait_for_extraction", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_extraction_step')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=5,
        )

        add_transformation = EmrAddStepsOperator(
        task_id="add_transformation_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=transformation_steps,
        )

        transformation_sensor = EmrStepSensor(
        task_id="wait_for_transformation", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_transformation_step')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=10,
        )

        terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        )

        cluster_termination_sensor = EmrJobFlowSensor(
        task_id="wait_for_termination", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states={"TERMINATED"},
        timeout=3600,
        poke_interval=5,
        mode='poke',
        )

        end_task = DummyOperator(task_id="end_pipeline")

        start_task >> create_cluster >> cluster_creation_sensor >> add_extraction >> extraction_sensor
        extraction_sensor >> add_transformation >> transformation_sensor >> terminate_cluster
        terminate_cluster >> cluster_termination_sensor >> end_task