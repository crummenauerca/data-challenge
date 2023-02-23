import scripts
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

with DAG('dag_code_challenge',
    start_date = datetime(2023, 2, 22),
    schedule_interval = '@daily', catchup = False,
    template_searchpath='/opt/airflow/sql') as dag:

    extracts_data_from_source_database = PythonOperator (
        task_id = 'extracts_data_from_source_database',
        python_callable = scripts.extracts_data_from_source_database
    )

    extracts_data_from_source_csv_file = PythonOperator (
        task_id = 'extracts_data_from_source_csv_file',
        python_callable = scripts.extracts_data_from_source_csv_file
    )

    creates_final_database_structures = PythonOperator (
        task_id = 'creates_final_database_structures',
        python_callable = scripts.creates_final_database_structures
    )

    inserts_extracted_data_from_source_database_into_final_database = PythonOperator (
        task_id = 'inserts_extracted_data_from_source_database_into_final_database',
        python_callable = scripts.inserts_extracted_data_from_source_database_into_final_database
    )

    inserts_extracted_data_from_source_csv_file_into_final_database = PythonOperator (
        task_id = 'inserts_extracted_data_from_source_csv_file_into_final_database',
        python_callable = scripts.inserts_extracted_data_from_source_csv_file_into_final_database
    )

    creates_referential_integrity_constraints_on_the_final_database = PythonOperator (
        task_id = 'creates_referential_integrity_constraints_on_the_final_database',
        python_callable = scripts.creates_referential_integrity_constraints_on_the_final_database
    )

    gets_sample_of_relationated_data_from_final_database = PythonOperator (
        task_id = 'gets_sample_of_relationated_data_from_final_database',
        python_callable = scripts.gets_sample_of_relationated_data_from_final_database
    )

    extracts_data_from_source_database >> creates_final_database_structures
    extracts_data_from_source_csv_file >> creates_final_database_structures
    creates_final_database_structures >> inserts_extracted_data_from_source_database_into_final_database
    creates_final_database_structures >> inserts_extracted_data_from_source_csv_file_into_final_database
    inserts_extracted_data_from_source_database_into_final_database >> creates_referential_integrity_constraints_on_the_final_database
    inserts_extracted_data_from_source_csv_file_into_final_database >> creates_referential_integrity_constraints_on_the_final_database
    creates_referential_integrity_constraints_on_the_final_database >> gets_sample_of_relationated_data_from_final_database