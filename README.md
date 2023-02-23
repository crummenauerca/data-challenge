# Code-challenge resolution v2 (using Python, Airflow, Docker and psycopg2 lib)
Simple solution to resolve the data-challenge

Note 1: The extracted data will be organized inside the folder named as "data"
Note 2: For simplicity, login credentials are hard-coded in scripts and versioned by Git/GitHub

# How to prepare the simple environment:
- Download and install Docker and Docker Compose from: https://www.docker.com/. A reboot may be required
- Prepare the Airflow, source database and the final database (ports mapped to 5441 and 5442) running:
```
docker-compose up
```
- Download and install Python (with pip and terminal integration) from: https://www.python.org/. A reboot may be required
- Install Apache Airflow running:
```
pip install apache-airflow
```
- Install psycopg2 (lib to connect with PostgreSQL) running:
```
pip install psycopg2
```

# How to run:
- Open your browser and access the Airflow with this url:
```
http://localhost:8080/
```
- Use "airflow" as user and password:
- Access and run "dag_code_challenge"

Please contact-me to know more :)