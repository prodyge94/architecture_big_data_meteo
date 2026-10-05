-- Une instance PostgreSQL, trois bases : métadonnées Airflow, backend MLflow, serving des prévisions.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
CREATE USER mlflow WITH PASSWORD 'mlflow';
CREATE DATABASE mlflow OWNER mlflow;
CREATE USER meteo WITH PASSWORD 'meteo';
CREATE DATABASE meteo OWNER meteo;
