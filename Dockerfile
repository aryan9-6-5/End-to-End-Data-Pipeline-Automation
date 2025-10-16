FROM apache/airflow:2.4.2
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow