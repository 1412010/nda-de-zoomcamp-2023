FROM prefecthq/prefect:2.7.7-python3.10

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org

COPY etl_fhv_data_to_gcs.py /opt/prefect/flows/etl_fhv_data_to_gcs.py