FROM prefecthq/prefect:2.7.7-python3.10

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org

COPY 02_gcp /opt/prefect/flows