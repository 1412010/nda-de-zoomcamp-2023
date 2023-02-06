# Week 2: Workflow orchestration with Prefect

+ Install requirements package:  
```
pandas==1.5.2
prefect==2.7.7
prefect-sqlalchemy==0.2.2
prefect-gcp[cloud_storage]==0.2.4
protobuf==4.21.11
pyarrow==10.0.1
pandas-gbq==0.18.1
psycopg2-binary==2.9.5
sqlalchemy==1.4.46
```

+ Run prefect UI:  
```
prefect orion start
```

+ Register GCP block for Prefect:  
```
prefect block register -m prefect_gcp
```

+ Deploy & Apply Prefect flow: 
```
prefect deployment build ./parameterization_flow.py:etl_parent_flow -n "Parameterization Flow"  

prefect deployment apply ./etl_parent_flow-deployment.yaml
```

+ Start the prefect agent to run flow:  
```
prefect agent start  --work-queue "default"
```

+ Set prefect agent to run within local server:  
```
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```


+ Create scheduled deployment via CLI:  
```
prefect deployment build ./parameterization_flow.py:etl_parent_flow -n "Scheduled Parameterization Flow " --cron "0 0
* * *" -a
```

+ Build docker for prefect:  
```
docker image build -t ngducanh1611/prefect:nda-zoomcamp .
```

+ Publish docker image to Docker Hub:  
```
docker image push ngducanh1611/prefect:nda-zoomcamp
```

+ Run flow in docker container:  
```
prefect deployment run <deployed flow name> -p "months=[1,2]"
```