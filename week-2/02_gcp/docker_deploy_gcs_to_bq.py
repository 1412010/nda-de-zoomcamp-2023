from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_gcs_to_bq import etl_main_flow

docker_container_block = DockerContainer.load("docker-block")

docker_dep = Deployment.build_from_flow(
    flow=etl_main_flow,
    name='docker-flow-etl-gcs-to-bq',
    infrastructure=docker_container_block
)

if __name__ == "__main__":
    docker_dep.apply()
