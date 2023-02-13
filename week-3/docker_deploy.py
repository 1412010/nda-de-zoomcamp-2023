from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_fhv_data_to_gcs import etl_parent_flow

docker_container_block = DockerContainer.load("docker-block-w3")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='fhv-web-to-gcs-flow',
    infrastructure=docker_container_block
)

if __name__ == "__main__":
    docker_dep.apply()
