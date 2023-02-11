from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from parameterization_flow import etl_parent_flow

github_block = GitHub.load("github-block")

github_dep_instance = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='github-web-to-gcs',
    storage=github_block,
    entrypoint="week-2/02_gcp/parameterization_flow.py:etl_parent_flow"
)

if __name__ == "__main__":
    github_dep_instance.apply()
