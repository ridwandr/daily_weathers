# etl_deployment.py

from prefect import flow
from prefect_github import GitHubRepository

if __name__ == "__main__":
    github_repository_block = GitHubRepository.load("weather-repo")

    flow.from_source(
        source=github_repository_block,
        entrypoint="etl_main.py:run_pipeline",
    ).deploy(
        name="owm-etl-deployment",
        work_pool_name="etl-worker"
    )
