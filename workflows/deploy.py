from prefect import flow
from prefect_github.repository import GitHubRepository
from accounts_flow import accounts_update_flow
from datetime import timedelta

if __name__ == "__main__":
    # Load the configured GitHub repo block
    github_repo = GitHubRepository.load("cietrade-etl-repo")

    accounts_update_flow.from_source(
        source=github_repo, entrypoint="workflows/accounts_flow.py:accounts_update_flow"
    ).deploy(
        name="accounts-update-flow",
        work_pool_name="cietrade-process-pool",
        interval=timedelta(hours=24),
    )
