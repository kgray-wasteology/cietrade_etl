# deploy.py
from prefect import flow
from datetime import timedelta
from prefect_github import GitHubRepository, GitHubCredentials


if __name__ == "__main__":
    github_token = os.getenv("GITHUB_TOKEN")
    github_url = f"https://{github_token}@github.com/kgray-wasteology/cietrade_etl"

    # Create deployment from source
    flow.from_source(
        source=github_url,
        entrypoint="flows/accounts_flow.py:accounts_update_flow",  # path to flow file and function
    ).deploy(
        name="accounts-update-github",
        work_pool_name="cietrade-process-pool",
        interval=timedelta(hours=24),  # Optional: Set schedule
        parameters={},  # Add any default parameters if needed
        job_variables={"env": {"EXTRA_PIP_PACKAGES": "prefect-github psycopg2-binary"}},
    )
