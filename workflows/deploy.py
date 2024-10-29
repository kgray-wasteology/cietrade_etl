from prefect import flow
from accounts_flow import accounts_update_flow

if __name__ == "__main__":
    accounts_update_flow.from_source(
        source="https://github.com/kgray-wasteology/cietrade_etl",
        entrypoint="accounts_flow.py:accounts_update_flow",
    ).deploy(name="accounts-update-flow", work_pool_name="cietrade-process-pool")
