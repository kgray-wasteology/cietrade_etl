from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
import psycopg2
import requests
from psycopg2 import sql
from datetime import datetime
from typing import Dict, List, Optional, Tuple


@task(retries=3, retry_delay_seconds=30)
def get_db_connection() -> psycopg2.extensions.connection:
    """Create and return a database connection using stored credentials."""
    logger = get_run_logger()
    logger.info("Connecting to database")
    db_config = Secret.load("wasteology-db-credentials").value
    conn = psycopg2.connect(**db_config)
    logger.info("Successfully connected to database")
    return conn


@task(retries=2)
def get_last_run_timestamp(conn: psycopg2.extensions.connection) -> Optional[datetime]:
    """Get the timestamp of the last successful run."""
    logger = get_run_logger()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_run FROM cietrade.etl_log WHERE process_name = 'accounts_update' ORDER BY last_run DESC LIMIT 1"
        )
        result = cur.fetchone()
        logger.info(f"Last run timestamp: {result[0] if result else None}")
        return result[0] if result else None


@task(retries=3, retry_delay_seconds=60)
def fetch_data_from_api(last_run: Optional[datetime]) -> List[Dict]:
    """Fetch data from the API with retry logic."""
    logger = get_run_logger()
    logger.info("Fetching data from API")

    # Load API credentials from Secret block
    api_creds = Secret.load("cietrade-api-credentials").value

    params = {
        "UserID": api_creds["UserID"],
        "UserPwd": api_creds["UserPwd"],
        "Account": "",
        "Role": "",
    }

    if last_run:
        params["UpdatedSince"] = last_run.strftime("%Y-%m-%dT%H:%M:%S")

    response = requests.get("https://api.cietrade.net/ListAccounts", params=params)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Successfully fetched {len(data)} records from API")
    return data


@task
def ensure_unique_constraint(conn: psycopg2.extensions.connection) -> None:
    """Ensure the account_id column has a unique constraint."""
    logger = get_run_logger()
    with conn.cursor() as cur:
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'accounts_account_id_key' AND conrelid = 'cietrade.accounts'::regclass
                ) THEN
                    ALTER TABLE cietrade.accounts ADD CONSTRAINT accounts_account_id_key UNIQUE (account_id);
                END IF;
            END $$;
            """
        )
    logger.info("Unique constraint check completed")


@task
def upsert_records(
    conn: psycopg2.extensions.connection, data: List[Dict]
) -> Tuple[int, int]:
    """Upsert records into the database."""
    logger = get_run_logger()
    logger.info("Starting data upsert")

    insert_stmt = sql.SQL(
        """
        INSERT INTO cietrade.accounts (
            account_id, account_name, role_name, payment_terms, active_status,
            invoice_note, currency_code, reference_id, supplier_glacct, expense_glacct,
            credit_limit, primary_contact, primary_email, billing_contact, billing_email,
            address_line_1, address_line_2, address_line_3, city, region, postal, country,
            last_updated
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (account_id) DO UPDATE SET
            account_name = EXCLUDED.account_name,
            role_name = EXCLUDED.role_name,
            payment_terms = EXCLUDED.payment_terms,
            active_status = EXCLUDED.active_status,
            invoice_note = EXCLUDED.invoice_note,
            currency_code = EXCLUDED.currency_code,
            reference_id = EXCLUDED.reference_id,
            supplier_glacct = EXCLUDED.supplier_glacct,
            expense_glacct = EXCLUDED.expense_glacct,
            credit_limit = EXCLUDED.credit_limit,
            primary_contact = EXCLUDED.primary_contact,
            primary_email = EXCLUDED.primary_email,
            billing_contact = EXCLUDED.billing_contact,
            billing_email = EXCLUDED.billing_email,
            address_line_1 = EXCLUDED.address_line_1,
            address_line_2 = EXCLUDED.address_line_2,
            address_line_3 = EXCLUDED.address_line_3,
            city = EXCLUDED.city,
            region = EXCLUDED.region,
            postal = EXCLUDED.postal,
            country = EXCLUDED.country,
            last_updated = CURRENT_TIMESTAMP
        WHERE
            cietrade.accounts.account_name != EXCLUDED.account_name OR
            cietrade.accounts.role_name != EXCLUDED.role_name OR
            cietrade.accounts.payment_terms != EXCLUDED.payment_terms OR
            cietrade.accounts.active_status != EXCLUDED.active_status OR
            cietrade.accounts.invoice_note != EXCLUDED.invoice_note OR
            cietrade.accounts.currency_code != EXCLUDED.currency_code OR
            cietrade.accounts.reference_id != EXCLUDED.reference_id OR
            cietrade.accounts.supplier_glacct != EXCLUDED.supplier_glacct OR
            cietrade.accounts.expense_glacct != EXCLUDED.expense_glacct OR
            cietrade.accounts.credit_limit != EXCLUDED.credit_limit OR
            cietrade.accounts.primary_contact != EXCLUDED.primary_contact OR
            cietrade.accounts.primary_email != EXCLUDED.primary_email OR
            cietrade.accounts.billing_contact != EXCLUDED.billing_contact OR
            cietrade.accounts.billing_email != EXCLUDED.billing_email OR
            cietrade.accounts.address_line_1 != EXCLUDED.address_line_1 OR
            cietrade.accounts.address_line_2 != EXCLUDED.address_line_2 OR
            cietrade.accounts.address_line_3 != EXCLUDED.address_line_3 OR
            cietrade.accounts.city != EXCLUDED.city OR
            cietrade.accounts.region != EXCLUDED.region OR
            cietrade.accounts.postal != EXCLUDED.postal OR
            cietrade.accounts.country != EXCLUDED.country
        RETURNING account_id, (xmax = 0) AS inserted
    """
    )

    inserted_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for item in data:
            cur.execute(
                insert_stmt,
                (
                    item["account_id"],
                    item["account_name"],
                    item["role_name"],
                    item["payment_terms"],
                    item["active_status"],
                    item["invoice_note"],
                    item["currency_code"],
                    item["reference_id"],
                    item["supplier_GLAcct"],
                    item["expense_GLAcct"],
                    item.get("credit_limit", 0),
                    item["primary_contact"],
                    item["primary_email"],
                    item["billing_contact"],
                    item["billing_email"],
                    item["address_line_1"],
                    item["address_line_2"],
                    item["address_line_3"],
                    item["city"],
                    item["region"],
                    item["postal"],
                    item["country"],
                ),
            )
            result = cur.fetchone()
            if result:
                account_id, inserted = result
                if inserted:
                    inserted_count += 1
                    logger.info(f"Inserted new record for account_id: {account_id}")
                else:
                    updated_count += 1
                    logger.info(f"Updated existing record for account_id: {account_id}")
            else:
                logger.warning(
                    f"No result returned for account_id: {item['account_id']}"
                )

    logger.info(
        f"Upsert completed. Inserted: {inserted_count}, Updated: {updated_count}"
    )
    return inserted_count, updated_count


@task
def update_last_run_timestamp(conn: psycopg2.extensions.connection) -> None:
    """Update the last run timestamp in the ETL log."""
    logger = get_run_logger()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO cietrade.etl_log (process_name, last_run) VALUES (%s, %s)",
            ("accounts_update", datetime.now()),
        )
    logger.info("Updated last run timestamp")


@flow(name="Accounts Update Flow", retries=2)
def accounts_update_flow():
    """Main flow for updating accounts data."""
    logger = get_run_logger()
    logger.info("Starting accounts update flow")

    conn = None
    try:
        # Get database connection
        conn = get_db_connection()

        # Get last run timestamp
        last_run = get_last_run_timestamp(conn)

        # Fetch data from API
        api_data = fetch_data_from_api(last_run)

        if api_data:
            # Ensure unique constraint
            ensure_unique_constraint(conn)

            # Upsert records
            inserted, updated = upsert_records(conn, api_data)

            # Update last run timestamp
            update_last_run_timestamp(conn)

            # Commit the transaction
            conn.commit()
            logger.info(
                f"Flow completed successfully. Inserted: {inserted}, Updated: {updated}"
            )
        else:
            logger.info("No data to process")

    except Exception as e:
        logger.error(f"Flow failed: {str(e)}")
        if conn and not conn.closed:
            conn.rollback()
        raise
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    accounts_update_flow()
