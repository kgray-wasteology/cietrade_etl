import requests
import psycopg2
import json
import os
from psycopg2 import sql
from datetime import datetime, timedelta
from prefect import task, flow
from typing import Dict, List, Optional, Tuple
from prefect.logging import get_run_logger

# API configuration
API_URL = "https://api.cietrade.net/ListAccountLocations"
API_PARAMS = {
    "UserID": "processautomation@wasteologygroup.com",
    "UserPwd": "Aliens4*Ditzy*Other",
    "AccountName": "",  # Leave blank to get locations for all accounts
}

# Database configuration
DB_CONFIG = {
    "dbname": "wasteology",
    "user": "kgray",
    "password": "Post@Waste12",
    "host": "pg-wasteology.postgres.database.azure.com",
    "port": "5432",
    "sslmode": "require",
}


@task(retries=3, retry_delay_seconds=30)
def get_db_connection() -> psycopg2.extensions.connection:
    """Create and return a database connection."""
    logger = get_run_logger()
    logger.info("Connecting to database")
    conn = psycopg2.connect(**DB_CONFIG)
    logger.info("Successfully connected to database")
    return conn


@task(retries=2)
def get_last_run_timestamp(conn: psycopg2.extensions.connection) -> Optional[datetime]:
    """Get the timestamp of the last successful run."""
    logger = get_run_logger()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_run FROM cietrade.etl_log WHERE process_name = 'account_locations_update' ORDER BY last_run DESC LIMIT 1"
        )
        result = cur.fetchone()
        logger.info(f"Last run timestamp: {result[0] if result else None}")
        return result[0] if result else None


@task(retries=3, retry_delay_seconds=60)
def fetch_data_from_api(last_run: Optional[datetime]) -> List[Dict]:
    """Fetch data from the API with retry logic."""
    logger = get_run_logger()
    logger.info("Fetching data from API")

    params = API_PARAMS.copy()
    if last_run:
        params["UpdatedSince"] = last_run.strftime("%Y-%m-%dT%H:%M:%S")

    response = requests.get(API_URL, params=params)
    response.raise_for_status()  # Will trigger retries if status code is not 200

    data = response.json()
    logger.info(f"Successfully fetched {len(data)} records from API")
    return data


@task
def upsert_records(
    conn: psycopg2.extensions.connection, data: List[Dict]
) -> Tuple[int, int, int]:
    """Upsert records into the database."""
    logger = get_run_logger()
    logger.info("Starting data upsert")

    insert_stmt = sql.SQL(
        """
        INSERT INTO cietrade.account_locations (
            account_name, location_short_name, address_id, account_id,
            company_primary_email, address_line_1, address_line_2, city,
            region, postal, country, is_primary_address, contact_name,
            contact_email, billing_email_address, last_updated
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (address_id) DO UPDATE SET
            account_name = EXCLUDED.account_name,
            location_short_name = EXCLUDED.location_short_name,
            account_id = EXCLUDED.account_id,
            company_primary_email = EXCLUDED.company_primary_email,
            address_line_1 = EXCLUDED.address_line_1,
            address_line_2 = EXCLUDED.address_line_2,
            city = EXCLUDED.city,
            region = EXCLUDED.region,
            postal = EXCLUDED.postal,
            country = EXCLUDED.country,
            is_primary_address = EXCLUDED.is_primary_address,
            contact_name = EXCLUDED.contact_name,
            contact_email = EXCLUDED.contact_email,
            billing_email_address = EXCLUDED.billing_email_address,
            last_updated = CURRENT_TIMESTAMP
        WHERE
            cietrade.account_locations.account_name != EXCLUDED.account_name OR
            cietrade.account_locations.location_short_name != EXCLUDED.location_short_name OR
            cietrade.account_locations.account_id != EXCLUDED.account_id OR
            cietrade.account_locations.company_primary_email != EXCLUDED.company_primary_email OR
            cietrade.account_locations.address_line_1 != EXCLUDED.address_line_1 OR
            cietrade.account_locations.address_line_2 != EXCLUDED.address_line_2 OR
            cietrade.account_locations.city != EXCLUDED.city OR
            cietrade.account_locations.region != EXCLUDED.region OR
            cietrade.account_locations.postal != EXCLUDED.postal OR
            cietrade.account_locations.country != EXCLUDED.country OR
            cietrade.account_locations.is_primary_address != EXCLUDED.is_primary_address OR
            cietrade.account_locations.contact_name != EXCLUDED.contact_name OR
            cietrade.account_locations.contact_email != EXCLUDED.contact_email OR
            cietrade.account_locations.billing_email_address != EXCLUDED.billing_email_address
        RETURNING address_id, (xmax = 0) AS inserted
    """
    )

    inserted_count = 0
    updated_count = 0
    skipped_count = 0

    for item in data:
        if item.get("address_id") is None:
            logger.warning(f"Skipped record due to null address_id: {json.dumps(item)}")
            skipped_count += 1
            continue

        try:
            with conn.cursor() as cur:
                cur.execute(
                    insert_stmt,
                    (
                        item.get("account_name"),
                        item.get("location_short_name"),
                        item["address_id"],
                        item.get("account_id"),
                        item.get("company_primary_email"),
                        item.get("address_line_1"),
                        item.get("address_line_2"),
                        item.get("city"),
                        item.get("region"),
                        item.get("postal"),
                        item.get("country"),
                        item.get("is_primary_address"),
                        item.get("contact_name"),
                        item.get("contact_email"),
                        item.get("billing_email_address"),
                    ),
                )
                result = cur.fetchone()
                if result:
                    address_id, inserted = result
                    if inserted:
                        inserted_count += 1
                        logger.info(f"Inserted new record for address_id: {address_id}")
                    else:
                        updated_count += 1
                        logger.info(
                            f"Updated existing record for address_id: {address_id}"
                        )
                else:
                    logger.warning(
                        f"No result returned for address_id: {item['address_id']}"
                    )

        except psycopg2.Error as e:
            logger.error(f"Error inserting/updating record: {e}")
            logger.error(f"Problematic record: {json.dumps(item)}")
            continue  # Skip to next record but don't fail the entire task

    logger.info(
        f"Upsert completed. Inserted: {inserted_count}, Updated: {updated_count}, "
        f"Skipped: {skipped_count}, Total processed: {len(data)}"
    )
    return inserted_count, updated_count, skipped_count


@task
def update_last_run_timestamp(conn: psycopg2.extensions.connection) -> None:
    """Update the last run timestamp in the ETL log."""
    logger = get_run_logger()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO cietrade.etl_log (process_name, last_run) VALUES (%s, %s)",
            ("account_locations_update", datetime.now()),
        )
    logger.info("Updated last run timestamp")


@flow(name="Account Locations Update Flow", retries=2)
def account_locations_update_flow():
    """Main flow for updating account locations data."""
    logger = get_run_logger()
    logger.info("Starting account locations update flow")

    try:
        # Get database connection
        conn = get_db_connection()

        # Get last run timestamp
        last_run = get_last_run_timestamp(conn)

        # Fetch data from API
        api_data = fetch_data_from_api(last_run)

        if api_data:
            # Upsert records
            inserted, updated, skipped = upsert_records(conn, api_data)

            # Update last run timestamp
            update_last_run_timestamp(conn)

            # Commit the transaction
            conn.commit()
            logger.info(
                f"Flow completed successfully. "
                f"Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}"
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
    account_locations_update_flow()
