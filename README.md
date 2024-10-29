<!-- @format -->

# Wasteology Prefect Flows

This repository contains Prefect flows for syncing data between CIETrade and the Wasteology database.

## Flows

- `accounts.py`: Syncs account data from CIETrade API
- `locations.py`: Syncs account location data from CIETrade API

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your credentials:

```env
DB_NAME=wasteology
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432
CIETRADE_USER_ID=your_api_user
CIETRADE_PASSWORD=your_api_password
```

3. Deploy flows to Prefect Cloud:

```bash
prefect deployment build flows/accounts.py:accounts_update_flow -n "prod-accounts-update" --cron "0 5 * * *"
prefect deployment build flows/locations.py:account_locations_update_flow -n "prod-locations-update" --cron "0 5 * * *"
```

## Running Locally

To run flows locally:

```bash
python flows/accounts.py
python flows/locations.py
```
