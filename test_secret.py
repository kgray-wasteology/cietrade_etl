# test_secret.py
from prefect.blocks.system import Secret

# Load and print the secret
secret = Secret.load("cietrade-api-credentials")
print(secret.get())

secret = Secret.load("wasteology-db-credentials")
print(secret.get())

secret = Secret.load("github-credentials")
print(secret.get())
