"""
DB Configuration objects for connecting to Postgres
"""
DB_NAME = "ticketdb"
# DB_USER = "dbadmin"
DB_USER = "myuser"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5555"

# Initialize the endpoint for distributed lock service
DISTRIBUTED_LOCK_SERVICE_URL = "http://localhost:6000"

# Initialize the endpoint for ephemeral lock service
EPHEMERAL_LOCK_SERVICE_URL = "http://localhost:6001"