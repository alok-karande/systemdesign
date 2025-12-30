"""
Initialization for the Ticketing Service DB. Sets up the DB, tables and initial data.
"""
import psycopg2, logging
from db_config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

# Update this to change number of tickets created during initialization
TOTAL_TICKETS = 10

logging.basicConfig(filename='ticket_reservation.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to initialize the ticketing service database
def init_ticketing_service_db(HOST=None):
    # Note this will recreate the tickets table if it already exists.
    # Proceed with caution if you have existing data that you care about.
    connection = None
    try:
        if not HOST:
            HOST = DB_HOST

        connection = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=HOST,
            port=DB_PORT
        )
        connection.autocommit = True
        cursor = connection.cursor()

        check_db_query = f"SELECT 1 FROM pg_database WHERE datname='{'postgres'}';"
        cursor.execute(check_db_query)
        db_exists = cursor.fetchone()
        logger.info("DB exists: %s", db_exists)
        if not db_exists:
            # Create ticketing service database if it doesn't exist
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            cursor.close()
            connection.close()
            logger.info(f"Database {DB_NAME} created successfully.")
        else:
            logger.info(f"Database {DB_NAME} already exists.")
            connection.close()

        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=HOST,
            port=DB_PORT
        )
        cursor = connection.cursor()


        # Lets delete the table and re-create it for fresh initialization
        logger.info("Tickets table already exists. Recreating it. Existing data will be deleted")
        cursor.execute("DROP TABLE IF EXISTS tickets;")
        cursor.execute("DROP TYPE IF EXISTS ticket_state;")
        connection.commit()
            

        # Create ticket state enum type
        ticket_type_enum = '''
        CREATE TYPE ticket_state AS ENUM (
        'available', 'reserved', 'sold');
        '''
        cursor.execute(ticket_type_enum)
        connection.commit()
        logger.info("Ticket state enum type created successfully.")


        # Create tickets table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id SERIAL PRIMARY KEY,
            sold_to VARCHAR(50),
            state ticket_state NOT NULL
        );
        '''
        cursor.execute(create_table_query)
        connection.commit()
        logger.info("Tickets table created successfully.")


        # Insert initial tickets
        insert_tickets_query = '''
        INSERT INTO {} (state) VALUES ('{}');
        '''
        for _ in range(TOTAL_TICKETS):
            query = insert_tickets_query.format('tickets', 'available')
            cursor.execute(query)
        connection.commit()
        logger.info("Initial tickets inserted successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error initializing ticketing service DB: {error}")
    finally:
        if connection and cursor:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    init_ticketing_service_db()