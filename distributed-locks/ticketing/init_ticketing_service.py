"""
Initialization for the Ticketing Service DB. Sets up the DB, tables and initial data.
"""
import psycopg2
from db_config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def init_ticketing_service_db(HOST=None):
    # Note this will recreate the tickets table if it already exists.
    # Proceed with caution if you have existing data that you care about.
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
        print ("DB exists: ", db_exists)
        if not db_exists:
            # Create ticketing service database if it doesn't exist
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            # connection.commit()
            cursor.close()
            connection.close()
            print(f"Database {DB_NAME} created successfully.")
        else:
            print(f"Database {DB_NAME} already exists.")
            connection.close()

        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=HOST,
            port=DB_PORT
        )

        # Lets check if tickets table exists, if not create it.
        cursor = connection.cursor()
        check_table_query = '''
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_name = 'tickets'
        );
        '''
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0]
        print("Tickets table exists: ", table_exists)       
        if table_exists:
            # Lets delete the table and re-create it for fresh initialization
            print("Tickets table already exists. Recreating it. Existing data will be deleted")
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
        print("Ticket state enum type created successfully.")


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
        print("Tickets table created successfully.")


        # Insert initial tickets
        insert_tickets_query = '''
        INSERT INTO {} (state) VALUES ('{}');
        '''
        for _ in range(10):
            query = insert_tickets_query.format('tickets', 'available')
            cursor.execute(query)
        connection.commit()
        print("Initial tickets inserted successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error initializing ticketing service DB: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    init_ticketing_service_db()