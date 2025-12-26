"""
Ticketing service implementation used to demonstrate distributed locks.
"""
import psycopg2
import requests, logging
from db_config import DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

# Initialize Flask app, logger, lock manager, and expired lock cleaner
logging.basicConfig(filename='ticket_reservation.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TicketingService:
    def __init__(self, lock_manager_type="distributed_lock", 
        DB_HOST="localhost", 
        DISTRIBUTED_LOCK_SERVICE_URL="http://localhost:6000", 
        EPHEMERAL_LOCK_SERVICE_URL="http://localhost:6001"):

        self.lock_manager_type = lock_manager_type
        self.base_url = DISTRIBUTED_LOCK_SERVICE_URL if lock_manager_type == "distributed_lock" else EPHEMERAL_LOCK_SERVICE_URL
        self.DB_HOST = DB_HOST
        self.connection = None
        self.cursor = None
        self.expiry = 10  # Lock expiry time in seconds

        # Lets connect with the Postgres DB
        try:
            self._acquire_connection()
            # Execute a sample query
            self.cursor.execute("SELECT version();")

            # Fetch the result
            db_version = self.cursor.fetchone()
            logger.debug(f"PostgreSQL database version: {db_version}")

            # Lets create some tickets and save the ticket_ids in a list
            self.tickets = {}
            # Fetch all tickets from the tickets table
            self.cursor.execute("SELECT ticket_id, sold_to, state FROM tickets;")
            rows = self.cursor.fetchall()
            for row in rows:
                self.tickets[row[0]] = (row[1], row[2])  # ticket_id: (sold_to, state)       
            logger.debug(f"Fetched tickets with IDs: {self.tickets}") 
        except (Exception, psycopg2.DatabaseError) as error:
            logger.debug(error)
            self._close_connection()


    def _acquire_connection(self):
        try:
            self.connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=self.DB_HOST,
            port=DB_PORT
            )
            self.cursor = self.connection.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            self._close_connection()

    def _close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def execute_db_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.debug("Error executing query:", error)
            self.connection.rollback()
            return False    
        return True


    def _acquire_lock_for_ticket(self, ticket_id, client_id):
        logger.debug(f"{client_id} attempting to acquire lock for ticket {ticket_id}")
        if self.lock_manager_type == "distributed_lock":
            client_data = {"key": f"ticket_lock_{ticket_id}", "client_id": client_id, "expiry": self.expiry }
            response = requests.post(f"{self.base_url}/acquire_lock", json=client_data)
            if response.status_code == 200:
                logger.info(f"{client_id} acquired Lock on ticket {ticket_id}:", response.json())
                return True
            elif response.status_code == 409:
                logger.debug("{client_id} could not acquire lock on ticket {ticket_id}:", response.json())
                return False
        elif self.lock_manager_type == "ephemeral_node":
            pass
        else:
            raise Exception("Invalid lock manager type specified.")
        
    def _client_has_lock_for_ticket(self, ticket_id, client_id):
        logger.debug(f"Checking if {client_id} has lock for ticket {ticket_id}")
        if self.lock_manager_type == "distributed_lock":
            response = requests.get(f"{self.base_url}/lock_status/ticket_lock_{ticket_id}")
            if response.status_code == 200:
                lock_info = response.json()
                logger.debug(f"Lock info for ticket {ticket_id}: {lock_info}")
                if lock_info["lock_status"] == "locked" and lock_info.get("client_id") == client_id:
                    return True
            return False
        elif self.lock_manager_type == "ephemeral_node":
            # For ephemeral node, we would check if the node exists and is owned by the client_id
            pass
        else:
            raise Exception("Invalid lock manager type specified.")

    def reserve_ticket(self, ticket_id, client_id):   
        logger.debug(f"{client_id} attempting to reserve ticket {ticket_id}")
        # Make a call to postgres to check if ticket is available.
        # Then try to obtain a distributed lock for that ticket.
        # If lock is obtained, mark the ticket as reserved in the DB.
        if ticket_id in self.tickets and self._acquire_lock_for_ticket(ticket_id, client_id):
            sold_to, state = self.tickets[ticket_id]
            # First lets acquire the lock for this ticket.
            # If we get the lock, we can proceed to reserve the ticket.
            # If we don't get the lock, we return failure to the client.
            if state == 'available' or state == 'reserved': # In case we try to reserve an already reserved ticket for the same client.
                # Mark ticket as reserved
                # Lets update the database entry as well.
                db_query = "UPDATE tickets SET sold_to = %s, state = %s WHERE ticket_id = %s;"
                if self.execute_db_query(db_query, (client_id, 'reserved', ticket_id)):
                    self.tickets[ticket_id] = (client_id, 'reserved')
                    return True
                else:
                    logger.debug ("Failed to update ticket status in DB.")
        return False


    def book_ticket(self, ticket_id, client_id):
        logger.debug(f"{client_id} attempting to book ticket {ticket_id}")
        # Make a call to postgres to check if ticket is reserved.
        # Check if this client has the lock for this ticket.
        # If yes, mark the ticket as booked in the DB.
        if ticket_id in self.tickets and self._client_has_lock_for_ticket(ticket_id, client_id):
            sold_to, state = self.tickets[ticket_id]
            if state == "reserved" and sold_to == client_id:
                db_query = "UPDATE tickets SET state = %s WHERE ticket_id = %s;"
                if self.execute_db_query(db_query, ('sold', ticket_id)):
                    # Mark ticket as sold
                    self.tickets[ticket_id] = (client_id, 'sold')
                    return True 
                else:
                    logger.debug ("Failed to update ticket status in DB.")
        return False

    def get_available_tickets(self):
        for ticket_id, (sold_to, state) in self.tickets.items():
            if state == 'available':
                yield ticket_id
    
    
if __name__ == "__main__":
    ticketing_service = TicketingService()
    available_tickets = list(ticketing_service.get_available_tickets())
    logger.debug(f"Available tickets: {available_tickets}")
    if available_tickets:
        ticket_id = available_tickets[0]
        client_id = "client_1"  
        if ticketing_service.reserve_ticket(ticket_id, client_id):
            logger.debug(f"Ticket {ticket_id} reserved by {client_id}.")
            if ticketing_service.book_ticket(ticket_id, client_id):
                logger.debug(f"Ticket {ticket_id} booked by {client_id}.")
            else:
                logger.debug(f"Failed to book ticket {ticket_id} by {client_id}.")
        else:
            logger.error(f"Failed to reserve ticket {ticket_id} by {client_id}.")
    else:
        logger.error("No available tickets to reserve.")
    
    # Lets try to book tickets concurrently with two clients trying to reserve and book the same ticket.
    # This code is commented out to avoid complexity in this example.
  