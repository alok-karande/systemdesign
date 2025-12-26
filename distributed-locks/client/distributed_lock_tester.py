"""
Client to test distributed locks
"""
import requests, time

base_url = "http://localhost:6000"
base_ticketing_url= "http://localhost:6005"


def init_ticketing_service_db():
    try:
        response = requests.post(f"{base_ticketing_url}/initialize", json={"DB_HOST": "host.docker.internal"})
        print("Ticketing Service DB Initialization Response:", response.json())
    except Exception as e:
        print("Error initializing ticketing service DB:", str(e))

def test_distributed_locks():
    # Client 1 tries to acquire a lock
    client_1_data = {
        "key": "resource_lock",
        "client_id": "client_1",
        "expiry": 10  # seconds
    }
    response = requests.post(f"{base_url}/acquire_lock", json=client_1_data)
    if response.status_code == 200:
        print("Client 1 Acquire Lock Response:", response.json())
    elif response.status_code == 409:
        print("Client 1 Acquire Lock Failed:", response.json())

    # Client 2 tries to acquire the same lock
    client_2_data = {
        "key": "resource_lock",
        "client_id": "client_2",
        "expiry": 10  # seconds
    }
    response = requests.post(f"{base_url}/acquire_lock", json=client_2_data)
    print("Client 2 Acquire Lock Response:", response.json())

    # Client 1 checks lock status
    response = requests.get(f"{base_url}/lock_status/resource_lock")
    print("Client 1 Lock Status Response:", response.json())

    # Client 1 releases the lock
    response = requests.post(f"{base_url}/release_lock", json={"key": "resource_lock", "client_id": "client_1"})
    print("Client 1 Release Lock Response:", response.json())

    # Client 2 tries to acquire the lock again
    response = requests.post(f"{base_url}/acquire_lock", json=client_2_data)
    print("Client 2 Acquire Lock After Release Response:", response.json())


def test_ticket_reservation():
    try:

        # Get Available tickets
        response = requests.get(f"{base_ticketing_url}/available_tickets")
        print("Available Tickets Response:", response.json())
        available_tickets = response.json().get("available_tickets", [])
        if available_tickets and len(available_tickets) > 1:
            url = f"{base_ticketing_url}/reserve_ticket"
            ticket_id = available_tickets[0]

            # Client 1 tries to reserve a ticket
            response = requests.post(f"{base_ticketing_url}/reserve_ticket", json={"ticket_id": ticket_id, "client_id": "client_1223"})
            print("Client 1 Reserve Ticket Response:", response.json())

            # Client 2 tries to reserve the same ticket
            response = requests.post(f"{base_ticketing_url}/reserve_ticket", json={"ticket_id": ticket_id, "client_id": "client_2224"})
            print("Client 2 Reserve Ticket Response:", response.json())

            # Client 1 tries to book the ticket
            response = requests.post(f"{base_ticketing_url}/book_ticket", json={"ticket_id": ticket_id, "client_id": "client_1223"})
            print("Client 1 Book Ticket Response:", response.json())

            # Client 1 tries to reserver another ticket
            ticket_id = available_tickets[1]
            response = requests.post(f"{base_ticketing_url}/reserve_ticket", json={"ticket_id": ticket_id, "client_id": "client_1223"})
            print("Client 1 Reserve Another Ticket Response:", response.json())

            # Lets sleep for a while to show that Client 1 has either timedout/crashed and Client 2 can now reserve the ticket.
            print("Sleeping for 12 seconds to let the lock expire...")
            time.sleep(12)
            # Client 2 tries to reserve the same ticket again
            response = requests.post(f"{base_ticketing_url}/reserve_ticket", json={"ticket_id": ticket_id, "client_id": "client_2224"})
            print("Client 2 Reserve Ticket After Lock Expiry Response:", response.json())

            # Client 2 tries to book the ticket
            response = requests.post(f"{base_ticketing_url}/book_ticket", json={"ticket_id": ticket_id, "client_id": "client_2224"})
            print("Client 2 Book Ticket Response:", response.json())


    except Exception as e:
        print("Error during ticket reservation and booking:", str(e))


if __name__ == "__main__":

    # init_ticketing_service_db()

    # test_distributed_locks()
    test_ticket_reservation()
