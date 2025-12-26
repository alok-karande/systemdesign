'''
Ticketing Service API to interact with the TicketingService class.
'''
from ticketing_service import TicketingService
from init_ticketing_service import init_ticketing_service_db
import db_config
from flask import Flask, request, jsonify
import logging, sys

# Initialize Flask app, logger, lock manager, and expired lock cleaner
app = Flask(__name__)
logging.basicConfig(filename='ticket_reservation.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ticketing_service = None

@app.route("/reserve_ticket", methods=["POST"])
def reserve_ticket():
    data = request.json
    ticket_id = data.get("ticket_id")
    client_id = data.get("client_id")
    if ticketing_service.reserve_ticket(ticket_id, client_id):
        return jsonify({"status": "success", "message": f"Ticket {ticket_id} reserved by {client_id}."}), 200
    else:
        return jsonify({"status": "error", "message": f"Failed to reserve ticket {ticket_id}."}), 400
    
@app.route("/book_ticket", methods=["POST"])
def book_ticket():
    data = request.json
    ticket_id = data.get("ticket_id")
    client_id = data.get("client_id")
    if ticketing_service.book_ticket(ticket_id, client_id):
        return jsonify({"status": "success", "message": f"Ticket {ticket_id} booked by {client_id}."}), 200
    else:
        return jsonify({"status": "error", "message": f"Failed to book ticket {ticket_id}."}), 400  
    
@app.route("/available_tickets", methods=["GET"])
def available_tickets():
    tickets = list(ticketing_service.get_available_tickets())
    logger.debug("Available tickets:", tickets)
    return jsonify({"status": "success", "available_tickets": tickets}), 200

@app.route("/initialize", methods=["POST"])
def initialize_db(DB_HOST="host.docker.internal"):
    if DB_HOST:
        init_ticketing_service_db(DB_HOST)
        return jsonify({"status": "success", "message": "Ticketing service initialized."}), 200
    else:
        return jsonify({"status": "error", "message": "DB_HOST parameter is required."}), 400   

if __name__ == "__main__":
    if len(sys.argv) >= 4:
        db_config.DB_HOST = sys.argv[1]
        db_config.DISTRIBUTED_LOCK_SERVICE_URL = sys.argv[2]
        db_config.EPHEMERAL_LOCK_SERVICE_URL = sys.argv[3]

    logger.info("Updating PostGresSQL Endpoint to %s, Distributed Lock Endpoint to %s and Ephemeral Lock Endpoint to %s", 
        db_config.DB_HOST, db_config.DISTRIBUTED_LOCK_SERVICE_URL, db_config.EPHEMERAL_LOCK_SERVICE_URL)
    ticketing_service = TicketingService("distributed_lock", 
        db_config.DB_HOST, db_config.DISTRIBUTED_LOCK_SERVICE_URL, db_config.EPHEMERAL_LOCK_SERVICE_URL)

    app.run(host='0.0.0.0', port=6005)



