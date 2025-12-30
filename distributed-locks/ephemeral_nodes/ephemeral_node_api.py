"""
API for managing ephemeral nodes in a distributed system.
"""

from ephemeral_node_manager import EphemeralNodeManager
from expired_lock_cleaner import ExpiredLockCleaner
from ephemeral_node import EphemeralNode
from datetime import datetime, timezone
from typing import Optional
import logging
from flask import Flask, request, jsonify
import time

# Initialize Flask app, logger, lock manager, and expired lock cleaner
app = Flask(__name__)
logging.basicConfig(filename='distributed_locks.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
node_manager = EphemeralNodeManager()
expired_lock_cleaner = ExpiredLockCleaner(node_manager=node_manager, cleanup_interval=10)
expired_lock_cleaner.start()

# API Endpoint to Create Ephemeral Node
@app.route("/create_node", methods=["POST"])
def create_node():
    logger.info("Received request to create node")
    data = request.json
    path = data.get("path")
    client_id = data.get("client_id")
    expiry = data.get("expiry")
    try:
        child_path = node_manager.create_node(path, client_id, expiry)
        if child_path:
            node = node_manager.get_node(child_path)
            return {"status": "success", "node_path": node.path, "client_id": node.client_id}, 200
        else:
            return {"status": "error", "message": "Failed to create node."}, 400
    except Exception as e:
        return {"status": "error", "message": str(e)}, 400
    
# API Endpoint to Delete Ephemeral Node
@app.route("/delete_node", methods=["POST"])
def delete_node():
    logger.info("Received request to delete node")
    data = request.json
    path = data.get("path")
    try:
        if node_manager.delete_node(path):
            return {"status": "success", "message": f"Node at path {path} deleted."}, 200
        else:
            return {"status": "error", "message": f"Node at path {path} not found."}, 404
    except Exception as e:
        return {"status": "error", "message": str(e)}, 400
    
# API Endpoint to Get Ephemeral Node Status
@app.route("/node_status/<path>", methods=["GET"])
def node_status(path):
    logger.info("Received request for node status of path %s", path)
    node = node_manager.get_node(path)
    if node:
        return {"status": "success", "node_path": node.path, "client_id": node.client_id}, 200
    else:
        return {"status": "error", "message": f"Node at path {path} not found."}, 404
    
# API Endpoint to Get All Ephemeral Nodes
@app.route("/all_nodes", methods=["GET"])
def all_nodes():
    logger.info("Received request to retrieve all nodes")
    nodes = node_manager.get_nodes()
    nodes_info = {path: node.client_id for path, node in nodes.items()}
    return {"status": "success", "nodes": nodes_info}, 200  

# API Endpoint to Get Current Lock Owner for a Given Path
@app.route("/current_lock_owner", methods=["GET"])
def current_lock_owner():
    path = request.args.get("path")
    logger.info("Received request to get current lock owner for path %s", path)
    owner = node_manager.get_current_lock_owner(path)
    if owner:
        return {"status": "success", "current_lock_owner": owner}, 200
    else:
        return {"status": "error", "message": f"No lock owner found for path {path}."}, 404
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6001)


    ''' Test code for Ephemeral Node Manager. Used for local testing. Use the distributed_lock_tester.py for testing via API calls.
    # Lets test by creating a node and checking its status
    parent_path = "/app/service/ticket1"
    path = node_manager.create_node(parent_path, "client_123", 15)
    node = node_manager.get_node(path)
    print(f"Node created at path: {node}")
    print(f"Current lock owner for {parent_path}: ", node_manager.get_current_lock_owner(parent_path)) 

    path = node_manager.create_node(parent_path, "client_234", 15)
    node = node_manager.get_node(path)
    print(f"Node created at path: {node}")
    print(f"All Nodes : {node_manager.get_nodes()}")
    print(f"Current lock owner for {parent_path}: ", node_manager.get_current_lock_owner(parent_path)) 

    # Test expired lock cleaner
    node_manager.cleanup_expired_nodes()

    time.sleep(10)  # Wait for nodes to expire
    #node_manager.cleanup_expired_nodes()
    print(f"After expiry, current lock owner for {parent_path}: ", node_manager.get_current_lock_owner(parent_path))
    time.sleep(20)  # Wait for nodes to expire
    #node_manager.cleanup_expired_nodes()
    print(f"After expiry, current lock owner for {parent_path}: ", node_manager.get_current_lock_owner(parent_path))
    time.sleep(20)  # Wait for nodes to expire
    #node_manager.cleanup_expired_nodes()
    print(f"After expiry, current lock owner for {parent_path}: ", node_manager.get_current_lock_owner(parent_path))
    '''


