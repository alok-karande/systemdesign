'''
API for managing ephemeral nodes in a distributed system.
'''
from ephemeral_node_manager import EphemeralNodeManager
from expired_lock_cleaner import ExpiredLockCleaner
from datetime import datetime, timezone
from typing import Optional
import logging
from flask import Flask, request, jsonify
import asyncio

# Initialize Flask app, logger, lock manager, and expired lock cleaner
app = Flask(__name__)
logging.basicConfig(filename='distributed_locks.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
node_manager = EphemeralNodeManager()
expired_lock_cleaner = ExpiredLockCleaner(node_manager=node_manager, cleanup_interval=5)
asyncio.run(expired_lock_cleaner.start())

@app.route("/create_node", methods=["POST"])
def create_node():
    data = request.json
    path = data.get("path")
    client_id = data.get("client_id")
    expiry = data.get("expiry")
    try:
        node = node_manager.create_node(path, client_id, expiry)
        return {"status": "success", "node_path": node.path, "client_id": node.client_id}, 200
    except Exception as e:
        return {"status": "error", "message": str(e)}, 400
    
@app.route("/delete_node", methods=["POST"])
def delete_node():
    data = request.json
    path = data.get("path")
    try:
        if node_manager.delete_node(path):
            return {"status": "success", "message": f"Node at path {path} deleted."}, 200
        else:
            return {"status": "error", "message": f"Node at path {path} not found."}, 404
    except Exception as e:
        return {"status": "error", "message": str(e)}, 400
    
@app.route("/node_status/<path>", methods=["GET"])
def node_status(path):
    node = node_manager.get_node(path)
    if node:
        return {"status": "success", "node_path": node.path, "client_id": node.client_id}, 200
    else:
        return {"status": "error", "message": f"Node at path {path} not found."}, 404
    
@app.route("/all_nodes", methods=["GET"])
def all_nodes():
    nodes = node_manager.get_nodes()
    nodes_info = {path: node.client_id for path, node in nodes.items()}
    return {"status": "success", "nodes": nodes_info}, 200  

@app.route("/current_lock_owner/<path>", methods=["GET"])
def current_lock_owner(path):
    owner = node_manager.get_current_lock_owner(path)
    if owner:
        return {"status": "success", "current_lock_owner": owner}, 200
    else:
        return {"status": "error", "message": f"No lock owner found for path {path}."}, 404
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6001)


