"""
This file is the wrapper API to invoke the Consistent Hashing Ring methods via Flask endpoints.
"""
from typing import List
from flask import Flask, request
from ConsistentHashingRing import ConsistentHashingRing
from ConsistentHashingRingContainer import ConsistentHashingRingContainer
from cache.CacheNode import CacheNode
import logging


logging.basicConfig(filename='consistent_hashing.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
app = Flask(__name__)

import os

# Initialize the Consistent Hashing Ring with configurable parameters via environment variables
cache_size = int(os.getenv('CACHE_SIZE', 3))
servers = os.getenv('SERVERS', 'server1,server2').split(',')
replication_factor = int(os.getenv('REPLICATION_FACTOR', 2))
### Set this variable to change how you want to run the Consistent Hashing Ring: Local or Dockerized Cache Nodes
RUN_MODE_LOCAL = os.getenv('RUN_MODE_LOCAL', 'True') == 'True'  # Set to 'True' to use local CacheNode instances


print ('Initializing Consistent Hashing Ring in mode:', 'Local' if RUN_MODE_LOCAL else 'Dockerized Cache Nodes')

ring_controller = ConsistentHashingRingContainer(
    cache_size=cache_size,
    servers=servers,
    replication_factor=replication_factor
) if not RUN_MODE_LOCAL else ConsistentHashingRing(
    cache_size=cache_size,
    servers=servers,
    replication_factor=replication_factor
)

# *** Note:  The Server related methods will be used specifically by monitoring programs 
# to add/remove servers from the ring dynamically depending on load. These should not be
# used directly by the clients. ***
@app.route('/add_server', methods=['POST'])
def add_server() -> tuple[str, int]:
    ''' API to add a server to the hash ring '''
    server = request.json.get('server')
    logger.info("Received request to add server: %s", server)
    if server:
        ring_controller.add_server(server)
        return f"Server {server} added to the hash ring.", 200
    else:
        return "Server parameter is required.", 400
    
@app.route('/remove_server', methods=['POST'])
def remove_server() -> tuple[str, int]:
    ''' API to remove a server from the hash ring '''
    server = request.json.get('server')
    logger.info("Received request to remove server: %s", server)
    if server:
        if ring_controller.remove_server(server):
            return f"Server {server} removed from the hash ring.", 200
        else:
            return f"Server {server} not found in the hash ring.", 404
    else:
        return "Server parameter is required.", 400
    
@app.route('/get_servers', methods=['GET'])
def get_servers() -> List[dict]:
    ''' API to get the list of servers in the hash ring '''
    logger.info("Received request to get list of servers in the hash ring")
    servers = ring_controller.get_servers()
    return {"servers": servers}, 200


# *** Note: The following methods are the client-facing APIs 
# to interact with the cache via the Consistent Hashing Ring ***

@app.route('/put_cache_entry', methods=['POST'])
def put_cache_entry() -> tuple[str, int]:
    ''' API to put a cache entry into the hash ring '''
    # Get Key/Value from the request body
    key = request.json.get('key')
    value = request.json.get('value')
    logger.info("Received request to put cache entry: key=%s, value=%s", key, value)
    if key is None or value is None:
        return "Key and Value must be provided.", 400
    if ring_controller.put_cache_entry(key, value):
        return f"Cache entry for key {key} added.", 200
    else:
        return f"Failed to add cache entry for key {key}.", 500

@app.route('/get_cache_entry/<key>', methods=['GET'])
def get_cache_entry(key: str) -> tuple[str, int]:
    ''' API to get a cache entry from the hash ring '''
    logger.info("Received request to get cache entry for key: %s", key)
    value =  ring_controller.get_cache_entry(key)
    if value:
        return value, 200
    else:
        return f"Key {key} not found in cache.", 404
    
# ----- Testing -----
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6000)