"""
This is a file to invoke the CacheNode API endpoints via Flask.
"""
import logging
from flask import Flask, request
import sys

from CacheNode import CacheNode

logging.basicConfig(filename='lru_cache.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
app = Flask(__name__)

cache_node = None  # Initialized in the 'if __name__ == "__main__":' block

@app.route('/get_cache_size', methods=['GET'])
def get_cache_size():
    ''' API to get the current cache size '''
    if cache_node is None:
        return "CacheNode not initialized.", 500
    size = cache_node.get_cache_size()
    logger.info("CacheNode %d: Current cache size is %d", cache_node.instance_no, size)
    return {"cache_size": size}, 200

@app.route('/put_entry', methods=['POST'])
def put_entry():
    ''' API to put an entry into the cache '''
    if cache_node is None:
        return "CacheNode not initialized.", 500
    key = request.json.get('key')
    value = request.json.get('value')
    if key is None or value is None:
        return "Key and Value must be provided.", 400
    logger.info("CacheNode %d: Putting entry key=%s, value=%s", cache_node.instance_no, key, value)
    cache_node.put_entry(key, value)
    return f"Entry for key {key} added/updated.", 200
   
    
@app.route('/get_entry/<key>', methods=['GET'])
def get_entry(key: str):
    ''' API to get an entry from the cache '''
    if cache_node is None:
        return "CacheNode not initialized.", 500
    logger.info("CacheNode %d: Getting entry for key=%s", cache_node.instance_no, key)
    value = cache_node.get_entry(key)
    if value is not None:
        return {"key": key, "value": value}, 200
    else:
        return {"error": f"Key {key} not found in cache"}, 404


if __name__ == '__main__':
    if len(sys.argv) >= 3:
        instance_no = int(sys.argv[1])
        cache_size = int(sys.argv[2])
        logger.info("Starting CacheNode instance %d with cache size %d", instance_no, cache_size)
        cache_node = CacheNode(instance_no=instance_no, cache_size=cache_size)
        app.run(host='0.0.0.0', port=5000)
    else:
        logger.error("Insufficient arguments provided. Usage: python CacheAPIInvocation.py <instance_no> <cache_size>")
        print("Usage: python CacheAPIInvocation.py <instance_no> <cache_size>")
