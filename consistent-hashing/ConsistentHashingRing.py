"""
This class implements a consistent hashing ring to distribute keys across multiple cache nodes.
Each cache node is represented by an instance of the CacheNode class.
"""

from typing import List, Optional
from cache.CacheNode import CacheNode
import hashlib
import bisect
import logging
from collections import defaultdict  

logging.basicConfig(filename='consistent_hashing.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConsistentHashingRing:
    def __init__(self, cache_size: int, servers: List[str], replication_factor: int) -> None:
        self.replication_factor = replication_factor
        self.ring = {} # Create the Hash Ring
        self.sorted_keys = [] # Sorted list of hash keys
        self.servers = set() # Set of servers in the ring
        self.cache_size = cache_size # Cache size for each CacheNode
        self.virtual_node_map = {} # Map of virtual nodes to real servers
        self.server_virtual_node_map = defaultdict(list) # Map of real servers to their virtual nodes
       
        logger.debug("Initializing Consistent Hashing Ring. Adding Servers: %s", servers)

        for server in servers:
            self.add_server(server)

    def _get_hash_key(self, key: str) -> int:
        ''' Returns a hash value for the given key using MD5'''
        # Convert the MD5 generated hash to int from base 16
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    

    # Note that the server related methods will be used specifically by monitoring programs 
    # to add/remove servers from the ring dynamically depending on load. These should not be
    # used directly by the clients.

    def add_server(self, server: str) -> None:
        ''' Adds a server to the hash ring with virtual nodes based on the replication factor '''
        self.servers.add(server)
        logger.debug("Adding Server: %s to the hash ring", server)
        parent_hash_val = self._get_hash_key(f"{server}-{0}") 
        logger.debug("Adding parent node with hash: %d for server: %s-0", parent_hash_val, server)
        self.ring[parent_hash_val] = CacheNode(instance_no=parent_hash_val, cache_size=self.cache_size)

        self.virtual_node_map[parent_hash_val] = parent_hash_val
        self.server_virtual_node_map[parent_hash_val].append(parent_hash_val)

        bisect.insort(self.sorted_keys, parent_hash_val)  
       
        for i in range(1, self.replication_factor):
            # Add as many virtual nodes as the replication factor
            hash_val = self._get_hash_key(f"{server}-{i}") 
            logger.debug("Adding virtual node with hash: %d for server: %s-%d", hash_val, server, i)

            self.virtual_node_map[hash_val] = parent_hash_val
            self.server_virtual_node_map[parent_hash_val].append(hash_val)

            #self.ring[hash_val] = CacheNode(instance_no=hash_val, cache_size=self.cache_size)
            # Keeping it sorted will help in efficient lookups
            bisect.insort(self.sorted_keys, hash_val)  

    def remove_server(self, server: str) -> None:
        ''' Removes a server and its virtual nodes from the hash ring '''
        if server in self.servers:
            # Get the parent hash value
            parent_hash_val = self._get_hash_key(f"{server}-0")
            logger.debug("Removing parent node with hash: %d for server: %s-0", parent_hash_val, server)
            for i in range(1, self.replication_factor):
                hash_val = self._get_hash_key(f"{server}-{i}")
                logger.debug("Removing virtual node with hash: %d for server: %s-%d", hash_val, server, i)
                # Remove from ring and sorted keys
                del self.ring[hash_val]
                # Find the index of hash_val in sorted_keys and remove it
                index = bisect.bisect_left(self.sorted_keys, hash_val)
                del self.sorted_keys[index]

            # Finally remove the parent node
            del self.virtual_node_map[parent_hash_val]
            del self.server_virtual_node_map[parent_hash_val]
            del self.ring[parent_hash_val]

            self.servers.remove(server)
            return True
        else:
            logger.warning("Attempted to remove non-existent server: %s from the hash ring", server)
            return False

    def get_server(self, key: str) -> CacheNode:
        ''' Returns the CacheNode responsible for the given key
            The key is hashed and the closest server is found clockwise in the ring '''
        # If the ring is empty return None
        if not self.ring:
            return None
        hash_val = self._get_hash_key(key)
        logger.debug("Hashed key: %s to hash value: %d", key, hash_val)
        # Find the index of the nearest server to the right (clockwise)
        index = bisect.bisect(self.sorted_keys, hash_val) % len(self.sorted_keys)
        if index == len(self.sorted_keys):
            index = 0  # Wrap around to the first server
        virtual_node_hash = self.sorted_keys[index]
        parent_hash = self.virtual_node_map[virtual_node_hash]
        return self.ring[parent_hash]  # Return the assigned CacheNode
    
    def get_servers(self) -> List[dict]: 
        ''' Returns the list of servers in the hash ring for testing purposes '''
        ''' This method is not to be used in production as it exposes internal state '''
        server_list = []
        for server in self.servers:
            server_dict = {"server": server}
            virtual_nodes = []
            for i in range(self.replication_factor):
                node_dict = {"virtual_node": {"name": f"{server}-{i}"}}
                node_dict["virtual_node"]["hash"] = self._get_hash_key(f"{server}-{i}")
                virtual_nodes.append(node_dict)
            server_dict["virtual_nodes"] = virtual_nodes
            server_list.append(server_dict)
        return server_list
    
    
    # TBD Methods to move keys when servers are added/removed *** 
    
    # Methods to interact with the cache nodes via consistent hashing

    def put_cache_entry(self, key: str, value: str) -> None:
        ''' Puts an entry into the appropriate cache node based on consistent hashing '''
        server = self.get_server(key)
        if server:
            logger.debug("Putting key: %s into server with instance_no: %d", key, server.instance_no)
            server.put_entry(key, value)
            return True
        else:
            logger.error("No servers available in the hash ring to put key: %s", key)
            # This could have been handled better with exceptions
            return False

    def get_cache_entry(self, key: str) -> 'Optional[str]':
        '''
        Gets an entry from the appropriate cache node based on consistent hashing.
        May raise an exception if no servers are available, or return None if the key is not found.
        '''
        server = self.get_server(key)
        logger.debug("Getting key: %s from server with instance_no: %d", key, server.instance_no)
        if server:

            value =  server.get_entry(key)
            if value is None:
                logger.warning("Key 'value' not found in response for key: %s from server with instance_no: %d", key, server.instance_no)
            return value
            
        else:
            raise Exception("No servers available in the hash ring")

# ----- Testing -----

if __name__ == "__main__":
    ring = ConsistentHashingRing(cache_size=3, servers=["svr1", "svr2"], replication_factor=2)
    ring.add_server("svr22")
    ring.put_cache_entry("key1", "value1")
    ring.put_cache_entry("key2", "value2")
    ring.put_cache_entry("key31", "value31")
    ring.put_cache_entry("key21", "value21")
    ring.put_cache_entry("all", "good")
    assert(ring.get_cache_entry("key1") == "value1")  # Should print value1
    assert(ring.get_cache_entry("key2") == "value2")  # Should print value2
    assert(ring.get_cache_entry("key31") == "value31")  # Should print value31
    assert(ring.get_cache_entry("key21") == "value21")  # Should print value21
    assert(ring.get_cache_entry("all") == "good")  # Should print sum
    




            
