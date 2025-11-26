"""
This is an implementation of the LRU cache. This cache represents one node in the cluster 
where data to be cached is stored
"""

from typing import Optional
import logging

logging.basicConfig(filename='lru_cache.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Doubly Linked List Node
class DLL_Node:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value
        self.prev, self.next = None, None
    

class CacheNode:
    def __init__(self, instance_no: int, cache_size: int):
        self.instance_no = instance_no # ID for the node
        self.cache_size = cache_size  # in bytes

        # We maintain two data structures to implement LRU Cache
        # The hash map stores the key to node mapping and allows for key searches in O(1) time
        # We use a doubly linked list to maintain the order of usage of the nodes
        # with the most recently used node at the tail and the least recently used node at the head
        self.hash_map = {} 
        self.head = DLL_Node(None, None)
        self.tail = DLL_Node(None, None)
        # Build the initial state of the Double linked list:
        # | Head | -> | Tail |
        # | Head | <- | Tail | 
        self.head.next, self.tail.prev = self.tail, self.head

    # Get the current cache size
    def get_cache_size(self):
        return len(self.hash_map)
    
    # Put entry into the cache
    def put_entry(self, key: str, value: str):
        # If key is present, overwrite it, else add the key
        logger.debug("CacheNode %d: Putting key: %s", self.instance_no, key)
        if key in self.hash_map:
            node = self.hash_map[key]
            self._remove_node(node) # Remove the node from its current position
        self._add_node(key, value) # Add the node to the tail
        # If addition of the new node exceeded cache size, 
        # remove the least recently used node from the head
        if len(self.hash_map) > self.cache_size:
            logger.debug("CacheNode %d: Cache size exceeded. Evicting least recently used key: %s", self.instance_no, self.head.next.key)
            # Remove the least recently used node
            node = self.head.next
            self._remove_node(node)
            del self.hash_map[node.key]
    
    # Get entry from the cache
    def get_entry(self, key: str):
        logger.debug("CacheNode %d: Getting key: %s", self.instance_no, key)
        if not key in self.hash_map:
            return None
        # Get the value
        node = self.hash_map[key]
        # Move the node to the tail as it was recently used
        self._remove_node(node)
        self._add_node(key, node.value)
        return node.value
    
    def _add_node(self, key: str, value: str):
        # Add the node to the tail
        node = DLL_Node(key, value)
        prev = self.tail.prev
        prev.next, node.prev = node, prev
        node.next, self.tail.prev = self.tail, node
        self.hash_map[key] = node
    
    def _remove_node(self, node: DLL_Node):
        prev, next = node.prev, node.next
        prev.next, next.prev = next, prev

    def _get_all_keys(self): 
        ''' Returns all keys in the cache node for testing purposes '''
        ''' This method is not to be used in production as it exposes internal state '''
        return list(self.hash_map.keys())
    
    def _get_all_kv_pairs(self):
        ''' Returns all key-value pairs in the cache node for testing purposes '''
        ''' This method is not to be used in production as it exposes internal state '''
        return {key: node.value for key, node in self.hash_map.items()}
        

# ----- Testing ----- 
if __name__ == "__main__":
    cache_node = CacheNode(instance_no=1, cache_size=3)
    cache_node.put_entry("key1", "value1")
    cache_node.put_entry("key2", "value2")
    cache_node.put_entry("key3", "value3")
    assert(cache_node.get_entry("key1") == "value1")  # Should print value1
    print(cache_node._get_all_kv_pairs())  # Should print all key-value pairs in the cache node
    cache_node.put_entry("key4", "value4")  # This should evict key2 as it is LRU
    assert(cache_node.get_entry("key2") is None)  # Should print None
    assert(cache_node.get_entry("key3") == "value3")  # Should print value3
    assert(cache_node.get_entry("key4") == "value4")  # Should print value4
    print(cache_node._get_all_kv_pairs())  # Should print all key-value pairs in the cache node


