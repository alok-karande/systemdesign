"""
This module provides helper functions for creating and managing docker containers for each CacheNode.
It uses the Docker SDK for Python to interact with Docker.
"""
import docker
import logging
import os

log_file = os.environ.get('CONSISTENT_HASHING_LOG', 'consistent_hashing.log')
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ContainerNode:
    def __init__(self, container, instance_no: int, port: int):
        self.container = container
        self.instance_no = instance_no
        self.port = port

class CacheDockerHelper:
    def __init__(self, port_base: int = 5000):
        self.client = docker.from_env()
        self.port_base = port_base

    def create_container(self, name: str, instance_no : int, cache_size: int, port: int):
        container = self.client.containers.run(
            image="lru_cache_node:latest",
            name=name,
            command=f"{instance_no} {cache_size}",
            detach=True,
            ports={"5000/tcp": port}
        )
        logger.info("Created container for CacheNode %d", instance_no)
        return ContainerNode(container=container, port=port, instance_no=instance_no)

    def stop_container(self, container_node: ContainerNode):
        container_node.container.stop()
        logger.info("Stopped container for CacheNode %d", container_node.instance_no)

    def remove_container(self, container_node: ContainerNode):
        container_node.container.remove()
        logger.info("Removed container for CacheNode %d", container_node.instance_no)