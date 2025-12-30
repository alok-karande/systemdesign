"""
Ephemeral node implementation for managing ephemeral nodes in a distributed system.
"""

from datetime import datetime, timezone

class EphemeralNode:
    def __init__(self, path: str, client_id: str, session_expiry: int, seq_num: int = 0, is_parent: bool = False):
        self.path = path
        self.client_id = client_id
        self.creation_time = None  # Set to current time when node is the current lock owner
        self.session_expiry = session_expiry  # Session expiry time in seconds
        self.seq_num = seq_num # For parent nodes this value is incremented for each child node created.
        self.is_parent = is_parent # Flag to indicate if this node is a parent node

    def __str__(self):
        return f"EphemeralNode(path={self.path}, client_id={self.client_id}, creation_time={self.creation_time}, session_expiry={self.session_expiry}, seq_num={self.seq_num}, is_parent={self.is_parent})"
    
    def increment_seq_num(self):
        # Only used for parent nodes to track child nodes.
        self.seq_num += 1

    def is_parent_node(self) -> bool:
        return self.is_parent
    
    def is_expired(self, current_time: datetime) -> bool:
        # Check if the ephemeral node has expired based on session expiry time.
        if self.creation_time is None:
            return False
        return (current_time - self.creation_time).total_seconds() > self.session_expiry
    
    #  Resets the creation time to current time when the node becomes the lock owner
    def reset_creation_time(self):
        self.creation_time = datetime.now(timezone.utc)