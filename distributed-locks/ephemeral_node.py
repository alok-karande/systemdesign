"""
Ephemeral node implementation for managing ephemeral nodes in a distributed system.
"""

class EphemeralNode:
    def __init__(self, path: str, client_id: str, session_expiry: int, seq_num: int = 0):
        self.path = path
        self.client_id = client_id
        self.creation_time = None  # Could be set to current time when node is created
        self.session_expiry = session_expiry  # Session expiry time in seconds
        self.seq_num = seq_num # For parent nodes this value is incremented for each child node created.
    
    def increment_seq_num(self):
        # Only used for parent nodes to track child nodes.
        self.seq_num += 1
    
    def is_expired(self, current_time) -> bool:
        if self.creation_time is None:
            return False
        return (current_time - self.creation_time).total_seconds() > self.session_expiry
    
    def reset_creation_time(self, current_time):
        self.creation_time = current_time