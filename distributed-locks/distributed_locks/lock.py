"""
This module provides a lightweight representation of a distributed lock. 
"""
from datetime import datetime, timedelta, timezone

class Lock:
    def __init__(self, key: str, client_id: str, expiry: int): 
        self.key = key # Unique identifier for the lock
        self.client_id = client_id # Identifier for the client that holds the lock
        self.expiry = expiry # Lock time-to-live (TTL) in seconds
        self.start_time = datetime.now(timezone.utc) # Time when the lock was acquired
        self.status = "locked" # Current status of the lock

    def __str__(self):
        return f"Lock(key={self.key}, client_id={self.client_id}, expiry={self.expiry}, start_time={self.start_time}, status={self.status})"

    def reset_start_time(self):
        # Reset the start time to the current time.
        # This is used in cases where the lock is renewed by the client.
        self.start_time = datetime.now(timezone.utc)
        self.status = "locked" # Update the status to locked

    def update_status(self):
        # Update the status of the lock based on its expiry.
        if datetime.now(timezone.utc) - self.start_time > timedelta(seconds=self.expiry):
            self.status = "expired"
        else:
            self.status = "locked"
    
    def get_status(self) -> str:
        # We add this check to ensure the status is updated if the cron job hasn't run recently.
        if datetime.now(timezone.utc) - self.start_time > timedelta(seconds=self.expiry):
            self.status = "expired"
        return self.status
