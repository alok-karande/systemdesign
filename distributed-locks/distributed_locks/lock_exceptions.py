"""
List of exceptions related to lock operations.
"""
class LockAlreadyHeldException(Exception):
    # Raised when attempting to acquire a lock that is already held by another client."""
    def __init__(self, error_message: str):
        super().__init__(error_message)