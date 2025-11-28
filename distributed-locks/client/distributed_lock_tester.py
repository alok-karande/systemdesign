"""
Client to test distributed locks
"""
import requests


if __name__ == "__main__":
    base_url = "http://localhost:6000"

    # Client 1 tries to acquire a lock
    client_1_data = {
        "key": "resource_lock",
        "client_id": "client_1",
        "expiry": 10  # seconds
    }
    response = requests.post(f"{base_url}/acquire_lock", json=client_1_data)
    print("Client 1 Acquire Lock Response:", response.json())

    # Client 2 tries to acquire the same lock
    client_2_data = {
        "key": "resource_lock",
        "client_id": "client_2",
        "expiry": 10  # seconds
    }
    response = requests.post(f"{base_url}/acquire_lock", json=client_2_data)
    print("Client 2 Acquire Lock Response:", response.json())

    # Client 1 checks lock status
    response = requests.get(f"{base_url}/lock_status", params={"key": "resource_lock"})
    print("Client 1 Lock Status Response:", response.json())

    # Client 1 releases the lock
    response = requests.post(f"{base_url}/release_lock", json={"key": "resource_lock"})
    print("Client 1 Release Lock Response:", response.json())

    # Client 2 tries to acquire the lock again
    response = requests.post(f"{base_url}/acquire_lock", json=client_2_data)
    print("Client 2 Acquire Lock After Release Response:", response.json())