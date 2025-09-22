"""
    This module is due to simulate the DB for this POC.
"""

from typing import Optional

USERS = [
    {"id": 1, "username": "mlet", "password": "mlet", "role": "admin"},
    {"id": 2, "username": "user", "password": "user", "role": "customer"},
    {"id": 3, "username": "ds", "password": "ds", "role": "ds"},
    {"id": 4, "username": "smoke", "password": "smoke", "role": "test"}
]

def verify_user(username: str, password: str) -> Optional[dict]:
    user_found = [u for u in USERS if u["username"] == username and u["password"] == password]

    if len(user_found) == 0:
        return None    
    
    return user_found[0]


def get_user_by_id(user_id: str) -> Optional[dict]:
    user_found = [u for u in USERS if u["id"] == user_id]

    if len(user_found) == 0:
        return None
    
    return user_found[0]
    
    