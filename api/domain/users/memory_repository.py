from typing import Optional
from api.domain.users.entity import User
from api.domain.users.abstract.repository import UserRepositoryInterface

class InMemoryUserRepository(UserRepositoryInterface):
    def __init__(self, connection: any):
        self.connection = connection
        self.table = [
            {"id": 1, "username": "mlet", "password": "mlet", "role": "admin"},
            {"id": 2, "username": "user", "password": "user", "role": "customer"},
            {"id": 3, "username": "ds", "password": "ds", "role": "ds"},
            {"id": 4, "username": "smoke", "password": "smoke", "role": "test"}
        ]

    def get_user_by_username_and_password(self, username: str, password: str) -> Optional[User]:
        user_found = [u for u in self.table if u["username"] == username and u["password"] == password]

        if len(user_found) == 0:
            return None    
        
        return User(
            id=user_found[0]["id"],
            username=user_found[0]["username"],
            role=user_found[0]["role"]
        )
        
        

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        user_found = [u for u in self.table if u["id"] == user_id]

        if len(user_found) == 0:
            return None
        
        return User(
            id=user_found[0]["id"],
            username=user_found[0]["username"],
            role=user_found[0]["role"]
        )