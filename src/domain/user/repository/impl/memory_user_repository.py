import logging
from typing import Optional
from src.domain.user.model.user import User
from src.domain.user.repository.abstract.user_repository import UserRepositoryInterface

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InMemoryUserRepository(UserRepositoryInterface):
    def __init__(self):
        logging.info("Initializing In Memory Database repository for users")
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