from typing import Optional
from src.domain.user.repository.abstract.user_repository import UserRepositoryInterface
from src.domain.user.model.user import User

class UserService:
    def __init__(self, repository: UserRepositoryInterface):
        self.repository = repository


    def verify_user(self, username: str, password: str) -> Optional[User]:
        return self.repository.get_user_by_username_and_password(username=username, password=password)


    def get_user_by_id(self, user_id: int) -> Optional[User]:
        return self.repository.get_user_by_id(user_id=user_id)