from abc import ABC, abstractmethod
from api.domain.users.entity import User
from typing import Optional

class UserRepositoryInterface(ABC):
    @abstractmethod
    def get_user_by_username_and_password(self, username: str, password: str) -> Optional[User]:
        pass

    @abstractmethod
    def get_user_by_id(self, user_id: int) -> Optional[User]:
        pass