from abc import ABC, abstractmethod
from typing import Optional

class AuthRepositoryInterface(ABC):
    @abstractmethod
    def set_token(self, user_id: int, access_token: str, refresh_token: str):
        pass

    @abstractmethod
    def get_token_record_by_refresh_token(self, refresh_token: str) -> Optional[dict]:
        pass

    @abstractmethod
    def update_tokens(self, user_id: int, new_access_token: str):
        pass