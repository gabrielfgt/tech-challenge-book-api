from typing import Optional
from src.domain.auth.repository.abstract.auth_repository import AuthRepositoryInterface
import logging


class InMemoryAuthRepository(AuthRepositoryInterface):
    TOKENS = []

    def __init__(self):
        logging.info("Initializing In Memory Auth Database repository")


    def set_token(self, user_id: int, access_token: str, refresh_token: str):
        record = {
            "user_id": user_id,
            "access_token": access_token,
            "refresh_token": refresh_token
        }

        self.TOKENS.append(record)


    def get_refresh_token_record_by_user_id(self, user_id: int) -> Optional[str]:
        refresh_token_found = [t["refresh_token"] for t in self.TOKENS if t["user_id"] == user_id]

        if len(refresh_token_found) == 0:
            return None
        
        return refresh_token_found[0]

    def get_token_record_by_refresh_token(self, refresh_token: str) -> Optional[dict]:
        token_record_found = [t for t in self.TOKENS if t["refresh_token"] == refresh_token]

        if len(token_record_found) == 0:
            return None
        
        return token_record_found[0]


    def verify_refresh_token(self, refresh_token: str) -> Optional[str]:
        refresh = [t["refresh_token"] for t in self.TOKENS if t["refresh_token"] == refresh_token]

        if len(refresh) == 0:
            return None
        
        return refresh[0]


    def update_tokens(self, user_id: str, access_token: str):
        for record in self.TOKENS:
            if record["user_id"] == user_id:
                record["access_token"] = access_token