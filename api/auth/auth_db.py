from typing import Optional

TOKENS = []


def set_token(user_id: int, access_token: str, refresh_token: str):
    record = {
        "user_id": user_id,
        "access_token": access_token,
        "refresh_token": refresh_token
    }

    TOKENS.append(record)


def get_refresh_token_record_by_user_id(user_id: int) -> Optional[str]:
    refresh_token_found = [t["refresh_token"] for t in TOKENS if t["user_id"] == user_id]

    if len(refresh_token_found) == 0:
        return None
    
    return refresh_token_found[0]


def verify_refresh_token(refresh_token: str) -> Optional[str]:
    refresh = [t["refresh_token"] for t in TOKENS if t["refresh_token"] == refresh_token]

    if len(refresh) == 0:
        return None
    
    return refresh[0]


def update_tokens(user_id: str, access_token: str):
    for record in TOKENS:
        if record["user_id"] == user_id:
            record["access_token"] = access_token