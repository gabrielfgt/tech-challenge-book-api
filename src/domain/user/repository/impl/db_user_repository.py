from typing import Optional
from src.domain.user.model.user import User
from src.domain.user.repository.abstract.user_repository import UserRepositoryInterface
import psycopg2
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DBUserRepository(UserRepositoryInterface):
    host = None
    port = None
    dbname = None
    user = None
    password = None

    def __init__(self):
        logging.info("Initializing Real Database repository for users")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT", 5432)
        self.dbname = os.getenv("DB_NAME", "book-api")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")

    def get_user_by_username_and_password(self, username: str, password: str) -> Optional[User]:
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )

            cur = conn.cursor()

            cur.execute("""
                SELECT id, username, role
                FROM users
                WHERE username = %s AND password = %s
                LIMIT 1;
            """, (username, password))

            user = cur.fetchone()

            return User(
                id=user[0],
                username=user[1],
                role=user[2]
            )

        except Exception as e:            
            logger.error(f"Failed to find user {username} on database")
            logger.error(e)
            return None

        finally:
            cur.close()
            conn.close()        
        

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )

            cur = conn.cursor()

            cur.execute("""
                SELECT id, username, role
                FROM users
                WHERE id = %s
                LIMIT 1;
            """, (user_id,))

            user = cur.fetchone()

            return User(
                id=user[0],
                username=user[1],
                role=user[2]
            )

        except Exception as e:
            logger.warning(f"Failed to find user id {user_id} on database")
            return None

        finally:
            cur.close()
            conn.close()   
