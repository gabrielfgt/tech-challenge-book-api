from typing import Optional
import psycopg2
import os
import logging
from src.domain.auth.repository.abstract.auth_repository import AuthRepositoryInterface

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DBAuthRepository(AuthRepositoryInterface):
    host = None
    port = None
    dbname = None
    user = None
    password = None

    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT", 5432)
        self.dbname = os.getenv("DB_NAME", "book-api")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")

        logging.info(f"Initializing Real Auth Database repository host={self.host}")


    def set_token(self, user_id: int, access_token: str, refresh_token: str) -> Optional[int]:
        logger.info(f"Connection on host: {self.host}")
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )

            query_existing_token = """
                SELECT user_id, access_token, refresh_token
                FROM jwt_tokens
                WHERE user_id = %s
            """            

            cur = conn.cursor()            
            
            cur.execute(query_existing_token, (user_id,))

            record = cur.fetchone()

            save_command = None

            if record is None:
                save_command = """
                    INSERT INTO jwt_tokens(access_token, refresh_token, user_id)
                    VALUES(%s, %s, %s)
                    RETURNING id;
                """
            else:
                save_command = """
                    UPDATE jwt_tokens 
                    SET access_token = %s,
                        refresh_token = %s
                    WHERE user_id = %s
                    RETURNING id;
                """

            cur.execute(save_command, (access_token, refresh_token, user_id))           

            record_id = cur.fetchone()[0]

            conn.commit()

            return record_id

        except Exception as e:
            logger.error(f"Failed to save jwt tokens for user_id {user_id} on database")
            logger.error(e)
            return None

        finally:
            cur.close()
            conn.close()        
        

    def get_token_record_by_refresh_token(self, refresh_token: str) -> Optional[dict]:
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )

            cur = conn.cursor()

            query = """
                SELECT id, user_id, access_token, refresh_token
                FROM jwt_tokens
                WHERE refresh_token = %s
                LIMIT 1;
            """

            cur.execute(query, (refresh_token,))

            record = cur.fetchone()

            return {
                "id": record[0],
                "user_id": record[1],
                "access_token": record[2],
                "refresh_token": record[3]
            }

        except Exception as e:
            logger.error(f"Failed to find jwt token record for refresh token on database")
            logger.error(e)
            return None

        finally:
            cur.close()
            conn.close()

    def update_tokens(self, user_id: int, new_access_token: str):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )

            cur = conn.cursor()

            query = """
                UPDATE jwt_tokens
                SET access_token = %s
                WHERE user_id = %s
            """

            cur.execute(query, (new_access_token, user_id))

            conn.commit()

        except Exception as e:
            logger.warning(f"Failed to update new access token for user id {user_id}")
            logger.error(e)
            return None

        finally:
            cur.close()
            conn.close()