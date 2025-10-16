# src/connectors/oracle_connector.py
import os
from dotenv import load_dotenv
import oracledb
from typing import Optional

load_dotenv()

def build_oracle_dsn():
    dsn = os.getenv("ORACLE_DSN")
    if dsn:
        return dsn.strip()
    host = os.getenv("ORACLE_HOST", "").strip()
    port = os.getenv("ORACLE_PORT", "1521").strip()
    service = os.getenv("ORACLE_SERVICE", "").strip()
    if not (host and service):
        raise ValueError("Either ORACLE_DSN or ORACLE_HOST+ORACLE_SERVICE must be set in .env")
    return f"{host}:{port}/{service}"

class OracleConnector:
    def __init__(self, user_env="ORACLE_USER", pwd_env="ORACLE_PASSWORD"):
        self.user = os.getenv(user_env)
        self.pwd = os.getenv(pwd_env)
        self.dsn = build_oracle_dsn()
        if not (self.user and self.pwd):
            raise ValueError("ORACLE_USER and ORACLE_PASSWORD must be set in .env")

    def get_connection(self) -> Optional[oracledb.Connection]:
        try:
            return oracledb.connect(user=self.user, password=self.pwd, dsn=self.dsn)
        except Exception as exc:
            raise RuntimeError(f"[OracleConnector] connection failed: {exc}")
