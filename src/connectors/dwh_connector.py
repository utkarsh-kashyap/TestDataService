# src/connectors/dwh_connector.py
import os
from dotenv import load_dotenv
import pyodbc
from typing import Optional

load_dotenv()

class DWHConnector:
    def __init__(self):
        self.driver = os.getenv("DWH_DRIVER", "ODBC Driver 17 for SQL Server")
        self.server = os.getenv("DWH_SERVER")
        self.database = os.getenv("DWH_DATABASE")
        self.username = os.getenv("DWH_USERNAME")
        self.password = os.getenv("DWH_PASSWORD")
        self.trusted = os.getenv("DWH_TRUSTED_CONNECTION", "no").lower() in ("yes", "true", "1")

        if not (self.server and self.database):
            raise ValueError("DWH_SERVER and DWH_DATABASE must be set in .env")

        if self.trusted:
            self.conn_str = f"DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;"
        else:
            if not (self.username and self.password):
                raise ValueError("DWH_USERNAME and DWH_PASSWORD must be set in .env")
            self.conn_str = (
                f"DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database};"
                f"UID={self.username};PWD={self.password};"
            )

    def get_connection(self) -> Optional[pyodbc.Connection]:
        try:
            return pyodbc.connect(self.conn_str, autocommit=False)
        except Exception as exc:
            raise RuntimeError(f"[DWHConnector] connection failed: {exc}")
