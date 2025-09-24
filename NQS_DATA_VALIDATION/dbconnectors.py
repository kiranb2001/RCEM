import configparser
import clickhouse_connect
from trino.dbapi import connect as trino_connect

def load_properties(file_path: str):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

# Connection to Clickhouse
def connect_clickhouse(config):
    """Connect to ClickHouse using clickhouse-connect (pure Python)"""
    try:
        client = clickhouse_connect.get_client(
            host=config.get("clickhouse", "host"),
            port=config.getint("clickhouse", "port"),
            username=config.get("clickhouse", "user"),
            password=config.get("clickhouse", "password"),
            database=config.get("clickhouse", "database")
        )
        print("✅ ClickHouse connection successful")
        return client
    except Exception as e:
        print("❌ ClickHouse connection failed:", e)
        return None   # Important!

# Connection to Trino

def connect_trino(config):
    """Connect to Trino using trino.dbapi.connect"""
    try:
        conn = trino_connect(
            host=config.get("trino", "host"),
            port=config.getint("trino", "port"),
            user=config.get("trino", "user"),
            catalog=config.get("trino", "catalog"),
            schema=config.get("trino", "schema")
        )
        print("✅ Trino connection successful")
        return conn
    except Exception as e:
        print("❌ Trino connection failed:", e)
        return None
