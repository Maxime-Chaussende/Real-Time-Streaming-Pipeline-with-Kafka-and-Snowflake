import snowflake.connector
import os
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


def get_snowflake_connection():

     # 1. Lire la clé privée
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    with open(private_key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None
        )
    
    # 2. Sérialiser la clé pour snowflake-connector
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    print(f"Type of private_key_bytes: {type(private_key_bytes)}")
    print(f"private_key_path: {private_key_path}")

    # 3. Se connecter à Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=private_key_bytes,
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )

    return conn