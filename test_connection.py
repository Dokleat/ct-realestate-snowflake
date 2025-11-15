"""
Test Snowflake Connection
Run: python test_connection.py
"""

import os
import snowflake.connector
from dotenv import load_dotenv

# Load .env file
load_dotenv()

print("\n--- TESTING SNOWFLAKE CONNECTION ---\n")

required_vars = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE"
]

# Show loaded ENV values (password hidden)
for var in required_vars:
    value = os.getenv(var)
    if var == "SNOWFLAKE_PASSWORD":
        print(f"{var}: {'*' * 10 if value else 'NOT SET'}")
    else:
        print(f"{var}: {value}")

missing = [v for v in required_vars if not os.getenv(v)]
if missing:
    print("\n✗ ERROR: Missing environment variables:")
    print(", ".join(missing))
    print("Fix your .env file and try again.\n")
    exit(1)

print("\nConnecting to Snowflake...\n")

try:
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA") or "PUBLIC",
        role=os.getenv("SNOWFLAKE_ROLE") or None,
    )

    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_USER()")
    result = cursor.fetchone()

    print("✓ CONNECTION SUCCESSFUL!\n")
    print(f"  Snowflake Version : {result[0]}")
    print(f"  Current Database  : {result[1]}")
    print(f"  Current User      : {result[2]}\n")

    cursor.close()
    conn.close()

except Exception as e:
    print("✗ CONNECTION FAILED!")
    print(f"  Error: {e}")
    print("\nPossible issues:")
    print(" - SNOWFLAKE_ACCOUNT format (usually: orgname-account)")
    print(" - Wrong password")
    print(" - Wrong warehouse/database")
    print(" - Network/VPN issues\n")