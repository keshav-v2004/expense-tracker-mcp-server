import os
import sys
from fastmcp import Client
from fastmcp.client.auth import BearerAuth
from fastmcp.server import create_proxy
from dotenv import load_dotenv

# --- BULLETPROOF ENV LOADING ---
# Forces Python to look for the .env file in the exact same folder as this script
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, ".env"))

AWS_IP = os.getenv("AWS_IP")
AUTH0_TOKEN = os.getenv("AUTH0_TOKEN")

if not AWS_IP or not AUTH0_TOKEN:
    # Print to stderr so Claude can read the error without crashing the JSON stream
    print("Error: Missing AWS_IP or AUTH0_TOKEN in .env file", file=sys.stderr)
    sys.exit(1)

# The exact URL for StreamableHttp FastMCP proxying
REMOTE_URL = f"http://{AWS_IP}:8000/mcp" 

try:
    # 1. Create a FastMCP client that natively handles the StreamableHttp handshake
    client = Client(
        REMOTE_URL,
        auth=BearerAuth(token=AUTH0_TOKEN)
    )

    # 2. Create the built-in proxy that translates Claude's local STDIO into remote streaming
    proxy = create_proxy(client, name="AWS Expense Tracker")

except Exception as e:
    print(f"Failed to initialize proxy: {e}", file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    # 3. Run the proxy!
    proxy.run()