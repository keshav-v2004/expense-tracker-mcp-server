import sys
import asyncio
import json
import httpx
from dotenv import load_dotenv
import os
load_dotenv()

AWS_IP = os.getenv("AWS_IP")
AUTH0_TOKEN = os.getenv("AUTH0_TOKEN")

if not AWS_IP or not AUTH0_TOKEN:
    print("Error: Missing AWS_IP or AUTH0_TOKEN in .env file")
    sys.exit(1)

REMOTE_URL = f"http://{AWS_IP}:8000/mcp/mcp" 

async def main():
    headers = {
        "Authorization": f"Bearer {AUTH0_TOKEN}",
        "Content-Type": "application/json"
    }

    # Create a local log file to catch AWS errors
    with open("bridge_debug.log", "w", encoding="utf-8") as log:
        log.write("Bridge initialized. Waiting for Claude...\n")

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                line = await asyncio.to_thread(sys.stdin.readline)
                if not line:
                    break
                    
                try:
                    request = json.loads(line)
                    log.write(f"-> Sending to AWS: {json.dumps(request)}\n")
                    log.flush()

                    response = await client.post(REMOTE_URL, json=request, headers=headers)
                    
                    log.write(f"<- AWS Response [{response.status_code}]: {response.text}\n")
                    log.flush()

                    # Only forward successful JSON to Claude
                    if response.status_code == 200:
                        sys.stdout.write(json.dumps(response.json()) + "\n")
                        sys.stdout.flush()
                    else:
                        # Prevent Claude from crashing by not sending it the HTTP error
                        sys.stderr.write(f"HTTP {response.status_code} Error. Check bridge_debug.log\n")
                        sys.stderr.flush()

                except Exception as e:
                    log.write(f"!! Exception: {str(e)}\n")
                    log.flush()

if __name__ == "__main__":
    asyncio.run(main())