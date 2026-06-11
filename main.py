import os
from contextvars import ContextVar
from contextlib import asynccontextmanager
from contextlib import contextmanager
from typing import List
from fastapi import FastAPI
import uvicorn
from fastmcp import FastMCP
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
import jwt
from starlette.types import ASGIApp, Receive, Scope, Send
from dotenv import load_dotenv

import redis.asyncio as redis

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


if not all([DATABASE_URL, AUTH0_DOMAIN, AUTH0_AUDIENCE]):
    raise ValueError("Missing critical configuration in .env file (Database or Auth0 variables)")

# -----------------------------------------------------------------------------
# 1. THREAD-SAFE MULTI-TENANT CONTEXT LIFECYCLE
# -----------------------------------------------------------------------------
# ContextVar acts as an isolated, asynchronous thread-local storage channel.
tenant_context: ContextVar[str] = ContextVar("tenant_id", default=None)

# -----------------------------------------------------------------------------
# 2. DATABASE MANAGEMENT WITH POOLING & TENANT SCHEMA INDEXES
# -----------------------------------------------------------------------------
db_pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=2,
    max_size=10,
    open=False,
    kwargs={"row_factory": dict_row}
)

@contextmanager
def get_db_connection():
    """Leases a connection and securely injects the tenant ID into the Postgres transaction."""
    tenant_id = tenant_context.get()
    with db_pool.connection() as conn:
        if tenant_id:
            # SET LOCAL binds the variable strictly to this single transaction. 
            # When the transaction ends, the variable is safely destroyed.
            conn.execute("SET LOCAL app.current_tenant = %s", (tenant_id,))
        yield conn

def init_db():
    """Defines schema, indexes, and mathematically strict Row-Level Security."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS expenses (
                    id SERIAL PRIMARY KEY,
                    tenant_id VARCHAR(255) DEFAULT current_setting('app.current_tenant', true) NOT NULL,
                    amount NUMERIC(12, 2) NOT NULL,
                    category VARCHAR(100) NOT NULL,
                    subcategory VARCHAR(100) NOT NULL,
                    date DATE NOT NULL,
                    note TEXT DEFAULT ''
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_expenses_tenant_id ON expenses (tenant_id)")
            
            # 1. Lock the table down
            cursor.execute("ALTER TABLE expenses ENABLE ROW LEVEL SECURITY")
            
            # 2. Wipe existing policies to prevent reload crashes
            cursor.execute("DROP POLICY IF EXISTS tenant_isolation_policy ON expenses")
            
            # 3. Create the iron-clad rule: You can only touch rows you own.
            cursor.execute("""
                CREATE POLICY tenant_isolation_policy ON expenses 
                USING (tenant_id = current_setting('app.current_tenant', true))
            """)
        conn.commit()

# -----------------------------------------------------------------------------
# 3. HIGH-PERFORMANCE ASYNCHRONOUS AUTH0 ASGI MIDDLEWARE
# -----------------------------------------------------------------------------
class Auth0MultiTenantMiddleware:
    """
    Validates incoming Auth0 JWTs using JWKS key rotation caches 
    without disrupting FastMCP's SSE streaming pipelines.
    """
    def __init__(self, app: ASGIApp, domain: str, audience: str):
        self.app = app
        self.audience = audience
        self.issuer = f"https://{domain}/"
        # Caches Auth0 public keys locally; rotates automatically when needed
        self.jwks_client = jwt.PyJWKClient(f"{self.issuer}.well-known/jwks.json", lifespan=3600)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Pass non-HTTP protocols, health checks, or CORS preflights straight through
        if scope["type"] != "http" or scope["path"] == "/health" or scope.get("method") == "OPTIONS":
            await self.app(scope, receive, send)
            return

        # Intercept HTTP headers
        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"").decode("utf-8")

        if not auth_header or not auth_header.startswith("Bearer "):
            await self.reject_unauthorized(send)
            return

        token = auth_header.split(" ", 1)[1]
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(token)

            payload = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                audience=self.audience,
                issuer=self.issuer,
            )
            
            # Extract Tenant context. Uses Auth0 'sub' (User ID) as fallback tenant reference
            tenant_id = payload.get("https://api.expense-tracker.mcp/tenant_id") or payload.get("sub")
            
            # Bind the tenant token exclusively to this async execution stack trace
            context_token = tenant_context.set(tenant_id)
            try:
                await self.app(scope, receive, send)
            finally:
                tenant_context.reset(context_token)

        except Exception as e:
            import traceback

            print("==== AUTH ERROR ====")
            print(type(e))
            print(repr(e))
            traceback.print_exc()
            print("====================")

            await self.reject_unauthorized(send)
            return

    async def reject_unauthorized(self, send: Send):
        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [(b"content-type", b"application/json")]
        })
        await send({
            "type": "http.response.body",
            "body": b'{"detail": "Unauthorized: Invalid or missing Auth0 Token"}'
        })


# -----------------------------------------------------------------------------
# Redis IP-Based Rate Limiting Middleware
# -----------------------------------------------------------------------------

class RedisRateLimitMiddleware:
    """Blocks IP addresses that exceed 50 requests per minute."""
    def __init__(self, app: ASGIApp, max_requests: int = 50, window_seconds: int = 60):
        self.app = app
        self.max_requests = max_requests
        self.window = window_seconds

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Skip rate limiting for non-HTTP requests or health checks
        if scope["type"] != "http" or scope["path"] == "/health":
            await self.app(scope, receive, send)
            return

        # Extract Client IP
        client_ip = scope.get("client")[0] if scope.get("client") else "unknown"
        redis_key = f"rate_limit:{client_ip}"

        try:
            # Increment the request count for this IP
            requests = await redis_client.incr(redis_key)
            
            # If it's the first request, set the expiration timer (e.g., 60 seconds)
            if requests == 1:
                await redis_client.expire(redis_key, self.window)
            
            # If they exceeded the limit, block the request
            if requests > self.max_requests:
                await self.reject_rate_limit(send)
                return
        except Exception as e:
            # "Fail Open" pattern: If Redis goes down, allow the request rather than crashing the app
            print(f"⚠️ Redis Connection Error: {e}")
            
        await self.app(scope, receive, send)

    async def reject_rate_limit(self, send: Send):
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [(b"content-type", b"application/json")]
        })
        await send({
            "type": "http.response.body",
            "body": b'{"detail": "Too Many Requests. Please wait a minute."}'
        })



# -----------------------------------------------------------------------------
# 4. MCP IMPLEMENTATION WITH SCOPED DATA LEASE 
# -----------------------------------------------------------------------------
mcp = FastMCP("expense_tracker_mcp_server")

@mcp.tool
def add_expense(amount: float, category: str, subcategory: str, date: str, note: str = "") -> str:
    """Adds a new expense. The database automatically tags it with the active tenant_id."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO expenses (amount, category, subcategory, date, note)
                VALUES (%s, %s, %s, %s, %s)
            """, (amount, category, subcategory, date, note))
        conn.commit()
    return "Expense added securely."

@mcp.tool
def list_expenses() -> List[dict]:
    """Lists expenses. PostgreSQL RLS automatically filters out other users' data."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, amount, category, subcategory, date, note FROM expenses")
            expenses = cursor.fetchall()
            for exp in expenses:
                exp["amount"] = float(exp["amount"])
                exp["date"] = exp["date"].isoformat()
    return expenses

@mcp.tool
def delete_expense(expense_id: int) -> str:
    """Deletes an expense record. PostgreSQL RLS ensures only authorized records can be removed."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM expenses WHERE id = %s
            """, (expense_id,))
        conn.commit()
    return "Expense deleted successfully if record existed."

@mcp.tool
def update_expense(
    expense_id: int, amount: float, category: str, subcategory: str, date: str, note: str = ""
) -> str:
    """Updates an expense record. PostgreSQL RLS automatically restricts updates to authorized records."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE expenses
                SET amount = %s, category = %s, subcategory = %s, date = %s, note = %s
                WHERE id = %s
            """, (amount, category, subcategory, date, note, expense_id))
        conn.commit()
    return "Expense updated successfully."

@mcp.tool
def get_expenses_by_category(category: str) -> List[dict]:
    """Gets expenses filtered by category. PostgreSQL RLS automatically filters records by tenant."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, amount, category, subcategory, date, note
                FROM expenses
                WHERE category = %s
            """, (category,))
            expenses = cursor.fetchall()
            for exp in expenses:
                exp["amount"] = float(exp["amount"])
                exp["date"] = exp["date"].isoformat()
    return expenses

@mcp.tool
def get_expenses_by_date(start_date: str, end_date: str) -> List[dict]:
    """Gets expenses between two dates. PostgreSQL RLS automatically filters records by tenant."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, amount, category, subcategory, date, note
                FROM expenses
                WHERE date BETWEEN %s AND %s
                ORDER BY date
            """, (start_date, end_date))
            expenses = cursor.fetchall()
            for exp in expenses:
                exp["amount"] = float(exp["amount"])
                exp["date"] = exp["date"].isoformat()
    return expenses

@mcp.tool
def monthly_summary(month: str) -> dict:
    """Gets total spending for a month. PostgreSQL RLS automatically limits aggregation to authorized records."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM expenses
                WHERE to_char(date, 'YYYY-MM') = %s
            """, (month,))
            total = cursor.fetchone()[0]
    return {"month": month, "total_spent": float(total)}

@mcp.tool
def category_breakdown(month: str) -> dict:
    """Gets spending grouped by category for a month. PostgreSQL RLS automatically limits results to authorized records."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT category, COALESCE(SUM(amount), 0) AS total
                FROM expenses
                WHERE to_char(date, 'YYYY-MM') = %s
                GROUP BY category
            """, (month,))
            rows = cursor.fetchall()
    return {row["category"]: float(row["total"]) for row in rows}

# -----------------------------------------------------------------------------
# 5. LIFESPAN BOUNDS & FAST MCP HTTP APP
# -----------------------------------------------------------------------------
mcp_asgi_app = mcp.http_app()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Boot Database
    db_pool.open()
    init_db()
    print("🚀 Database pool operational and Multi-Tenant schemas verified.")
    
    # Boot FastMCP internal tasks (CRITICAL FOR STREAMING)
    async with mcp_asgi_app.router.lifespan_context(app):
        print("🚀 FastMCP Streamable Transport operational.")
        yield
        
    # Shutdown Database
    db_pool.close()
    print("🛑 Database pools terminated.")

# -----------------------------------------------------------------------------
# 6. APPLICATION ROUTING AND MIDDLEWARE BINDINGS
# -----------------------------------------------------------------------------
app = FastAPI(title="Enterprise Multi-Tenant Expense MCP", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {
        "status": "production automated!", 
        "tenancy": "ContextVar Isolation Active",
        "version": "1.0.0" 
    }

@app.get("/test-auth", tags=["Infrastructure"])
async def test_auth_context():
    tenant_id = tenant_context.get()
    return {
        "message": "Authentication Successful!",
        "active_tenant_id": tenant_id
    }

# Mount Auth0 Token Interception Layer
app.add_middleware(Auth0MultiTenantMiddleware, domain=AUTH0_DOMAIN, audience=AUTH0_AUDIENCE)

# Mount Redis Rate Limiter (Runs before Auth0)
app.add_middleware(RedisRateLimitMiddleware, max_requests=60, window_seconds=30) 

# Mount the MCP ASGI App at the root wildcard.
# This prevents Starlette from stripping the path, allowing 
# FastMCP to receive exactly "/mcp" and route it correctly!
app.mount("/", mcp_asgi_app)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)