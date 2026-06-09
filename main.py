from fastmcp import FastMCP
from typing import List
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "expenses.db")

mcp = FastMCP("expense_tracker_mcp_server")

def init_db():
    '''Initializes the database and creates the expenses table if it doesn't exist.'''
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount REAL NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT NOT NULL,
                date TEXT NOT NULL,
                note TEXT DEFAULT ''
            )
        """)

init_db()

@mcp.tool
def add_expense(amount: float, category: str, subcategory: str, date: str, note: str = "") -> str:
    '''Adds a new expense to the database.'''
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO expenses (amount, category, subcategory, date, note)
            VALUES (?, ?, ?, ?, ?)
        """, (amount, category, subcategory, date, note))

    return "Expense added successfully."

@mcp.tool
def list_expenses() -> List[dict]:
    '''Lists all expenses in the database.'''
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, amount, category, subcategory, date, note FROM expenses")
        rows = cursor.fetchall()

    expenses = []
    for row in rows:
        expenses.append({
            "id": row[0],
            "amount": row[1],
            "category": row[2],
            "subcategory": row[3],
            "date": row[4],
            "note": row[5]
        })
    
    return expenses

if __name__ == "__main__":
    mcp.run()