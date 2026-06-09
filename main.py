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

@mcp.tool
def delete_expense(expense_id: int) -> str:
    '''Deletes an expense from the database by its ID.'''
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))

    return "Expense deleted successfully."

@mcp.tool
def update_expense(
    expense_id: int,
    amount: float,
    category: str,
    subcategory: str,
    date: str,
    note: str = ""
) -> str:
    """Update an existing expense."""

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE expenses
            SET amount = ?, category = ?, subcategory = ?, date = ?, note = ?
            WHERE id = ?
        """, (amount, category, subcategory, date, note, expense_id))

    return "Expense updated successfully."

@mcp.tool
def get_expenses_by_category(category: str) -> List[dict]:
    """Get expenses for a specific category."""

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, amount, category, subcategory, date, note
            FROM expenses
            WHERE category = ?
        """, (category,))
        rows = cursor.fetchall()

    return [
        {
            "id": row[0],
            "amount": row[1],
            "category": row[2],
            "subcategory": row[3],
            "date": row[4],
            "note": row[5]
        }
        for row in rows
    ]

@mcp.tool
def get_expenses_by_date(start_date: str, end_date: str) -> List[dict]:
    """Get expenses between two dates."""

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, amount, category, subcategory, date, note
            FROM expenses
            WHERE date BETWEEN ? AND ?
            ORDER BY date
        """, (start_date, end_date))

        rows = cursor.fetchall()

    return [
        {
            "id": row[0],
            "amount": row[1],
            "category": row[2],
            "subcategory": row[3],
            "date": row[4],
            "note": row[5]
        }
        for row in rows
    ]

@mcp.tool
def monthly_summary(month: str) -> dict:
    """
    month format: YYYY-MM
    Example: 2026-06
    """

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM expenses
            WHERE substr(date, 1, 7) = ?
        """, (month,))

        total = cursor.fetchone()[0]

    return {
        "month": month,
        "total_spent": total
    }

@mcp.tool
def category_breakdown(month: str) -> dict:
    """Get spending grouped by category for a month."""

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT category, SUM(amount)
            FROM expenses
            WHERE substr(date, 1, 7) = ?
            GROUP BY category
        """, (month,))

        rows = cursor.fetchall()

    return {
        category: total
        for category, total in rows
    }




if __name__ == "__main__":
    mcp.run()