import aiosqlite
from pathlib import Path
from typing import List, Dict, Any, Optional

DB_PATH = Path(__file__).with_suffix(".db")

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL,
                email_password TEXT NOT NULL,
                proxy TEXT,
                status TEXT DEFAULT 'new',
                last_activity TIMESTAMP,
                folder_id INTEGER,
                FOREIGN KEY (folder_id) REFERENCES folders (id)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
            """
        )
        await db.commit()

async def add_account(username: str, password: str, email: str, email_password: str, proxy: Optional[str] = None, folder_id: Optional[int] = None) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                """
                INSERT INTO accounts (username, password, email, email_password, proxy, folder_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (username, password, email, email_password, proxy, folder_id)
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def get_accounts() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM accounts")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def update_account_status(account_id: int, status: str, last_activity: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        query = """
            UPDATE accounts 
            SET status = ?, last_activity = ?
            WHERE id = ?
        """
        await db.execute(query, (status, last_activity, account_id))
        await db.commit()

async def get_account_by_id(account_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM accounts WHERE id = ?", (account_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_folders() -> List[Dict[str, Any]]:
    """Получает все папки из базы данных."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT id, name FROM folders ORDER BY name")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def add_folder(name: str) -> Optional[int]:
    """Добавляет новую папку и возвращает ее ID, или None в случае ошибки."""
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            cursor = await db.execute("INSERT INTO folders (name) VALUES (?)", (name,))
            await db.commit()
            return cursor.lastrowid
        except aiosqlite.IntegrityError:
            # Папка с таким именем уже существует
            return None
