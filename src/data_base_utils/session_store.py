"""
SQLite 存储 chat_id -> session_id 映射
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "data" / "sessions.db"


def _get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            chat_id TEXT PRIMARY KEY,
            session_id TEXT,
            workspace TEXT DEFAULT ''
        )
    """)
    # 迁移：旧表可能没有 workspace 列
    cursor = conn.execute("PRAGMA table_info(sessions)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'workspace' not in columns:
        conn.execute("ALTER TABLE sessions ADD COLUMN workspace TEXT DEFAULT ''")
    conn.commit()
    return conn


def get_session(chat_id: str) -> str | None:
    """获取 session_id"""
    conn = _get_conn()
    cursor = conn.execute("SELECT session_id FROM sessions WHERE chat_id = ?", (chat_id,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def save_session(chat_id: str, session_id: str):
    """保存 session_id（保留已有的 workspace）"""
    conn = _get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO sessions (chat_id, session_id, workspace)
        SELECT ?, ?, COALESCE((SELECT workspace FROM sessions WHERE chat_id = ?), '')
    """, (chat_id, session_id, chat_id))
    conn.commit()
    conn.close()


def get_workspace(chat_id: str) -> str | None:
    """获取工作空间路径"""
    conn = _get_conn()
    cursor = conn.execute("SELECT workspace FROM sessions WHERE chat_id = ?", (chat_id,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def save_workspace(chat_id: str, workspace: str):
    """保存工作空间路径"""
    conn = _get_conn()
    # 确保 chat_id 存在
    conn.execute("""
        INSERT OR IGNORE INTO sessions (chat_id, session_id, workspace)
        VALUES (?, '', ?)
    """, (chat_id, workspace))
    conn.execute("""
        UPDATE sessions SET workspace = ? WHERE chat_id = ?
    """, (workspace, chat_id))
    conn.commit()
    conn.close()
