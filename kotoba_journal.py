import csv
import json
import logging
import queue
import random
import re
import sqlite3
import threading
import time
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    import pandas as pd
except ImportError:
    pd = None

from janome.tokenizer import Tokenizer


APP_TITLE = "Kotoba Journal"
APP_SUBTITLE = "日语单词手帐 Pro"
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "kotoba_journal.db"
LOG_DIR = BASE_DIR / "logs"
BACKUP_DIR = BASE_DIR / "backups"
DEFAULT_REVIEW_STAGES = [10, 1440, 2880, 10080, 43200]
COMMON_SYSTEM_DICT = {
    "もらい": "得到，收到",
    "友達": "朋友",
    "一緒": "一起",
    "図書館": "图书馆",
    "行く": "去",
    "行っ": "去",
    "読み": "读，阅读",
    "読む": "读，阅读",
    "日本": "日本",
    "日本語": "日语",
    "文化": "文化",
    "とても": "非常，很",
    "面白い": "有趣的",
    "食べ物": "食物",
    "美味しい": "好吃的，美味的",
    "音楽": "音乐",
    "素晴らしい": "精彩的，极好的",
    "技術": "技术",
    "勉強": "学习",
    "毎日": "每天",
    "よう": "样子，方式",
    "先生": "老师",
    "学生": "学生",
    "学校": "学校",
    "映画": "电影",
    "時間": "时间",
    "今日": "今天",
    "明日": "明天",
    "昨日": "昨天",
    "大切": "重要",
    "好き": "喜欢",
    "本": "书",
    "見る": "看",
    "作る": "制作",
    "食べる": "吃",
}

THEMES = {
    "mist_blue": {
        "name": "雾蓝手帐",
        "bg": "#EEF2F4",
        "panel": "#F8F7F3",
        "card": "#FFFDFC",
        "sidebar": "#D9E2E7",
        "sidebar_dark": "#A6B9C6",
        "line": "#DCE2E0",
        "text": "#2F3941",
        "muted": "#70808A",
        "accent": "#7F9BAC",
        "accent_soft": "#E6EDF1",
        "wood": "#96755C",
        "wood_soft": "#ECE0D5",
        "good": "#6D8A74",
        "warn": "#C59774",
        "danger": "#BA7575",
    },
    "fog_gray": {
        "name": "雾灰手帐",
        "bg": "#F5F5F7",
        "panel": "#FBFBFC",
        "card": "#FFFFFF",
        "sidebar": "#E6E7EB",
        "sidebar_dark": "#BCC0C9",
        "line": "#E4E4E7",
        "text": "#333333",
        "muted": "#7A7D84",
        "accent": "#8A8F98",
        "accent_soft": "#EEF0F3",
        "wood": "#9D8571",
        "wood_soft": "#EFE5DC",
        "good": "#6D8B76",
        "warn": "#B38B6F",
        "danger": "#B97878",
    },
    "rice_cream": {
        "name": "奶糯米白",
        "bg": "#FAFAF8",
        "panel": "#FFFDF9",
        "card": "#FFFFFF",
        "sidebar": "#F1EEE7",
        "sidebar_dark": "#D8C9B8",
        "line": "#F2F2EF",
        "text": "#2D2D2D",
        "muted": "#76706A",
        "accent": "#D4B896",
        "accent_soft": "#F6EFE7",
        "wood": "#B0835A",
        "wood_soft": "#F2E5D8",
        "good": "#7B8D70",
        "warn": "#C29466",
        "danger": "#B97A7A",
    },
    "blue_breeze": {
        "name": "青岚淡蓝",
        "bg": "#F0F7FF",
        "panel": "#F8FBFF",
        "card": "#FFFFFF",
        "sidebar": "#DFEAFA",
        "sidebar_dark": "#9CB9D7",
        "line": "#E6F0FF",
        "text": "#2F3437",
        "muted": "#6D7881",
        "accent": "#7BA7CC",
        "accent_soft": "#EAF3FC",
        "wood": "#8A745F",
        "wood_soft": "#ECE3DB",
        "good": "#68887A",
        "warn": "#C18E67",
        "danger": "#B77777",
    },
    "evening_ink": {
        "name": "夜墨护眼",
        "bg": "#1E2428",
        "panel": "#232B31",
        "card": "#2A333A",
        "sidebar": "#1C2328",
        "sidebar_dark": "#151B1F",
        "line": "#3B474F",
        "text": "#E8E2D8",
        "muted": "#B3B8BC",
        "accent": "#8FAFC2",
        "accent_soft": "#34424C",
        "wood": "#C0A080",
        "wood_soft": "#4A4037",
        "good": "#8FB39A",
        "warn": "#D1A27E",
        "danger": "#C98E8E",
    },
}

QUOTES = [
    ("千里の道も一歩から", "千里之行，始于足下"),
    ("失敗は成功のもと", "失败是成功之母"),
    ("継続は力なり", "坚持就是力量"),
    ("石の上にも三年", "功夫不负有心人"),
    ("努力に勝る天才なし", "没有比努力更厉害的天才"),
    ("苦あれば楽あり", "有苦才有乐"),
    ("時は金なり", "一寸光阴一寸金"),
    ("天は自ら助くる者を助く", "天助自助者"),
    ("七転び八起き", "百折不挠"),
    ("明日からがんばる", "从明天开始努力，但最好从今天"),
]

ENCOURAGEMENTS = {
    "correct": ["答对了，记忆在稳定下来。", "这一题很稳，状态不错。", "很好，这个词开始变得熟悉了。"],
    "close": ["已经很接近了。", "方向是对的，只差一点点。"],
    "wrong": ["这题容易混，没关系，我们稍后再见它一次。", "先别急，这类词本来就需要多见几次。"],
    "finish": ["今天已经有扎实进步了。", "节奏很好，继续这样会越来越轻松。"],
}

POS_MAP_UI_TO_DB = {
    "名词": "名詞",
    "动词": "動詞",
    "形容词": "形容詞",
    "副词": "副詞",
}

POS_CHOICES = ["全部词性", "名词", "动词", "形容词", "副词"]


def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8")],
    )


setup_logging()


def now_ts():
    return time.time()


def fmt_ts(ts):
    if not ts:
        return "未记录"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def today_str():
    return date.today().isoformat()


def safe_message(kind, title, message):
    try:
        if kind == "error":
            messagebox.showerror(title, message)
        elif kind == "warning":
            messagebox.showwarning(title, message)
        else:
            messagebox.showinfo(title, message)
    except Exception:
        logging.exception("弹窗失败: %s - %s", title, message)


class DBHelper:
    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self._ensure_dirs()
        self._init_db()
        self.migrate_legacy_files()

    def _ensure_dirs(self):
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    @contextmanager
    def transaction(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self.transaction() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS system_dict (
                    word TEXT PRIMARY KEY,
                    meaning TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS text_notes (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    content TEXT NOT NULL DEFAULT '',
                    segments TEXT NOT NULL DEFAULT '[]',
                    highlights TEXT NOT NULL DEFAULT '[]',
                    updated_at REAL NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS vocab (
                    word TEXT PRIMARY KEY,
                    meaning TEXT NOT NULL,
                    reading TEXT DEFAULT '',
                    base_form TEXT DEFAULT '',
                    pos TEXT DEFAULT '',
                    tags TEXT DEFAULT '',
                    example TEXT DEFAULT '',
                    priority INTEGER NOT NULL DEFAULT 1,
                    notes TEXT DEFAULT '',
                    created_at REAL NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL DEFAULT 0,
                    polite_form TEXT DEFAULT '',
                    te_form TEXT DEFAULT '',
                    ta_form TEXT DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS reviews (
                    word TEXT PRIMARY KEY,
                    last_review_at REAL NOT NULL DEFAULT 0,
                    due_at REAL NOT NULL DEFAULT 0,
                    review_count INTEGER NOT NULL DEFAULT 0,
                    correct_count INTEGER NOT NULL DEFAULT 0,
                    wrong_count INTEGER NOT NULL DEFAULT 0,
                    stage_index INTEGER NOT NULL DEFAULT 0,
                    streak INTEGER NOT NULL DEFAULT 0,
                    wrong_streak INTEGER NOT NULL DEFAULT 0,
                    mastered INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(word) REFERENCES vocab(word) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS mistakes (
                    word TEXT PRIMARY KEY,
                    meaning TEXT NOT NULL,
                    wrong_count INTEGER NOT NULL DEFAULT 0,
                    last_wrong_at REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY(word) REFERENCES vocab(word) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS test_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    total INTEGER NOT NULL,
                    correct INTEGER NOT NULL,
                    accuracy REAL NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'test'
                );
                CREATE TABLE IF NOT EXISTS deleted_vocab (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    deleted_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS checkin (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_date TEXT NOT NULL DEFAULT '',
                    streak INTEGER NOT NULL DEFAULT 0
                );
                INSERT OR IGNORE INTO text_notes (id, content, segments, highlights, updated_at)
                VALUES (1, '', '[]', '[]', 0);
                INSERT OR IGNORE INTO checkin (id, last_date, streak) VALUES (1, '', 0);
                """
            )
        self._seed_settings()

    def _seed_settings(self):
        defaults = {
            "theme": "mist_blue",
            "font_size_cn": "11",
            "font_size_jp": "13",
            "daily_review_limit": "15",
            "daily_new_limit": "5",
            "review_stages": ",".join(str(x) for x in DEFAULT_REVIEW_STAGES),
            "fade_enabled": "1",
            "furigana_default": "0",
            "auto_save_textlab": "1",
            "sidebar_collapsed": "0",
        }
        with self.transaction() as conn:
            for key, value in defaults.items():
                conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
            conn.executemany(
                "INSERT OR IGNORE INTO system_dict (word, meaning) VALUES (?, ?)",
                list(COMMON_SYSTEM_DICT.items()),
            )

    def is_word_in_vocab(self, word: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM vocab WHERE word = ? LIMIT 1", (word.strip(),)).fetchone()
        return row is not None

    def get_setting(self, key, default=None):
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key, value):
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )

    def get_all_settings(self):
        with self._connect() as conn:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {row["key"]: row["value"] for row in rows}

    def migrate_legacy_files(self):
        migrated_flag = self.get_setting("legacy_migrated", "0")
        if migrated_flag == "1":
            return
        try:
            self._import_legacy_dict(BASE_DIR / "dict.txt")
            self._import_legacy_vocab(BASE_DIR / "words.txt")
            self._import_legacy_reviews(BASE_DIR / "review_record.txt")
            self._import_legacy_tests(BASE_DIR / "test_records.txt")
            self._import_legacy_checkin(BASE_DIR / "checkin.txt")
            self._import_legacy_text(BASE_DIR / "last_text.txt")
            self._import_legacy_mistakes(BASE_DIR / "mistakes.json")
            self.set_setting("legacy_migrated", "1")
            logging.info("旧文件迁移完成")
        except Exception:
            logging.exception("旧数据迁移失败")

    def _import_legacy_dict(self, path: Path):
        if not path.exists():
            return
        with self.transaction() as conn:
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                parts = line.strip().split(maxsplit=1)
                if len(parts) == 2:
                    conn.execute(
                        "INSERT OR IGNORE INTO system_dict (word, meaning) VALUES (?, ?)",
                        (parts[0], parts[1]),
                    )

    def _import_legacy_vocab(self, path: Path):
        if not path.exists():
            return
        current = now_ts()
        with self.transaction() as conn:
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                parts = line.strip().split(maxsplit=1)
                if len(parts) != 2:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO vocab
                    (word, meaning, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (parts[0], parts[1], current, current),
                )
                conn.execute("INSERT OR IGNORE INTO reviews (word, due_at) VALUES (?, 0)", (parts[0],))

    def _import_legacy_reviews(self, path: Path):
        if not path.exists():
            return
        with self.transaction() as conn:
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                parts = line.strip().split()
                if len(parts) != 5:
                    continue
                try:
                    word, last, count, stage, mastered = parts
                    conn.execute(
                        """
                        INSERT INTO reviews
                        (word, last_review_at, due_at, review_count, stage_index, mastered)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(word) DO UPDATE SET
                            last_review_at=excluded.last_review_at,
                            review_count=excluded.review_count,
                            stage_index=excluded.stage_index,
                            mastered=excluded.mastered
                        """,
                        (word, float(last), 0, int(count), int(stage), 1 if mastered == "True" else 0),
                    )
                except Exception:
                    continue

    def _import_legacy_tests(self, path: Path):
        if not path.exists():
            return
        with self.transaction() as conn:
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                parts = line.strip().split(",")
                if len(parts) != 4:
                    continue
                try:
                    conn.execute(
                        """
                        INSERT INTO test_records (created_at, total, correct, accuracy, mode)
                        VALUES (?, ?, ?, ?, 'test')
                        """,
                        (float(parts[0]), int(parts[1]), int(parts[2]), float(parts[3])),
                    )
                except Exception:
                    continue

    def _import_legacy_checkin(self, path: Path):
        if not path.exists():
            return
        lines = [line.strip() for line in path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
        if not lines:
            return
        last = lines[0]
        streak = int(lines[1]) if len(lines) > 1 and lines[1].isdigit() else 0
        with self.transaction() as conn:
            conn.execute("UPDATE checkin SET last_date = ?, streak = ? WHERE id = 1", (last, streak))

    def _import_legacy_text(self, path: Path):
        if not path.exists():
            return
        content = path.read_text(encoding="utf-8", errors="ignore")
        with self.transaction() as conn:
            conn.execute("UPDATE text_notes SET content = ?, updated_at = ? WHERE id = 1", (content, now_ts()))

    def _import_legacy_mistakes(self, path: Path):
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            return
        with self.transaction() as conn:
            for word, item in data.items():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO mistakes (word, meaning, wrong_count, last_wrong_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        word,
                        item.get("meaning", ""),
                        int(item.get("wrong_count", 0)),
                        float(item.get("last_wrong", 0)),
                    ),
                )

    def load_system_dict(self):
        with self._connect() as conn:
            rows = conn.execute("SELECT word, meaning FROM system_dict ORDER BY word").fetchall()
        return {row["word"]: row["meaning"] for row in rows}

    def replace_system_dict(self, mapping):
        with self.transaction() as conn:
            conn.execute("DELETE FROM system_dict")
            conn.executemany(
                "INSERT INTO system_dict (word, meaning) VALUES (?, ?)",
                [(word, meaning) for word, meaning in mapping.items()],
            )

    def get_text_note(self):
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM text_notes WHERE id = 1").fetchone()
        return dict(row)

    def save_text_note(self, content, segments, highlights):
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE text_notes
                SET content = ?, segments = ?, highlights = ?, updated_at = ?
                WHERE id = 1
                """,
                (
                    content,
                    json.dumps(segments, ensure_ascii=False),
                    json.dumps(highlights, ensure_ascii=False),
                    now_ts(),
                ),
            )

    def list_vocab(self, order_by="v.created_at DESC", filters=None):
        filters = filters or {}
        clauses = []
        params = []
        if filters.get("tag"):
            clauses.append("v.tags LIKE ?")
            params.append(f"%{filters['tag']}%")
        if filters.get("pos"):
            clauses.append("v.pos = ?")
            params.append(filters["pos"])
        if filters.get("state") == "mastered":
            clauses.append("r.mastered = 1")
        elif filters.get("state") == "learning":
            clauses.append("IFNULL(r.mastered, 0) = 0")
        sql = """
            SELECT
                v.word, v.meaning, v.reading, v.base_form, v.pos, v.tags, v.example, v.priority,
                v.notes, v.created_at, v.updated_at, v.polite_form, v.te_form, v.ta_form,
                r.review_count, r.correct_count, r.wrong_count, r.stage_index, r.mastered,
                r.last_review_at, r.due_at, r.streak, r.wrong_streak
            FROM vocab v
            LEFT JOIN reviews r ON v.word = r.word
        """
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += f" ORDER BY {order_by}"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def get_vocab(self, word):
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT v.*, r.review_count, r.correct_count, r.wrong_count, r.stage_index,
                       r.mastered, r.last_review_at, r.due_at, r.streak, r.wrong_streak
                FROM vocab v
                LEFT JOIN reviews r ON v.word = r.word
                WHERE v.word = ?
                """,
                (word,),
            ).fetchone()
        return dict(row) if row else None

    def upsert_vocab(self, item):
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO vocab
                (word, meaning, reading, base_form, pos, tags, example, priority, notes, created_at, updated_at,
                 polite_form, te_form, ta_form)
                VALUES (:word, :meaning, :reading, :base_form, :pos, :tags, :example, :priority, :notes, :created_at, :updated_at,
                        :polite_form, :te_form, :ta_form)
                ON CONFLICT(word) DO UPDATE SET
                    meaning=excluded.meaning,
                    reading=excluded.reading,
                    base_form=excluded.base_form,
                    pos=excluded.pos,
                    tags=excluded.tags,
                    example=excluded.example,
                    priority=excluded.priority,
                    notes=excluded.notes,
                    updated_at=excluded.updated_at,
                    polite_form=excluded.polite_form,
                    te_form=excluded.te_form,
                    ta_form=excluded.ta_form
                """,
                item,
            )
            conn.execute(
                "INSERT OR IGNORE INTO reviews (word, due_at) VALUES (?, ?)",
                (item["word"], now_ts() + 12 * 60 * 60),
            )

    def delete_vocab_words(self, words):
        with self.transaction() as conn:
            for word in words:
                row = conn.execute(
                    """
                    SELECT v.*, r.review_count, r.correct_count, r.wrong_count, r.stage_index,
                           r.mastered, r.last_review_at, r.due_at, r.streak, r.wrong_streak
                    FROM vocab v
                    LEFT JOIN reviews r ON v.word = r.word
                    WHERE v.word = ?
                    """,
                    (word,),
                ).fetchone()
                if row:
                    conn.execute(
                        "INSERT INTO deleted_vocab (word, payload, deleted_at) VALUES (?, ?, ?)",
                        (word, json.dumps(dict(row), ensure_ascii=False), now_ts()),
                    )
            conn.executemany("DELETE FROM vocab WHERE word = ?", [(word,) for word in words])

    def batch_update_tags(self, words, tag_text):
        with self.transaction() as conn:
            conn.executemany(
                "UPDATE vocab SET tags = ?, updated_at = ? WHERE word = ?",
                [(tag_text, now_ts(), word) for word in words],
            )

    def update_priority(self, word, priority):
        with self.transaction() as conn:
            conn.execute("UPDATE vocab SET priority = ?, updated_at = ? WHERE word = ?", (priority, now_ts(), word))

    def get_today_review(self, stages, limit=100):
        now_value = now_ts()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    v.word, v.meaning, v.reading, v.pos, v.priority, v.tags,
                    r.last_review_at, r.due_at, r.review_count, r.correct_count,
                    r.wrong_count, r.stage_index, r.mastered, r.streak, r.wrong_streak
                FROM reviews r
                JOIN vocab v ON v.word = r.word
                WHERE IFNULL(r.mastered, 0) = 0 AND (r.due_at = 0 OR r.due_at <= ?)
                ORDER BY CASE WHEN r.review_count = 0 THEN 0 ELSE 1 END ASC,
                         v.priority DESC,
                         IFNULL(r.wrong_streak, 0) DESC,
                         r.stage_index ASC,
                         r.due_at ASC,
                         r.review_count ASC,
                         v.updated_at DESC
                LIMIT ?
                """,
                (now_value, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def apply_review_result(self, word, is_correct, stages):
        vocab = self.get_vocab(word)
        if not vocab:
            return
        review_count = vocab.get("review_count") or 0
        correct_count = vocab.get("correct_count") or 0
        wrong_count = vocab.get("wrong_count") or 0
        stage_index = vocab.get("stage_index") or 0
        streak = vocab.get("streak") or 0
        wrong_streak = vocab.get("wrong_streak") or 0
        priority = vocab.get("priority") or 1
        mastered = vocab.get("mastered") or 0
        current = now_ts()
        accuracy = correct_count / review_count if review_count else 0.0

        if is_correct:
            review_count += 1
            correct_count += 1
            streak += 1
            wrong_streak = 0
            if streak >= 1:
                stage_index = min(stage_index + 1, len(stages) - 1)
            base_interval = stages[min(stage_index, len(stages) - 1)]
            priority_factor = {1: 1.0, 2: 0.8, 3: 0.65}.get(priority, 1.0)
            accuracy = correct_count / max(1, review_count)
            confidence_factor = 1.0 + min(0.35, streak * 0.08)
            accuracy_factor = 0.92 if accuracy >= 0.8 else (0.8 if accuracy >= 0.92 else 1.0)
            due_at = current + base_interval * 60 * priority_factor * confidence_factor * accuracy_factor
            mastered = 1 if stage_index >= len(stages) - 1 else mastered
        else:
            review_count += 1
            wrong_count += 1
            wrong_streak += 1
            streak = 0
            if mastered and wrong_streak < 2:
                stage_index = max(len(stages) - 2, 0)
                base_interval = max(45, int(stages[min(stage_index, len(stages) - 1)] * 0.6))
            else:
                base_interval = max(15, int(stages[min(stage_index, len(stages) - 1)] * (0.55 if accuracy >= 0.75 else 0.4)))
            due_at = current + base_interval * 60
            if wrong_streak >= 3:
                mastered = 0

        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE reviews
                SET last_review_at = ?, due_at = ?, review_count = ?, correct_count = ?, wrong_count = ?,
                    stage_index = ?, streak = ?, wrong_streak = ?, mastered = ?
                WHERE word = ?
                """,
                (current, due_at, review_count, correct_count, wrong_count, stage_index, streak, wrong_streak, mastered, word),
            )

    def set_word_mastery(self, word, mastered):
        mastered_value = 1 if mastered else 0
        due_at = now_ts() + (90 * 24 * 60 * 60 if mastered_value else 10 * 60)
        stage_index = len(DEFAULT_REVIEW_STAGES) - 1 if mastered_value else 0
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO reviews (word, due_at) VALUES (?, ?)
                """,
                (word, due_at),
            )
            conn.execute(
                """
                UPDATE reviews
                SET mastered = ?, due_at = ?, stage_index = ?, wrong_streak = 0, streak = CASE WHEN ? = 1 THEN MAX(streak, 1) ELSE 0 END
                WHERE word = ?
                """,
                (mastered_value, due_at, stage_index, mastered_value, word),
            )

    def snooze_review_word(self, word, minutes):
        with self.transaction() as conn:
            stage_delta_sql = "stage_index = CASE WHEN ? >= 1440 THEN MAX(stage_index - 1, 0) ELSE stage_index END,"
            conn.execute(
                f"UPDATE reviews SET due_at = ?, mastered = 0, {stage_delta_sql} wrong_streak = 0 WHERE word = ?",
                (now_ts() + minutes * 60, minutes, word),
            )

    def list_mistakes(self):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT m.word, m.meaning, m.wrong_count, m.last_wrong_at,
                       v.pos, v.tags, v.priority
                FROM mistakes m
                LEFT JOIN vocab v ON m.word = v.word
                ORDER BY m.wrong_count DESC, m.last_wrong_at DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_mistake(self, word, meaning):
        current = now_ts()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO mistakes (word, meaning, wrong_count, last_wrong_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(word) DO UPDATE SET
                    meaning=excluded.meaning,
                    wrong_count=mistakes.wrong_count + 1,
                    last_wrong_at=excluded.last_wrong_at
                """,
                (word, meaning, current),
            )

    def resolve_mistake(self, word):
        with self.transaction() as conn:
            row = conn.execute("SELECT wrong_count FROM mistakes WHERE word = ?", (word,)).fetchone()
            if not row:
                return
            count = row["wrong_count"] - 1
            if count <= 0:
                conn.execute("DELETE FROM mistakes WHERE word = ?", (word,))
            else:
                conn.execute("UPDATE mistakes SET wrong_count = ? WHERE word = ?", (count, word))

    def clear_mistake(self, word):
        with self.transaction() as conn:
            conn.execute("DELETE FROM mistakes WHERE word = ?", (word,))

    def list_deleted_vocab(self, limit=50):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, word, payload, deleted_at FROM deleted_vocab ORDER BY deleted_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["payload"] = json.loads(item["payload"])
            except Exception:
                item["payload"] = {}
            result.append(item)
        return result

    def restore_deleted_words(self, ids):
        restored = 0
        with self.transaction() as conn:
            for item_id in ids:
                row = conn.execute("SELECT * FROM deleted_vocab WHERE id = ?", (item_id,)).fetchone()
                if not row:
                    continue
                try:
                    payload = json.loads(row["payload"])
                except Exception:
                    continue
                vocab_item = {
                    "word": payload.get("word", row["word"]),
                    "meaning": payload.get("meaning", ""),
                    "reading": payload.get("reading", ""),
                    "base_form": payload.get("base_form", payload.get("word", row["word"])),
                    "pos": payload.get("pos", ""),
                    "tags": payload.get("tags", ""),
                    "example": payload.get("example", ""),
                    "priority": payload.get("priority", 1),
                    "notes": payload.get("notes", ""),
                    "created_at": payload.get("created_at", now_ts()),
                    "updated_at": now_ts(),
                    "polite_form": payload.get("polite_form", ""),
                    "te_form": payload.get("te_form", ""),
                    "ta_form": payload.get("ta_form", ""),
                }
                conn.execute(
                    """
                    INSERT INTO vocab
                    (word, meaning, reading, base_form, pos, tags, example, priority, notes, created_at, updated_at,
                     polite_form, te_form, ta_form)
                    VALUES (:word, :meaning, :reading, :base_form, :pos, :tags, :example, :priority, :notes, :created_at, :updated_at,
                            :polite_form, :te_form, :ta_form)
                    ON CONFLICT(word) DO UPDATE SET
                        meaning=excluded.meaning,
                        reading=excluded.reading,
                        base_form=excluded.base_form,
                        pos=excluded.pos,
                        tags=excluded.tags,
                        example=excluded.example,
                        priority=excluded.priority,
                        notes=excluded.notes,
                        updated_at=excluded.updated_at,
                        polite_form=excluded.polite_form,
                        te_form=excluded.te_form,
                        ta_form=excluded.ta_form
                    """,
                    vocab_item,
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO reviews
                    (word, last_review_at, due_at, review_count, correct_count, wrong_count, stage_index, streak, wrong_streak, mastered)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        vocab_item["word"],
                        payload.get("last_review_at", 0),
                        payload.get("due_at", now_ts() + 12 * 60 * 60),
                        payload.get("review_count", 0),
                        payload.get("correct_count", 0),
                        payload.get("wrong_count", 0),
                        payload.get("stage_index", 0),
                        payload.get("streak", 0),
                        payload.get("wrong_streak", 0),
                        payload.get("mastered", 0),
                    ),
                )
                conn.execute("DELETE FROM deleted_vocab WHERE id = ?", (item_id,))
                restored += 1
        return restored

    def reset_learning_progress(self, words=None):
        with self.transaction() as conn:
            if words:
                conn.executemany(
                    """
                    UPDATE reviews
                    SET last_review_at = 0, due_at = ?, review_count = 0, correct_count = 0, wrong_count = 0,
                        stage_index = 0, streak = 0, wrong_streak = 0, mastered = 0
                    WHERE word = ?
                    """,
                    [(now_ts() + 12 * 60 * 60, word) for word in words],
                )
                conn.executemany("DELETE FROM mistakes WHERE word = ?", [(word,) for word in words])
            else:
                conn.execute(
                    """
                    UPDATE reviews
                    SET last_review_at = 0, due_at = ?, review_count = 0, correct_count = 0, wrong_count = 0,
                        stage_index = 0, streak = 0, wrong_streak = 0, mastered = 0
                    """,
                    (now_ts() + 12 * 60 * 60,),
                )
                conn.execute("DELETE FROM mistakes")

    def search_vocab(self, keyword):
        keyword = keyword.strip()
        if not keyword:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT v.*, r.review_count, r.correct_count, r.wrong_count, r.stage_index,
                       r.mastered, r.last_review_at, r.due_at, r.streak, r.wrong_streak
                FROM vocab v
                LEFT JOIN reviews r ON v.word = r.word
                WHERE v.word LIKE ? OR v.meaning LIKE ? OR v.reading LIKE ? OR v.tags LIKE ?
                ORDER BY v.updated_at DESC
                LIMIT 100
                """,
                tuple([f"%{keyword}%"] * 4),
            ).fetchall()
        return [dict(row) for row in rows]

    def add_test_record(self, total, correct, accuracy, mode):
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO test_records (created_at, total, correct, accuracy, mode)
                VALUES (?, ?, ?, ?, ?)
                """,
                (now_ts(), total, correct, accuracy, mode),
            )

    def list_test_records(self, limit=120):
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM test_records ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in reversed(rows)]

    def get_checkin(self):
        with self._connect() as conn:
            row = conn.execute("SELECT last_date, streak FROM checkin WHERE id = 1").fetchone()
        return dict(row)

    def save_checkin(self, last_date, streak):
        with self.transaction() as conn:
            conn.execute("UPDATE checkin SET last_date = ?, streak = ? WHERE id = 1", (last_date, streak))

    def backup_database(self, target_path=None):
        target = (
            Path(target_path)
            if target_path
            else BACKUP_DIR / f"kotoba_journal_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        )
        with self._connect() as source:
            target_conn = sqlite3.connect(str(target))
            source.backup(target_conn)
            target_conn.close()
        logging.info("数据库备份完成: %s", target)
        return str(target)

    def restore_database(self, backup_path):
        backup_conn = sqlite3.connect(str(backup_path))
        live_conn = sqlite3.connect(str(DB_PATH))
        try:
            backup_conn.backup(live_conn)
            live_conn.commit()
        finally:
            backup_conn.close()
            live_conn.close()
        logging.info("数据库恢复完成: %s", backup_path)


class StudyEngine:
    def __init__(self):
        try:
            self.tokenizer = Tokenizer()
            self.janome_available = True
        except Exception as exc:
            self.tokenizer = None
            self.janome_available = False
            logging.warning("Janome 初始化失败，已切换到基础分词模式: %s", exc)
        self.detail_cache = {}

    def analyze_text_tokens(self, text):
        if not text.strip():
            return []
        if not self.tokenizer:
            return self.fallback_split(text)
        tokens = []
        try:
            for token in self.tokenizer.tokenize(text):
                surface = token.surface.strip()
                if not surface or len(surface) <= 1:
                    continue
                pos = token.part_of_speech.split(",")[0]
                if pos in ("助词", "助動詞", "記号", "助詞", "助动词", "感动词", "感動詞"):
                    continue
                base = token.base_form.strip() if token.base_form and token.base_form != "*" else surface
                reading = token.reading if token.reading and token.reading != "*" else ""
                entry = {
                    "surface": surface,
                    "base": base,
                    "reading": self.katakana_to_hiragana(reading) if reading else "-",
                    "pos": pos,
                }
                tokens.append(entry)
        except Exception as exc:
            logging.exception("课文分词失败，已退回基础分词模式")
            return self.fallback_split(text, exc)
        return tokens

    def analyze_text_tokens_chunked(self, text, chunk_size=180):
        if not text.strip():
            return []
        chunks = []
        current = []
        current_len = 0
        for piece in re.split(r"(\n+)", text):
            if current_len + len(piece) > chunk_size and current:
                chunks.append("".join(current))
                current = [piece]
                current_len = len(piece)
            else:
                current.append(piece)
                current_len += len(piece)
        if current:
            chunks.append("".join(current))
        return chunks

    def fallback_split(self, text, exc=None):
        if exc:
            logging.warning("基础分词兜底启用: %s", exc)
        chunks = re.findall(r"[一-龥ぁ-んァ-ヶーA-Za-z0-9]+", text)
        return [
            {"surface": chunk, "base": chunk, "reading": "-", "pos": "未知"}
            for chunk in chunks
            if len(chunk.strip()) > 1
        ]

    def split_words(self, text):
        return [item["surface"] for item in self.analyze_text_tokens(text)]

    @staticmethod
    def katakana_to_hiragana(text):
        return "".join(chr(ord(c) - 0x60) if "ァ" <= c <= "ヶ" else c for c in text)

    @staticmethod
    def normalize_text(text):
        return (
            str(text).strip()
            .replace("；", ";")
            .replace("，", ",")
            .replace("、", ",")
            .replace("：", ":")
            .replace("（", "(")
            .replace("）", ")")
            .lower()
        )

    def split_meanings(self, text):
        base = self.normalize_text(text)
        return [chunk.strip() for chunk in re.split(r"[,/;|]", base) if chunk.strip()] or [base]

    def answer_matches(self, answer, meaning):
        ans = self.normalize_text(answer)
        targets = self.split_meanings(meaning)
        if ans in targets:
            return "exact"
        for target in targets:
            if ans and (ans in target or target in ans):
                return "close"
        return "wrong"

    def get_word_detail(self, word):
        if word in self.detail_cache:
            return self.detail_cache[word]
        if not self.tokenizer:
            detail = {"reading": "-", "base_form": word, "pos": "未知"}
            self.detail_cache[word] = detail
            return detail
        try:
            token = next(iter(self.tokenizer.tokenize(word)))
            reading = token.reading if token.reading and token.reading != "*" else "无"
            base_form = token.base_form if token.base_form and token.base_form != "*" else word
            pos = token.part_of_speech.split(",")[0] if token.part_of_speech else "无"
            detail = {
                "reading": self.katakana_to_hiragana(reading),
                "base_form": base_form,
                "pos": pos,
            }
        except Exception:
            detail = {"reading": "-", "base_form": word, "pos": "-"}
        self.detail_cache[word] = detail
        return detail

    def annotate_text(self, text, furigana=False):
        if not furigana:
            return text
        annotated = []
        try:
            for token in self.tokenizer.tokenize(text):
                surface = token.surface
                reading = token.reading if token.reading and token.reading != "*" else ""
                if any("\u4e00" <= c <= "\u9fff" for c in surface) and reading:
                    annotated.append(f"{surface}({self.katakana_to_hiragana(reading)})")
                else:
                    annotated.append(surface)
            return "".join(annotated)
        except Exception:
            return text

    def build_choices(self, correct_meaning, all_meanings):
        pool = [meaning for meaning in {item for item in all_meanings if item and item != correct_meaning}]
        random.shuffle(pool)
        choices = [correct_meaning] + pool[:3]
        random.shuffle(choices)
        return choices

    def infer_verb_forms(self, word, pos):
        if "动词" not in pos and "動詞" not in pos:
            return {"polite": "", "te": "", "ta": ""}
        if word.endswith("いく") or word.endswith("行く"):
            stem = word[:-1]
            return {"polite": stem + "きます", "te": stem + "って", "ta": stem + "った"}
        if word.endswith("する"):
            return {"polite": word[:-2] + "します", "te": word[:-2] + "して", "ta": word[:-2] + "した"}
        if word.endswith("くる") or word.endswith("来る"):
            stem = word[:-2]
            return {"polite": stem + "きます", "te": stem + "きて", "ta": stem + "きた"}
        if word.endswith("る") and len(word) >= 2 and word[-2] in "いきしちにひみりえけせてねへめれ":
            stem = word[:-1]
            return {"polite": stem + "ます", "te": stem + "て", "ta": stem + "た"}
        if word.endswith("う") or word.endswith("つ") or word.endswith("る"):
            stem = word[:-1]
            return {"polite": stem + "います", "te": stem + "って", "ta": stem + "った"}
        if word.endswith("む") or word.endswith("ぶ") or word.endswith("ぬ"):
            stem = word[:-1]
            return {"polite": stem + "みます", "te": stem + "んで", "ta": stem + "んだ"}
        if word.endswith("く"):
            stem = word[:-1]
            return {"polite": stem + "きます", "te": stem + "いて", "ta": stem + "いた"}
        if word.endswith("ぐ"):
            stem = word[:-1]
            return {"polite": stem + "ぎます", "te": stem + "いで", "ta": stem + "いだ"}
        if word.endswith("す"):
            stem = word[:-1]
            return {"polite": stem + "します", "te": stem + "して", "ta": stem + "した"}
        return {"polite": "", "te": "", "ta": ""}

    def weak_point_analysis(self, vocab_rows, mistake_rows):
        pos_counter = Counter()
        tag_counter = Counter()
        for row in mistake_rows:
            pos = row.get("pos") or "未分类"
            pos_counter[pos] += row.get("wrong_count", 0)
            for tag in [part.strip() for part in (row.get("tags") or "").split(",") if part.strip()]:
                tag_counter[tag] += row.get("wrong_count", 0)
        messages = []
        if pos_counter:
            pos, count = pos_counter.most_common(1)[0]
            messages.append(f"你最近最容易失误的词性是“{pos}”，累计错误 {count} 次。")
        if tag_counter:
            tag, count = tag_counter.most_common(1)[0]
            messages.append(f"标签“{tag}”相关词汇波动较大，累计错误 {count} 次。")
        if not messages and vocab_rows:
            messages.append("目前没有明显的薄弱点，整体状态比较均衡。")
        if not messages:
            messages.append("先收录一些词，系统就能逐步分析你的薄弱环节。")
        return messages


class SplashScreen(tk.Toplevel):
    def __init__(self, master, theme):
        super().__init__(master)
        self.overrideredirect(True)
        self.configure(bg=theme["bg"])
        width = 620
        height = 360
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        self.geometry(f"{width}x{height}+{x}+{y}")
        self.attributes("-topmost", True)
        self.progress = 0.0

        shell = tk.Frame(self, bg=theme["card"], highlightthickness=1, highlightbackground=theme["line"])
        shell.place(relx=0.5, rely=0.5, anchor="center", width=540, height=280)
        logo = tk.Canvas(shell, width=76, height=76, bg=theme["card"], highlightthickness=0)
        logo.pack(pady=(42, 12))
        logo.create_oval(6, 6, 70, 70, fill=theme["wood"], outline="")
        logo.create_text(38, 38, text="言", fill="white", font=("Meiryo", 28, "bold"))
        tk.Label(shell, text=APP_TITLE, bg=theme["card"], fg=theme["text"], font=("Meiryo", 22, "bold")).pack()
        tk.Label(shell, text="静かに、深く、続けられる学習帳", bg=theme["card"], fg=theme["muted"], font=("SimHei", 10)).pack(pady=(8, 18))
        self.loading = tk.Label(shell, text="正在整理你的学习手帐…", bg=theme["card"], fg=theme["muted"], font=("SimHei", 10))
        self.loading.pack()
        self.bar = tk.Canvas(shell, width=220, height=8, bg=theme["card"], highlightthickness=0)
        self.bar.pack(pady=(20, 0))
        self.bar.create_rectangle(0, 0, 220, 8, fill=theme["accent_soft"], outline="")
        self.bar_fg = self.bar.create_rectangle(0, 0, 0, 8, fill=theme["accent"], outline="")

    def tick(self, callback):
        self.progress += 0.1
        if self.progress > 1.0:
            self.progress = 1.0
        self.bar.coords(self.bar_fg, 0, 0, 220 * self.progress, 8)
        if self.progress >= 1.0:
            self.after(150, callback)
        else:
            self.after(80, lambda: self.tick(callback))


class PromptDialog(tk.Toplevel):
    def __init__(self, master, theme, title, prompt, initial=""):
        super().__init__(master)
        self.result = None
        self.theme = theme
        self.title(title)
        self.configure(bg=theme["panel"])
        self.geometry("380x190")
        self.transient(master)
        self.grab_set()
        self.bind("<Escape>", lambda e: self.destroy())
        self.bind("<Return>", lambda e: self.submit())

        box = tk.Frame(self, bg=theme["panel"], padx=20, pady=20)
        box.pack(fill="both", expand=True)
        tk.Label(box, text=prompt, bg=theme["panel"], fg=theme["text"], wraplength=320, justify="left").pack(anchor="w")
        self.var = tk.StringVar(value=initial)
        entry = tk.Entry(box, textvariable=self.var, bg=theme["card"], fg=theme["text"], relief="flat")
        entry.pack(fill="x", pady=(12, 18))
        entry.focus_set()
        entry.select_range(0, "end")
        foot = tk.Frame(box, bg=theme["panel"])
        foot.pack(fill="x")
        tk.Button(foot, text="取消", command=self.destroy, bg=theme["wood_soft"], fg=theme["wood"], relief="flat", padx=14, pady=8).pack(side="right")
        tk.Button(foot, text="确定", command=self.submit, bg=theme["accent"], fg="white", relief="flat", padx=14, pady=8).pack(side="right", padx=(0, 8))

    def submit(self):
        self.result = self.var.get().strip()
        self.destroy()


class WordEditor(tk.Toplevel):
    def __init__(self, master, theme, title, payload=None, mode="simple"):
        super().__init__(master)
        self.result = None
        self.theme = theme
        self.payload = payload or {}
        self.mode = mode
        self.title(title)
        self.configure(bg=theme["panel"])
        self.geometry("620x540" if mode == "simple" else "760x760")
        self.minsize(560, 500 if mode == "simple" else 700)
        self.transient(master)
        self.grab_set()
        self.bind("<Escape>", lambda e: self.destroy())
        self.bind("<Control-Return>", lambda e: self.submit())

        wrap = tk.Frame(self, bg=theme["panel"], padx=24, pady=22)
        wrap.pack(fill="both", expand=True)
        tk.Label(wrap, text=title, bg=theme["panel"], fg=theme["text"], font=("SimHei UI", 15, "bold")).pack(anchor="w")
        hint_text = "快速模式：先填写单词和释义，其他信息可后面再补。" if mode == "simple" else "完整模式：可继续补充标签、例句和词条信息。"
        tk.Label(wrap, text=hint_text, bg=theme["panel"], fg=theme["muted"], font=("SimHei UI", 9)).pack(anchor="w", pady=(4, 16))

        self.entries = {}
        fields = [("word", "单词"), ("meaning", "释义"), ("tags", "标签"), ("example", "例句")]
        if mode == "full":
            fields.append(("notes", "备注"))
        for key, label in fields:
            tk.Label(wrap, text=label, bg=theme["panel"], fg=theme["muted"], font=("SimHei UI", 10)).pack(anchor="w", pady=(8, 6))
            if key in ("example", "notes"):
                widget = tk.Text(
                    wrap,
                    height=5 if key == "example" else 4,
                    wrap="word",
                    bg=theme["card"],
                    fg=theme["text"],
                    relief="flat",
                    highlightthickness=1,
                    highlightbackground=theme["line"],
                    highlightcolor=theme["accent"],
                    font=("SimHei UI", 10),
                    padx=10,
                    pady=8,
                )
                widget.pack(fill="x", pady=(0, 4))
                widget.insert("1.0", self.payload.get(key, ""))
            else:
                widget = tk.Entry(
                    wrap,
                    bg=theme["card"],
                    fg=theme["text"],
                    relief="flat",
                    highlightthickness=1,
                    highlightbackground=theme["line"],
                    highlightcolor=theme["accent"],
                    font=("SimHei UI", 11),
                    insertbackground=theme["text"],
                )
                widget.pack(fill="x", pady=(0, 4))
                widget.insert(0, self.payload.get(key, ""))
                widget.bind("<Return>", self.handle_enter)
            self.entries[key] = widget

        self.meta_vars = {
            "reading": tk.StringVar(value=self.payload.get("reading", "")),
            "base_form": tk.StringVar(value=self.payload.get("base_form", "")),
            "pos": tk.StringVar(value=self.payload.get("pos", "")),
            "polite_form": tk.StringVar(value=self.payload.get("polite_form", "")),
            "te_form": tk.StringVar(value=self.payload.get("te_form", "")),
            "ta_form": tk.StringVar(value=self.payload.get("ta_form", "")),
        }
        if mode == "full":
            meta_row = tk.Frame(wrap, bg=theme["panel"])
            meta_row.pack(fill="x", pady=(14, 0))
            meta_specs = [("reading", "读音"), ("base_form", "原形"), ("pos", "词性")]
            for idx, (key, label) in enumerate(meta_specs):
                cell = tk.Frame(meta_row, bg=theme["panel"])
                cell.grid(row=0, column=idx, sticky="ew", padx=(0 if idx == 0 else 8, 0))
                meta_row.grid_columnconfigure(idx, weight=1)
                tk.Label(cell, text=label, bg=theme["panel"], fg=theme["muted"], font=("SimHei UI", 10)).pack(anchor="w", pady=(0, 6))
                tk.Entry(
                    cell,
                    textvariable=self.meta_vars[key],
                    bg=theme["card"],
                    fg=theme["text"],
                    relief="flat",
                    highlightthickness=1,
                    highlightbackground=theme["line"],
                    highlightcolor=theme["accent"],
                ).pack(fill="x")
            form_row = tk.Frame(wrap, bg=theme["panel"])
            form_row.pack(fill="x", pady=(14, 0))
            form_specs = [("polite_form", "ます形"), ("te_form", "て形"), ("ta_form", "た形")]
            for idx, (key, label) in enumerate(form_specs):
                cell = tk.Frame(form_row, bg=theme["panel"])
                cell.grid(row=0, column=idx, sticky="ew", padx=(0 if idx == 0 else 8, 0))
                form_row.grid_columnconfigure(idx, weight=1)
                tk.Label(cell, text=label, bg=theme["panel"], fg=theme["muted"], font=("SimHei UI", 10)).pack(anchor="w", pady=(0, 6))
                tk.Entry(
                    cell,
                    textvariable=self.meta_vars[key],
                    bg=theme["card"],
                    fg=theme["text"],
                    relief="flat",
                    highlightthickness=1,
                    highlightbackground=theme["line"],
                    highlightcolor=theme["accent"],
                ).pack(fill="x")

        tk.Label(wrap, text="复习优先级", bg=theme["panel"], fg=theme["muted"], font=("SimHei UI", 10)).pack(anchor="w", pady=(10, 6))
        self.priority_var = tk.IntVar(value=int(self.payload.get("priority", 1) or 1))
        selector = tk.Frame(wrap, bg=theme["panel"])
        selector.pack(fill="x")
        for idx, text in [(1, "普通"), (2, "重点"), (3, "高频")]:
            tk.Radiobutton(
                selector,
                text=text,
                variable=self.priority_var,
                value=idx,
                bg=theme["panel"],
                fg=theme["text"],
                selectcolor=theme["card"],
                activebackground=theme["panel"],
                activeforeground=theme["text"],
                font=("SimHei UI", 10),
            ).pack(side="left", padx=(0, 18))

        switcher = tk.Frame(wrap, bg=theme["panel"])
        switcher.pack(fill="x", pady=(18, 0))
        if mode == "simple":
            tk.Button(switcher, text="打开完整编辑", command=self.open_full_mode, bg=theme["accent_soft"], fg=theme["text"], relief="flat", padx=12, pady=8).pack(side="left")

        foot = tk.Frame(wrap, bg=theme["panel"])
        foot.pack(fill="x", pady=(24, 0))
        tk.Button(foot, text="取消", command=self.destroy, bg=theme["wood_soft"], fg=theme["wood"], relief="flat", padx=16, pady=8, cursor="hand2").pack(side="right", padx=(8, 0))
        tk.Button(foot, text="保存", command=self.submit, bg=theme["accent"], fg="white", relief="flat", padx=16, pady=8, cursor="hand2", font=("SimHei UI", 10, "bold")).pack(side="right")

    def open_full_mode(self):
        current = {
            "word": self.entries["word"].get().strip() if "word" in self.entries else "",
            "meaning": self.entries["meaning"].get().strip() if "meaning" in self.entries else "",
            "tags": self.entries["tags"].get().strip() if "tags" in self.entries else "",
            "example": self.entries["example"].get("1.0", "end").strip() if isinstance(self.entries.get("example"), tk.Text) else "",
            "notes": self.payload.get("notes", ""),
            "priority": self.priority_var.get(),
            "reading": self.meta_vars["reading"].get().strip(),
            "base_form": self.meta_vars["base_form"].get().strip(),
            "pos": self.meta_vars["pos"].get().strip(),
            "polite_form": self.meta_vars["polite_form"].get().strip(),
            "te_form": self.meta_vars["te_form"].get().strip(),
            "ta_form": self.meta_vars["ta_form"].get().strip(),
            "created_at": self.payload.get("created_at"),
        }
        self.destroy()
        self.master.open_word_editor(current, mode="full")

    def handle_enter(self, event):
        if isinstance(event.widget, tk.Text):
            return
        self.submit()

    def submit(self):
        def get_text(name):
            widget = self.entries[name]
            if isinstance(widget, tk.Text):
                return widget.get("1.0", "end").strip()
            return widget.get().strip()

        data = {
            "word": get_text("word"),
            "meaning": get_text("meaning"),
            "tags": get_text("tags"),
            "example": get_text("example"),
            "notes": get_text("notes") if "notes" in self.entries else self.payload.get("notes", ""),
            "priority": self.priority_var.get(),
            "reading": self.meta_vars["reading"].get().strip(),
            "base_form": self.meta_vars["base_form"].get().strip(),
            "pos": self.meta_vars["pos"].get().strip(),
            "polite_form": self.meta_vars["polite_form"].get().strip(),
            "te_form": self.meta_vars["te_form"].get().strip(),
            "ta_form": self.meta_vars["ta_form"].get().strip(),
        }
        if not data["word"] or not data["meaning"]:
            safe_message("warning", "提示", "单词和释义不能为空。")
            return
        self.result = data
        self.destroy()


class JournalApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.withdraw()
        self.db = DBHelper(DB_PATH)
        self.settings = self.db.get_all_settings()
        self.theme_key = self.settings.get("theme", "mist_blue")
        self.theme = THEMES.get(self.theme_key, THEMES["mist_blue"])
        self.cn_font_size = int(self.settings.get("font_size_cn", "11"))
        self.jp_font_size = int(self.settings.get("font_size_jp", "13"))
        self.review_stages = self.parse_stages(self.settings.get("review_stages", ",".join(map(str, DEFAULT_REVIEW_STAGES))))
        self.fade_enabled = self.settings.get("fade_enabled", "1") == "1"
        self.fade_var = tk.BooleanVar(value=self.fade_enabled)
        self.furigana_default = self.settings.get("furigana_default", "0") == "1"
        self.auto_save_textlab = self.settings.get("auto_save_textlab", "1") == "1"
        self.engine = StudyEngine()
        self.current_note = self.db.get_text_note()
        self.current_words = []
        self.current_word_entries = []
        self.last_auto_analyzed_content = ""
        self.async_queue = queue.Queue()
        self.spinner_job = None
        self.analysis_generation = 0
        self.analysis_in_progress = False
        self.page_widgets = []
        self.tree_cache = {}
        self.selected_nav = tk.StringVar(value="dashboard")
        self.review_session = []
        self.review_index = 0
        self.review_correct = 0
        self.review_total = 0
        self.test_session = []
        self.test_index = 0
        self.test_correct = 0
        self.test_total = 0
        self.practice_session = []
        self.practice_index = 0
        self.practice_correct = 0
        self.practice_total = 0
        self.pending_next_callback = None
        self.answer_lock = False
        self.pending_answer = None
        self.autosave_job = None
        self.review_memory_choice = tk.StringVar(value="记得")
        self.hide_mastered_var = tk.BooleanVar(value=True)
        self.show_advanced_filters_var = tk.BooleanVar(value=False)
        self.sidebar_collapsed = self.settings.get("sidebar_collapsed", "0") == "1"
        self.furigana_var = tk.BooleanVar(value=self.furigana_default)
        self.review_direction_var = tk.StringVar(value="日语 -> 中文")
        self.test_direction_var = tk.StringVar(value="日语 -> 中文")
        self.practice_direction_var = tk.StringVar(value="日语 -> 中文")
        self.text_filter_pos = tk.StringVar(value="全部词性")
        self.text_filter_state = tk.StringVar(value="全部状态")
        self.vocab_search_var = tk.StringVar(value="")
        self.vocab_filter_tag = tk.StringVar(value="")
        self.vocab_filter_pos = tk.StringVar(value="全部词性")
        self.vocab_filter_state = tk.StringVar(value="全部状态")
        self.vocab_scope_var = tk.StringVar(value="全部范围")
        self.vocab_sort_key = tk.StringVar(value="加入时间")
        self.status_var = tk.StringVar(value="欢迎回来，今天也继续慢慢积累。")
        self.status_color = tk.StringVar(value=self.theme["muted"])
        self.analysis_job = None
        self.quiet_mode_var = tk.BooleanVar(value=False)
        self.active_option_buttons = []
        self.build_root()
        self.attributes("-alpha", 0.0)
        self.after(50, self.show_splash)

    def parse_stages(self, raw):
        try:
            values = [int(part.strip()) for part in str(raw).split(",") if part.strip()]
            return values or DEFAULT_REVIEW_STAGES[:]
        except Exception:
            return DEFAULT_REVIEW_STAGES[:]

    def build_root(self):
        self.title(f"{APP_TITLE} | {APP_SUBTITLE}")
        self.refresh_window_metrics()
        self.configure(bg=self.theme["bg"])
        self.option_add("*Font", ("SimHei UI", self.cn_font_size))
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.bind_shortcuts()
        self.bind("<FocusOut>", self.handle_app_focus_out)
        self.bind("<Unmap>", self.handle_app_unmap)

    def show_splash(self):
        splash = SplashScreen(self, self.theme)

        def finish():
            splash.destroy()
            self.deiconify()
            self.attributes("-alpha", 1.0)
            self.setup_styles()
            self.do_checkin()
            self.ensure_daily_backup()
            self.build_shell()
            self.show_dashboard()
            if not self.engine.janome_available:
                self.set_status("Janome 未安装，当前使用基础分词模式；程序可正常使用，但词性和原形精度会下降。", "warning")
            self.poll_async_queue()

        splash.tick(finish)

    def bind_shortcuts(self):
        self.bind("<Control-n>", lambda e: self.open_word_editor())
        self.bind("<Control-Shift-N>", lambda e: self.open_word_editor(mode="full"))
        self.bind("<Control-e>", lambda e: self.export_vocab())
        self.bind("<Control-i>", lambda e: self.import_excel())
        self.bind("<Escape>", lambda e: self.close_top_dialog())
        self.bind_all("<Return>", self.handle_global_enter)
        self.bind_all("<KP_Enter>", self.handle_global_enter)
        self.bind_all("<space>", self.handle_global_enter)
        for key in ["1", "2", "3", "4", "a", "b", "c", "d", "A", "B", "C", "D"]:
            self.bind_all(key, self.handle_option_hotkey)

    def refresh_window_metrics(self):
        scale = max(0, self.cn_font_size - 11)
        screen_width = max(1280, self.winfo_screenwidth())
        screen_height = max(800, self.winfo_screenheight())
        max_width = max(1180, screen_width - 90)
        max_height = max(760, screen_height - 120)
        width = min(1540 + scale * 70, max_width)
        height = min(940 + scale * 35, max_height)
        min_width = min(1260 + scale * 45, max_width)
        min_height = min(820 + scale * 24, max_height)
        self.geometry(f"{width}x{height}")
        self.minsize(min_width, min_height)

    def is_text_input_focus(self, widget):
        return isinstance(widget, (tk.Entry, tk.Text, ttk.Combobox, tk.Spinbox))

    def handle_global_enter(self, event):
        widget = self.focus_get()
        if widget and self.is_text_input_focus(widget):
            return None
        if self.pending_next_callback and not self.answer_lock:
            self.pending_next_callback()
            return "break"
        if event.keysym == "space" and self.reveal_current_answer():
            return "break"

    def reveal_current_answer(self):
        if self.pending_next_callback or self.pending_answer:
            return False
        if self.selected_nav.get() == "review" and getattr(self, "review_session", None) and self.review_index < len(self.review_session):
            row = self.review_session[self.review_index]
            self.review_feedback.config(text=f"辅助查看：正确答案是 {row['meaning']}\n这一步不会记分，也不会影响复习结果。", fg=self.theme["muted"])
            return True
        if self.selected_nav.get() == "test" and getattr(self, "test_session", None) and self.test_index < len(self.test_session):
            row = self.test_session[self.test_index]
            self.test_feedback.config(text=f"辅助查看：正确答案是 {row['meaning']}\n这一步不会记分，也不会影响测试结果。", fg=self.theme["muted"])
            return True
        if self.selected_nav.get() == "mistakes" and getattr(self, "practice_session", None) and self.practice_index < len(self.practice_session):
            row = self.practice_session[self.practice_index]
            vocab = self.db.get_vocab(row["word"])
            meaning = vocab["meaning"] if vocab else row["meaning"]
            self.practice_feedback.config(text=f"辅助查看：正确答案是 {meaning}\n这一步不会记分，也不会影响错题结果。", fg=self.theme["muted"])
            return True
        return False

    def handle_app_focus_out(self, _event):
        if self.selected_nav.get() == "textlab":
            self.flush_textlab_autosave()

    def handle_app_unmap(self, _event):
        if self.selected_nav.get() == "textlab":
            self.flush_textlab_autosave()

    def handle_option_hotkey(self, event):
        widget = self.focus_get()
        if widget and self.is_text_input_focus(widget):
            return
        if self.answer_lock or not self.active_option_buttons:
            return
        mapping = {"1": 0, "a": 0, "A": 0, "2": 1, "b": 1, "B": 1, "3": 2, "c": 2, "C": 2, "4": 3, "d": 3, "D": 3}
        index = mapping.get(event.keysym)
        if index is None or index >= len(self.active_option_buttons):
            return
        button = self.active_option_buttons[index]
        if button.winfo_exists():
            button.invoke()

    def close_top_dialog(self):
        top = self.focus_get()
        if top:
            widget = top.winfo_toplevel()
            if widget is not self and isinstance(widget, tk.Toplevel):
                widget.destroy()

    def setup_styles(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure(
            "Journal.Treeview",
            background=self.theme["card"],
            fieldbackground=self.theme["card"],
            foreground=self.theme["text"],
            rowheight=34,
            borderwidth=0,
            font=("SimHei UI", self.cn_font_size),
        )
        style.configure(
            "Journal.Treeview.Heading",
            background=self.theme["accent_soft"],
            foreground=self.theme["text"],
            relief="flat",
            borderwidth=0,
            font=("SimHei UI", self.cn_font_size, "bold"),
        )
        style.map("Journal.Treeview", background=[("selected", self.theme["accent_soft"])], foreground=[("selected", self.theme["text"])])
        style.configure(
            "Vertical.TScrollbar",
            background=self.theme["accent_soft"],
            troughcolor=self.theme["panel"],
            bordercolor=self.theme["panel"],
            arrowcolor=self.theme["text"],
            lightcolor=self.theme["accent_soft"],
            darkcolor=self.theme["accent_soft"],
        )
        style.configure(
            "Horizontal.TScrollbar",
            background=self.theme["accent_soft"],
            troughcolor=self.theme["panel"],
            bordercolor=self.theme["panel"],
            arrowcolor=self.theme["text"],
            lightcolor=self.theme["accent_soft"],
            darkcolor=self.theme["accent_soft"],
        )

    def do_checkin(self):
        info = self.db.get_checkin()
        today = today_str()
        last = info["last_date"]
        streak = info["streak"]
        if last == today:
            self.streak = streak
        else:
            if last:
                diff_days = (date.today() - date.fromisoformat(last)).days
            else:
                diff_days = 999
            if diff_days == 1:
                self.streak = streak + 1
            elif diff_days == 2:
                self.streak = max(1, streak)
            else:
                self.streak = 1
            self.db.save_checkin(today, self.streak)
        logging.info("打卡完成，连续天数: %s", self.streak)

    def build_shell(self):
        self.sidebar_width = 280 + max(0, self.cn_font_size - 11) * 10
        self.sidebar = tk.Frame(self, bg=self.theme["sidebar"], width=self.sidebar_width)
        self.sidebar.grid(row=0, column=0, sticky="nsw")
        self.sidebar.grid_propagate(False)
        self.content = tk.Frame(self, bg=self.theme["bg"])
        self.content.grid(row=0, column=1, sticky="nsew")
        self.content.grid_columnconfigure(0, weight=1)
        self.content.grid_rowconfigure(1, weight=1)
        self.build_sidebar()
        self.build_header()
        self.page_host = tk.Frame(self.content, bg=self.theme["bg"])
        self.page_host.grid(row=1, column=0, sticky="nsew", padx=26, pady=(0, 24))
        self.page_host.grid_columnconfigure(0, weight=1)
        self.page_host.grid_rowconfigure(0, weight=1)
        footer = tk.Frame(self.content, bg=self.theme["bg"])
        footer.grid(row=2, column=0, sticky="ew", padx=26, pady=(0, 18))
        self.status_label = tk.Label(footer, textvariable=self.status_var, bg=self.theme["bg"], fg=self.theme["muted"], font=("SimHei UI", 9))
        self.status_label.pack(anchor="w")
        self.apply_sidebar_visibility()

    def set_status(self, message, level="info"):
        colors = {
            "info": self.theme["muted"],
            "success": self.theme["good"],
            "warning": self.theme["warn"],
            "error": self.theme["danger"],
        }
        self.status_var.set(message)
        color = colors.get(level, self.theme["muted"])
        self.status_color.set(color)
        if hasattr(self, "status_label"):
            self.status_label.configure(fg=color)

    def toast(self, title, message, level="info"):
        self.set_status(message, "success" if level == "success" else level)
        if not self.quiet_mode_var.get() and level in {"success", "warning", "error"}:
            safe_message("info" if level == "success" else level, title, message)

    def ensure_daily_backup(self):
        stamp = self.db.get_setting("last_auto_backup_date", "")
        today = today_str()
        if stamp == today:
            return
        try:
            self.db.backup_database(BACKUP_DIR / f"auto_backup_{today}.db")
            self.db.set_setting("last_auto_backup_date", today)
            logging.info("自动备份完成: %s", today)
        except Exception:
            logging.exception("自动备份失败")

    def build_sidebar(self):
        logo_card = tk.Frame(self.sidebar, bg=self.theme["sidebar"])
        logo_card.pack(fill="x", padx=24, pady=(28, 20))
        logo_canvas = tk.Canvas(logo_card, width=54, height=54, bg=self.theme["sidebar"], highlightthickness=0)
        logo_canvas.pack(side="left")
        logo_canvas.create_oval(4, 4, 54, 54, fill=self.theme["wood"], outline="")
        logo_canvas.create_text(29, 29, text="言", fill="white", font=("Meiryo", 18, "bold"))
        logo_text = tk.Frame(logo_card, bg=self.theme["sidebar"])
        logo_text.pack(side="left", padx=14)
        tk.Label(logo_text, text=APP_TITLE, bg=self.theme["sidebar"], fg=self.theme["text"], font=("Meiryo", 20, "bold")).pack(anchor="w")
        tk.Label(logo_text, text="calm vocabulary notebook", bg=self.theme["sidebar"], fg=self.theme["muted"], font=("SimHei UI", 8, "italic")).pack(anchor="w")
        tk.Label(logo_text, text=APP_SUBTITLE, bg=self.theme["sidebar"], fg=self.theme["wood"], font=("SimHei UI", 9)).pack(anchor="w")
        navs = [
            ("dashboard", "今日页"),
            ("textlab", "课文手札"),
            ("vocab", "词汇本"),
            ("review", "复习计划"),
            ("test", "随机自测"),
            ("mistakes", "错题本"),
            ("report", "学习报告"),
            ("settings", "设置中心"),
        ]
        self.nav_buttons = {}
        for key, label in navs:
            btn = tk.Button(
                self.sidebar,
                text=label,
                command=lambda name=key: self.transition_to(name),
                bg=self.theme["sidebar"],
                fg=self.theme["text"],
                activebackground=self.theme["card"],
                activeforeground=self.theme["text"],
                relief="flat",
                anchor="w",
                padx=28,
                pady=12,
                cursor="hand2",
                font=("SimHei UI", 11),
            )
            self.decorate_button(btn)
            btn.pack(fill="x", padx=16, pady=4)
            self.nav_buttons[key] = btn

        task_card = tk.Frame(self.sidebar, bg=self.theme["wood_soft"], highlightthickness=7, highlightbackground=self.theme["line"])
        task_card.pack(side="bottom", fill="x", padx=18, pady=22)
        tk.Label(task_card, text="今日节奏", bg=self.theme["wood_soft"], fg=self.theme["wood"], font=("SimHei UI", 10, "bold")).pack(anchor="w", padx=16, pady=(14, 4))
        task = self.get_today_task()
        tk.Label(task_card, text=f"复习 {task['review']}  新学 {task['new']}  错题 {task['mistake']}", bg=self.theme["wood_soft"], fg=self.theme["muted"], font=("SimHei UI", 9)).pack(anchor="w", padx=16, pady=(0, 12))
        self.refresh_nav_style()

    def build_header(self):
        self.header = tk.Frame(self.content, bg=self.theme["bg"])
        self.header.grid(row=0, column=0, sticky="ew", padx=26, pady=(24, 18))
        self.header.grid_columnconfigure(1, weight=1)
        left_wrap = tk.Frame(self.header, bg=self.theme["bg"])
        left_wrap.grid(row=0, column=0, rowspan=2, sticky="w")
        self.sidebar_toggle_button = self.make_action_button(left_wrap, "收起导航", self.toggle_sidebar, "soft")
        self.sidebar_toggle_button.pack(side="left", padx=(0, 12))
        title_wrap = tk.Frame(left_wrap, bg=self.theme["bg"])
        title_wrap.pack(side="left")
        self.page_title = tk.Label(title_wrap, text="", bg=self.theme["bg"], fg=self.theme["text"], font=("SimHei UI", 23, "bold"))
        self.page_title.pack(anchor="w")
        self.page_subtitle = tk.Label(title_wrap, text="", bg=self.theme["bg"], fg=self.theme["muted"], font=("SimHei UI", 10))
        self.page_subtitle.pack(anchor="w", pady=(6, 0))
        self.header_info = tk.Label(self.header, text="", bg=self.theme["bg"], fg=self.theme["muted"], font=("SimHei UI", 10))
        self.header_info.grid(row=0, column=1, rowspan=2, sticky="e")

    def set_header(self, title, subtitle):
        self.page_title.config(text=title)
        self.page_subtitle.config(text=subtitle)
        self.header_info.config(text=f"{datetime.now().strftime('%Y-%m-%d')}  连续学习 {self.streak} 天")

    def refresh_nav_style(self):
        for key, btn in self.nav_buttons.items():
            active = key == self.selected_nav.get()
            btn.configure(
                bg=self.theme["card"] if active else self.theme["sidebar"],
                fg=self.theme["text"],
                relief="flat",
                bd=0,
                highlightthickness=0,
                activebackground=self.theme["card"] if active else self.theme["sidebar"],
                font=("SimHei UI", 11, "bold" if active else "normal"),
            )

    def toggle_sidebar(self):
        self.sidebar_collapsed = not self.sidebar_collapsed
        self.db.set_setting("sidebar_collapsed", "1" if self.sidebar_collapsed else "0")
        self.apply_sidebar_visibility()

    def apply_sidebar_visibility(self):
        if not hasattr(self, "content"):
            return
        if self.sidebar_collapsed:
            self.grid_columnconfigure(0, weight=1, minsize=0)
            self.grid_columnconfigure(1, weight=0, minsize=0)
            if hasattr(self, "sidebar"):
                self.sidebar.grid_remove()
            self.content.grid_configure(column=0, columnspan=2, sticky="nsew")
            if hasattr(self, "sidebar_toggle_button"):
                self.sidebar_toggle_button.configure(text="展开导航")
            self.set_status("左侧导航已收起，页面已切换到宽屏模式。", "info")
        else:
            self.grid_columnconfigure(0, weight=0, minsize=self.sidebar_width)
            self.grid_columnconfigure(1, weight=1, minsize=0)
            if hasattr(self, "sidebar"):
                self.sidebar.configure(width=self.sidebar_width)
                self.sidebar.grid()
            self.content.grid_configure(column=1, columnspan=1, sticky="nsew")
            if hasattr(self, "sidebar_toggle_button"):
                self.sidebar_toggle_button.configure(text="收起导航")

    def decorate_button(self, button):
        def on_press(_):
            button.configure(relief="flat", pady=11)

        def on_release(_):
            button.configure(relief="flat", pady=12, bd=0, highlightthickness=0)

        button.bind("<ButtonPress-1>", on_press)
        button.bind("<ButtonRelease-1>", on_release)

    def make_action_button(self, parent, text, command, kind="accent", width=None):
        palette = {
            "accent": (self.theme["accent"], "white"),
            "soft": (self.theme["accent_soft"], self.theme["text"]),
            "wood": (self.theme["wood_soft"], self.theme["wood"]),
            "danger": ("#F4DEDE", self.theme["danger"]),
        }
        bg, fg = palette[kind]
        btn = tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg,
            fg=fg,
            relief="flat",
            activebackground=bg,
            activeforeground=fg,
            padx=16,
            pady=10,
            width=width,
            cursor="hand2",
            wraplength=280,
            justify="left",
            bd=0,
            highlightthickness=0,
            font=("SimHei UI", 10, "bold" if kind == "accent" else "normal"),
        )
        self.decorate_button(btn)
        return btn

    def make_card(self, parent, title, subtitle="", padding=(18, 18)):
        card = tk.Frame(parent, bg=self.theme["card"], highlightthickness=6, highlightbackground=self.theme["accent_soft"], bd=1, relief="groove")
        head = tk.Frame(card, bg=self.theme["card"])
        head.pack(fill="x", padx=padding[0], pady=(padding[1], 8))
        tk.Label(head, text=title, bg=self.theme["card"], fg=self.theme["text"], font=("SimHei UI", 13, "bold")).pack(anchor="w")
        if subtitle:
            tk.Label(head, text=subtitle, bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9)).pack(anchor="w", pady=(4, 0))
        return card

    def create_scroll_page(self):
        outer = tk.Frame(self.page_host, bg=self.theme["bg"])
        outer.grid(row=0, column=0, sticky="nsew")
        outer.grid_rowconfigure(0, weight=1)
        outer.grid_columnconfigure(0, weight=1)
        canvas = tk.Canvas(outer, bg=self.theme["bg"], highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        frame = tk.Frame(canvas, bg=self.theme["bg"])
        frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        window_id = canvas.create_window((0, 0), window=frame, anchor="nw")

        def sync_page_width(_event=None):
            viewport_width = max(320, canvas.winfo_width() - 2)
            canvas.itemconfigure(window_id, width=viewport_width)

        canvas.bind("<Configure>", sync_page_width)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        outer.bind("<Enter>", lambda e: self.bind_all("<MouseWheel>", lambda ev: canvas.yview_scroll(int(-ev.delta / 120), "units")))
        outer.bind("<Leave>", lambda e: self.unbind_all("<MouseWheel>"))
        self.page_widgets.append(outer)
        return frame

    def clear_page(self):
        for widget in self.page_host.winfo_children():
            widget.destroy()
        self.page_widgets = []

    def fade(self, start, end, callback=None, steps=8, delay=24):
        if not self.fade_enabled:
            self.attributes("-alpha", end)
            if callback:
                callback()
            return
        delta = (end - start) / steps

        def step(index=0, current=start):
            self.attributes("-alpha", current)
            if index >= steps:
                self.attributes("-alpha", end)
                if callback:
                    callback()
                return
            self.after(delay, lambda: step(index + 1, current + delta))

        step()

    def transition_to(self, page_name):
        self.flush_textlab_autosave()
        self.selected_nav.set(page_name)
        self.refresh_nav_style()

        def load():
            self.clear_page()
            mapping = {
                "dashboard": self.show_dashboard,
                "textlab": self.show_textlab,
                "vocab": self.show_vocab,
                "review": self.show_review,
                "test": self.show_test,
                "mistakes": self.show_mistakes,
                "report": self.show_report,
                "settings": self.show_settings,
            }
            mapping[page_name]()
            self.fade(0.8, 1.0)

        self.fade(1.0, 0.8, load)

    def get_today_task(self):
        vocab = self.db.list_vocab(order_by="v.created_at DESC")
        reviews = self.db.get_today_review(self.review_stages, 9999)
        mistakes = self.db.list_mistakes()
        return {
            "review": min(len(reviews), int(self.settings.get("daily_review_limit", "15"))),
            "new": min(sum(1 for row in vocab if (row.get("review_count") or 0) == 0), int(self.settings.get("daily_new_limit", "5"))),
            "mistake": min(len(mistakes), 5),
        }

    def stage_text(self, row):
        if row.get("mastered"):
            return "已掌握"
        if (row.get("review_count") or 0) == 0:
            return "新词"
        return f"阶段 {row.get('stage_index', 0)}"

    def display_word(self, word):
        if self.furigana_var.get():
            return self.engine.annotate_text(word, True)
        return word

    def get_quiz_direction(self, mode):
        mapping = {
            "review": self.review_direction_var.get(),
            "test": self.test_direction_var.get(),
            "practice": self.practice_direction_var.get(),
        }
        return mapping.get(mode, "日语 -> 中文")

    def build_quiz_choices(self, row, mode):
        direction = self.get_quiz_direction(mode)
        if direction == "中文 -> 日语":
            all_words = [item["word"] for item in self.db.list_vocab(order_by="v.updated_at DESC") if item.get("word")]
            pool = [word for word in dict.fromkeys(all_words) if word != row["word"]]
            random.shuffle(pool)
            choices = [row["word"]] + pool[:3]
            random.shuffle(choices)
            return choices
        return self.engine.build_choices(row["meaning"], self.get_quiz_meanings())

    def get_quiz_prompt(self, row, mode):
        direction = self.get_quiz_direction(mode)
        if direction == "中文 -> 日语":
            prompt = row["meaning"]
            hint = f"请选择对应的日语单词\n读音：{row.get('reading', '')}\n词性：{row.get('pos', '')}"
            return prompt, hint
        prompt = self.display_word(row["word"])
        hint = f"读音：{row.get('reading', '')}\n词性：{row.get('pos', '')}"
        return prompt, hint

    def evaluate_quiz_answer(self, choice, row, mode):
        direction = self.get_quiz_direction(mode)
        if direction == "中文 -> 日语":
            return "exact" if choice == row["word"] else "wrong"
        return self.engine.answer_matches(choice, row["meaning"])

    def apply_tree_sorting(self, tree, columns, cache_key_builder):
        for key, label, width in columns:
            tree.heading(key, text=label, command=lambda name=key: self.sort_tree_by_column(tree, name, cache_key_builder))
            tree.column(key, width=width, anchor="w", stretch=True)

    def sort_tree_by_column(self, tree, column, cache_key_builder):
        rows = [tree.item(item, "values") for item in tree.get_children()]
        if not rows:
            return
        direction_key = f"{tree}_{column}"
        reverse = not getattr(self, direction_key, False)
        setattr(self, direction_key, reverse)

        def sort_key(values):
            value = values[list(tree["columns"]).index(column)]
            try:
                return float(value)
            except Exception:
                return str(value)

        rows.sort(key=sort_key, reverse=reverse)
        self.sync_tree(tree, cache_key_builder(column, reverse), rows)

    def attach_treeview_menu(self, tree, kind="generic"):
        menu = tk.Menu(tree, tearoff=0)
        menu.add_command(label="复制选中内容", command=lambda: self.copy_tree_selection(tree))
        if kind == "vocab":
            menu.add_command(label="编辑词条", command=self.edit_selected_vocab)
            menu.add_command(label="标记已掌握", command=lambda: self.batch_set_mastery(True))
            menu.add_command(label="标记未掌握", command=lambda: self.batch_set_mastery(False))
            menu.add_command(label="提高优先级", command=lambda: self.batch_priority(3))
            menu.add_command(label="加入错题本", command=self.add_selected_vocab_to_mistakes)
            menu.add_command(label="移出错题本", command=self.remove_selected_vocab_from_mistakes)
        if kind == "focus":
            menu.add_command(label="查看详情", command=self.show_selected_word_detail)
        tree.bind("<Button-3>", lambda e: self.show_treeview_menu(e, tree, menu))
        tree.bind("<Control-c>", lambda e: self.copy_tree_selection(tree))

    def show_treeview_menu(self, event, tree, menu):
        item = tree.identify_row(event.y)
        if item:
            tree.selection_set(item)
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def copy_tree_selection(self, tree):
        selected = tree.selection()
        if not selected:
            return
        lines = ["\t".join(map(str, tree.item(item, "values"))) for item in selected]
        self.clipboard_clear()
        self.clipboard_append("\n".join(lines))
        self.set_status("已复制选中内容到剪贴板。", "success")

    def add_selected_vocab_to_mistakes(self):
        if not hasattr(self, "vocab_tree"):
            return
        selected = self.vocab_tree.selection()
        if not selected:
            return
        for item in selected:
            word = self.vocab_tree.item(item, "values")[0]
            vocab = self.db.get_vocab(word)
            if vocab:
                self.db.mark_mistake(word, vocab.get("meaning", ""))
        self.toast("已加入错题本", f"已加入 {len(selected)} 个词到错题本。", "success")

    def remove_selected_vocab_from_mistakes(self):
        if not hasattr(self, "vocab_tree"):
            return
        selected = self.vocab_tree.selection()
        if not selected:
            return
        for item in selected:
            word = self.vocab_tree.item(item, "values")[0]
            self.db.clear_mistake(word)
        self.toast("已移出错题本", f"已移出 {len(selected)} 个词。", "success")

    def is_meaningful_token(self, entry):
        word = entry.get("surface", "").strip()
        if not word:
            return False
        if re.fullmatch(r"[\W_]+", word):
            return False
        if re.fullmatch(r"[0-9]+", word):
            return False
        if re.fullmatch(r"[A-Za-z]", word):
            return False
        return True

    def should_add_to_mistakes(self, word):
        vocab = self.db.get_vocab(word) or {}
        projected_wrong = (vocab.get("wrong_count") or 0) + 1
        return projected_wrong >= 2

    def maybe_clear_mistake_after_success(self, word):
        vocab = self.db.get_vocab(word) or {}
        if (vocab.get("streak") or 0) >= 2:
            self.db.clear_mistake(word)

    def is_placeholder_meaning(self, meaning):
        value = str(meaning or "").strip()
        return value in {"", "未收录", "-", "无", "未知"}

    def get_quiz_meanings(self):
        return [
            row["meaning"]
            for row in self.db.list_vocab(order_by="v.priority DESC, v.created_at DESC")
            if not self.is_placeholder_meaning(row.get("meaning"))
        ]

    def get_quiz_rows(self, rows):
        meanings = self.get_quiz_meanings()
        if len(set(meanings)) < 2:
            return []
        return [row for row in rows if not self.is_placeholder_meaning(row.get("meaning"))]

    def show_dashboard(self):
        self.set_header("今日页", "像翻开一本安静的单词手帐，从这里开始今天的学习。")
        body = self.create_scroll_page()
        body.grid_columnconfigure(0, weight=7)
        body.grid_columnconfigure(1, weight=3)
        left = tk.Frame(body, bg=self.theme["bg"])
        right = tk.Frame(body, bg=self.theme["bg"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
        right.grid(row=0, column=1, sticky="nsew", padx=(12, 0))

        quote = QUOTES[date.today().toordinal() % len(QUOTES)]
        hero = self.make_card(left, "今日手帐", "轻一点、稳一点，把学习做成长期能坚持的样子。")
        hero.pack(fill="x")
        wrap = tk.Frame(hero, bg=self.theme["card"])
        wrap.pack(fill="x", padx=18, pady=(8, 18))
        tk.Label(
            wrap,
            text=quote[0],
            bg=self.theme["card"],
            fg=self.theme["text"],
            font=("Meiryo", self.jp_font_size + 6, "bold"),
            justify="left",
            wraplength=720,
        ).pack(anchor="w")
        tk.Label(
            wrap,
            text=quote[1],
            bg=self.theme["card"],
            fg=self.theme["muted"],
            font=("SimHei UI", self.cn_font_size),
            justify="left",
            wraplength=720,
        ).pack(anchor="w", pady=(8, 0))
        actions = tk.Frame(wrap, bg=self.theme["card"])
        actions.pack(fill="x", pady=(18, 0))
        action_specs = [
            ("开始复习", lambda: self.transition_to("review"), "accent"),
            ("快速学 5 词", self.open_quick_study_window, "wood"),
            ("课文手札", lambda: self.transition_to("textlab"), "soft"),
        ]
        for idx, (label, command, kind) in enumerate(action_specs):
            btn = self.make_action_button(actions, label, command, kind)
            btn.grid(row=0, column=idx, sticky="ew", padx=(0 if idx == 0 else 10, 0))
            actions.grid_columnconfigure(idx, weight=1)

        stats = tk.Frame(left, bg=self.theme["bg"])
        stats.pack(fill="x", pady=18)
        rows = self.db.list_vocab(order_by="v.created_at DESC")
        review_due = self.db.get_today_review(self.review_stages, 9999)
        mistakes = self.db.list_mistakes()
        mastered = sum(1 for row in rows if row.get("mastered"))
        cards = [("词汇本", str(len(rows)), "累计收录"), ("待复习", str(len(review_due)), "今天该见的词"), ("已掌握", str(mastered), "稳定记住的词"), ("错题本", str(len(mistakes)), "还需要再见一次")]
        for idx, item in enumerate(cards):
            card = self.make_card(stats, item[0], item[2])
            card.grid(row=0, column=idx, sticky="nsew", padx=(0 if idx == 0 else 8, 0))
            stats.grid_columnconfigure(idx, weight=1)
            tk.Label(card, text=item[1], bg=self.theme["card"], fg=self.theme["accent"] if idx != 2 else self.theme["good"], font=("SimHei UI", 26, "bold")).pack(anchor="w", padx=18, pady=(12, 14))

        plan = self.make_card(left, "学习建议", "根据当前节奏，给你一个不费力但有效率的安排。")
        plan.pack(fill="x")
        task = self.get_today_task()
        box = tk.Frame(plan, bg=self.theme["card"])
        box.pack(fill="x", padx=18, pady=(6, 18))
        lines = [
            f"先复习 {task['review']} 个词，优先照顾记忆即将松动的部分。",
            f"如果状态还不错，再新学 {task['new']} 个词，保持输入和消化的平衡。",
            f"错题本里有 {task['mistake']} 个词值得回看，它们最容易带来提升。",
        ]
        for line in lines:
            item = tk.Frame(box, bg=self.theme["card"])
            item.pack(fill="x", pady=5)
            dot = tk.Canvas(item, width=16, height=16, bg=self.theme["card"], highlightthickness=0)
            dot.create_oval(4, 4, 12, 12, fill=self.theme["accent"], outline="")
            dot.pack(side="left")
            tk.Label(item, text=line, bg=self.theme["card"], fg=self.theme["text"], font=("SimHei UI", self.cn_font_size)).pack(side="left")

        starter = self.make_card(right, "新手起步", "第一次使用时，只要按这三个步骤来就够了。")
        starter.pack(fill="x")
        starter_box = tk.Frame(starter, bg=self.theme["card"])
        starter_box.pack(fill="x", padx=18, pady=(8, 18))
        starter_lines = [
            "1. 先在课文手札里贴一段课文。",
            "2. 把想学的词加入词汇本。",
            "3. 回到复习计划开始今天学习。",
        ]
        for line in starter_lines:
            tk.Label(starter_box, text=line, bg=self.theme["card"], fg=self.theme["text"], justify="left", wraplength=320).pack(anchor="w", pady=4)

        state = self.make_card(right, "今日状态", "让系统安静地记住你的节奏，不急，也不停。")
        state.pack(fill="x", pady=18)
        inner = tk.Frame(state, bg=self.theme["card"])
        inner.pack(fill="x", padx=18, pady=(10, 18))
        tk.Label(inner, text=f"连续学习 {self.streak} 天", bg=self.theme["card"], fg=self.theme["wood"], font=("SimHei UI", 20, "bold")).pack(anchor="w")
        tk.Label(inner, text="你的学习记录正在形成一条稳定而清晰的轨迹。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 10)).pack(anchor="w", pady=(8, 0))

        note = self.make_card(right, "最近课文", "上次打开的课文和标注，会在这里安静地等你回来。")
        note.pack(fill="both", expand=True, pady=18)
        text_preview = self.current_note["content"][:240] + ("..." if len(self.current_note["content"]) > 240 else "")
        tk.Label(note, text=text_preview or "暂时还没有课文内容，可以去课文手札添加一段新的文本。", bg=self.theme["card"], fg=self.theme["text"] if text_preview else self.theme["muted"], wraplength=360, justify="left", font=("SimHei UI", self.cn_font_size)).pack(fill="both", expand=True, padx=18, pady=(10, 18), anchor="nw")

    def show_textlab(self):
        self.set_header("课文手札", "从分词统计走向场景化学习，让课文本身成为可反复回看的学习现场。")
        body = self.create_scroll_page()
        compact = (not self.sidebar_collapsed) or self.cn_font_size >= 13 or self.winfo_width() < 1520
        if compact:
            body.grid_columnconfigure(0, weight=1)
        else:
            body.grid_columnconfigure(0, weight=8)
            body.grid_columnconfigure(1, weight=2)
        left = tk.Frame(body, bg=self.theme["bg"])
        right = tk.Frame(body, bg=self.theme["bg"])
        if compact:
            left.grid(row=0, column=0, sticky="nsew")
            right.grid(row=1, column=0, sticky="nsew", pady=(18, 0))
        else:
            left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            right.grid(row=0, column=1, sticky="nsew", padx=(12, 0))

        editor = self.make_card(left, "原文展示区", "先贴课文，再分析单词；只有加入词汇本的词，后面才会进入复习。")
        editor.pack(fill="both", expand=True)
        tools = tk.Frame(editor, bg=self.theme["card"])
        tools.pack(fill="x", padx=18, pady=(8, 10))
        tool_main = tk.Frame(tools, bg=self.theme["card"])
        tool_main.pack(fill="x")
        self.make_action_button(tool_main, "分析这段课文", self.async_analyze_text, "accent").pack(side="left", padx=(0, 8))
        self.make_action_button(tool_main, "停止分析", self.cancel_text_analysis, "danger").pack(side="left", padx=8)
        self.make_action_button(tool_main, "导入文本", self.load_text_file, "soft").pack(side="left", padx=8)
        self.make_action_button(tool_main, "收藏这段句子", self.mark_current_selection, "wood").pack(side="left", padx=8)
        tool_aux = tk.Frame(tools, bg=self.theme["card"])
        tool_aux.pack(fill="x", pady=(10, 0))
        tk.Checkbutton(tool_aux, text="显示注音", variable=self.furigana_var, command=self.refresh_annotated_preview, bg=self.theme["card"], fg=self.theme["text"], activebackground=self.theme["card"], font=("SimHei UI", 10), selectcolor=self.theme["card"]).pack(side="left")
        self.loading_label = tk.Label(tool_aux, text="", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9))
        self.loading_label.pack(side="right")
        tip_wrap = 760 if not compact else 980
        tk.Label(editor, text="使用顺序：1. 贴入课文  2. 点“分析这段课文”  3. 在右侧选词  4. 点“加入词汇本并进入复习”。输入后会自动保存；停下约 3 秒才会自动分析。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9), wraplength=tip_wrap, justify="left").pack(anchor="w", padx=18, pady=(0, 8))

        editor_text_wrap = tk.Frame(editor, bg=self.theme["card"])
        editor_text_wrap.pack(fill="both", expand=True, padx=18, pady=(0, 10))
        self.text_editor = tk.Text(
            editor_text_wrap,
            wrap="word",
            bg=self.theme["panel"],
            fg=self.theme["text"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.theme["line"],
            highlightcolor=self.theme["accent"],
            insertbackground=self.theme["text"],
            font=("SimHei UI", self.cn_font_size),
            padx=14,
            pady=14,
            height=22,
        )
        text_scroll_y = ttk.Scrollbar(editor_text_wrap, orient="vertical", command=self.text_editor.yview)
        self.text_editor.configure(yscrollcommand=text_scroll_y.set)
        self.text_editor.pack(side="left", fill="both", expand=True)
        text_scroll_y.pack(side="right", fill="y")
        self.text_editor.insert("1.0", self.current_note["content"])
        self.text_editor.edit_modified(False)
        self.text_editor.bind("<<Modified>>", self.on_text_modified)
        self.text_editor.bind("<KeyRelease>", lambda e: self.schedule_text_analysis())

        preview = tk.Frame(editor, bg=self.theme["card"])
        preview.pack(fill="x", padx=18, pady=(0, 18))
        tk.Label(preview, text="注音预览", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9)).pack(anchor="w")
        self.annotated_preview = tk.Text(preview, wrap="word", height=7, bg=self.theme["card"], fg=self.theme["text"], relief="flat", font=("SimHei UI", self.cn_font_size - 1), padx=2, pady=8)
        self.annotated_preview.pack(fill="x", expand=False)
        self.annotated_preview.configure(state="disabled")
        self.refresh_annotated_preview()

        filters = self.make_card(right, "分析结果与候选词", "这里是课文里识别出的词，还没有自动进入你的词汇本。")
        filters.pack(fill="both", expand=True)
        bar = tk.Frame(filters, bg=self.theme["card"])
        bar.pack(fill="x", padx=18, pady=(8, 10))
        if compact:
            ttk.Combobox(bar, textvariable=self.text_filter_pos, values=POS_CHOICES, state="readonly", width=12).pack(fill="x", pady=(0, 8))
            ttk.Combobox(bar, textvariable=self.text_filter_state, values=["全部状态", "未收录", "已收录"], state="readonly", width=12).pack(fill="x", pady=(0, 8))
            self.make_action_button(bar, "应用筛选", self.render_text_analysis, "soft").pack(fill="x", pady=(0, 8))
            self.make_action_button(bar, "把未收录词都加入词汇本", self.add_all_untracked_focus_words, "accent").pack(fill="x")
        else:
            ttk.Combobox(bar, textvariable=self.text_filter_pos, values=POS_CHOICES, state="readonly", width=10).pack(side="left")
            ttk.Combobox(bar, textvariable=self.text_filter_state, values=["全部状态", "未收录", "已收录"], state="readonly", width=10).pack(side="left", padx=8)
            self.make_action_button(bar, "应用筛选", self.render_text_analysis, "soft").pack(side="left")
            self.make_action_button(bar, "把未收录词都加入词汇本", self.add_all_untracked_focus_words, "accent").pack(side="right")

        self.focus_tree = ttk.Treeview(filters, columns=("word", "meaning", "reading", "pos", "state"), show="headings", style="Journal.Treeview", selectmode="extended", height=14 if compact else 16)
        focus_columns = [("word", "单词", 150), ("meaning", "释义", 300), ("reading", "读音", 220), ("pos", "词性", 78), ("state", "状态", 78)]
        self.apply_tree_sorting(self.focus_tree, focus_columns, lambda col, rev: f"focus:{col}:{int(rev)}")
        focus_outer = tk.Frame(filters, bg=self.theme["card"])
        focus_outer.pack(fill="both", expand=True, padx=18, pady=(0, 12))
        focus_outer.grid_columnconfigure(0, weight=1)
        focus_outer.grid_rowconfigure(0, weight=1)
        focus_tree_wrap = tk.Frame(focus_outer, bg=self.theme["card"])
        focus_tree_wrap.grid(row=0, column=0, sticky="nsew")
        focus_scroll_x = ttk.Scrollbar(focus_outer, orient="horizontal", command=self.focus_tree.xview)
        focus_scroll_y = ttk.Scrollbar(focus_outer, orient="vertical", command=self.focus_tree.yview)
        self.focus_tree.configure(xscrollcommand=focus_scroll_x.set, yscrollcommand=focus_scroll_y.set)
        self.focus_tree.pack(fill="both", expand=True)
        focus_scroll_y.grid(row=0, column=1, sticky="ns", padx=(6, 0))
        focus_scroll_x.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        self.focus_tree.bind("<Double-1>", lambda e: self.show_selected_word_detail())
        self.attach_treeview_menu(self.focus_tree, "focus")
        action_row = tk.Frame(filters, bg=self.theme["card"])
        action_row.pack(fill="x", padx=18, pady=(0, 18))
        if compact:
            self.make_action_button(action_row, "加入词汇本并进入复习", self.add_selected_focus_words, "accent").pack(fill="x", pady=(0, 8))
            self.make_action_button(action_row, "补一个意思后快速加入", self.quick_add_from_textlab, "wood").pack(fill="x", pady=(0, 8))
            self.make_action_button(action_row, "查看详情", self.show_selected_word_detail, "soft").pack(fill="x")
        else:
            self.make_action_button(action_row, "加入词汇本并进入复习", self.add_selected_focus_words, "accent").pack(side="left")
            self.make_action_button(action_row, "补一个意思后快速加入", self.quick_add_from_textlab, "wood").pack(side="left", padx=8)
            self.make_action_button(action_row, "查看详情", self.show_selected_word_detail, "soft").pack(side="left", padx=8)
        tk.Label(filters, text="说明：右侧词表只是“课文分析结果”。只有加入词汇本后，这些词才会进入复习、自测和错题追踪。双击可看详情，右键有更多操作。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9), wraplength=520 if compact else 360, justify="left").pack(anchor="w", padx=18, pady=(0, 18))

        freq = self.make_card(right, "词频统计", "这里只是辅助参考，优先看上面的候选词列表就够了。")
        freq.pack(fill="x", pady=(18, 0))
        self.freq_tree = ttk.Treeview(freq, columns=("word", "count"), show="headings", style="Journal.Treeview", height=5 if compact else 6)
        self.apply_tree_sorting(self.freq_tree, [("word", "单词", 260), ("count", "出现次数", 110)], lambda col, rev: f"freq:{col}:{int(rev)}")
        freq_wrap = tk.Frame(freq, bg=self.theme["card"])
        freq_wrap.pack(fill="both", expand=True, padx=18, pady=(8, 18))
        freq_scroll_y = ttk.Scrollbar(freq_wrap, orient="vertical", command=self.freq_tree.yview)
        self.freq_tree.configure(yscrollcommand=freq_scroll_y.set)
        self.freq_tree.pack(side="left", fill="both", expand=True)
        freq_scroll_y.pack(side="right", fill="y")
        self.attach_treeview_menu(self.freq_tree, "generic")
        self.render_text_analysis()

    def schedule_text_analysis(self):
        if self.analysis_in_progress:
            return
        if not hasattr(self, "text_editor"):
            return
        content = self.text_editor.get("1.0", "end").strip()
        if len(content) < 60:
            return
        if abs(len(content) - len(self.last_auto_analyzed_content)) < 40:
            return
        if self.analysis_job:
            self.after_cancel(self.analysis_job)
        delay = 4500 if len(content) > 500 else 3200
        self.analysis_job = self.after(delay, lambda: self.async_analyze_text(silent=True))

    def on_text_modified(self, _event=None):
        if not self.auto_save_textlab or not hasattr(self, "text_editor"):
            return
        if self.text_editor.edit_modified():
            self.schedule_text_autosave()
            self.refresh_annotated_preview()
            self.text_editor.edit_modified(False)

    def schedule_text_autosave(self):
        if self.autosave_job:
            self.after_cancel(self.autosave_job)
        self.autosave_job = self.after(800, self.flush_textlab_autosave)

    def flush_textlab_autosave(self):
        if self.autosave_job:
            self.after_cancel(self.autosave_job)
            self.autosave_job = None
        if hasattr(self, "text_editor") and self.text_editor.winfo_exists():
            self.save_current_note()
            if self.selected_nav.get() == "textlab":
                self.set_status("课文已自动保存。", "info")

    def cancel_text_analysis(self):
        if self.analysis_job:
            self.after_cancel(self.analysis_job)
            self.analysis_job = None
        self.analysis_generation += 1
        self.analysis_in_progress = False
        self.stop_spinner()
        self.set_status("已取消当前分词分析。", "warning")

    def refresh_annotated_preview(self):
        content = self.text_editor.get("1.0", "end").strip() if hasattr(self, "text_editor") else self.current_note["content"]
        preview = self.engine.annotate_text(content, self.furigana_var.get())
        if hasattr(self, "annotated_preview"):
            self.annotated_preview.configure(state="normal")
            self.annotated_preview.delete("1.0", "end")
            self.annotated_preview.insert("1.0", preview)
            self.annotated_preview.configure(state="disabled")

    def mark_current_selection(self):
        try:
            selected = self.text_editor.get("sel.first", "sel.last").strip()
        except tk.TclError:
            safe_message("info", "提示", "请先在原文里选中要标记的句子或段落。")
            return
        segments = self.deserialize_json(self.current_note.get("segments", "[]"))
        if selected and selected not in segments:
            segments.append(selected)
        self.current_note["segments"] = self.serialize_json(segments)
        self.save_current_note()
        safe_message("info", "已收藏", "已将选中的内容加入重点段落，后续可以把它们当作重点句子反复回看。")

    def serialize_json(self, value):
        return json.dumps(value, ensure_ascii=False)

    def deserialize_json(self, value):
        try:
            return json.loads(value or "[]")
        except Exception:
            return []

    def save_current_note(self):
        content = self.text_editor.get("1.0", "end").strip() if hasattr(self, "text_editor") else self.current_note["content"]
        segments = self.deserialize_json(self.current_note.get("segments", "[]"))
        highlights = self.deserialize_json(self.current_note.get("highlights", "[]"))
        self.db.save_text_note(content, segments, highlights)
        self.current_note = self.db.get_text_note()

    def async_analyze_text(self, silent=False):
        content = self.text_editor.get("1.0", "end").strip()
        self.current_note["content"] = content
        self.save_current_note()
        if self.analysis_job:
            self.after_cancel(self.analysis_job)
            self.analysis_job = None
        self.analysis_generation += 1
        generation = self.analysis_generation
        self.analysis_in_progress = True
        if not silent:
            self.start_spinner("正在温和地分词整理…")

        def worker(text):
            try:
                entries = []
                chunks = self.engine.analyze_text_tokens_chunked(text, chunk_size=120)
                total = max(1, len(chunks))
                for index, chunk in enumerate(chunks, start=1):
                    if generation != self.analysis_generation:
                        return
                    chunk_entries = [entry for entry in self.engine.analyze_text_tokens(chunk) if self.is_meaningful_token(entry)]
                    entries.extend(chunk_entries)
                    self.async_queue.put(("analysis_progress", index, total, silent, generation))
                self.async_queue.put(("analysis_done", entries, None, silent, generation))
            except Exception as exc:
                self.async_queue.put(("analysis_done", None, str(exc), silent, generation))

        threading.Thread(target=worker, args=(content,), daemon=True).start()

    def start_spinner(self, prefix):
        dots = ["◜", "◠", "◝", "◞", "◡", "◟"]
        self.spinner_index = 0

        def animate():
            self.loading_label.config(text=f"{dots[self.spinner_index % len(dots)]} {prefix}")
            self.spinner_index += 1
            self.spinner_job = self.after(160, animate)

        self.stop_spinner()
        animate()

    def stop_spinner(self):
        if self.spinner_job:
            self.after_cancel(self.spinner_job)
            self.spinner_job = None
        if hasattr(self, "loading_label"):
            self.loading_label.config(text="")

    def poll_async_queue(self):
        try:
            while True:
                event = self.async_queue.get_nowait()
                if event[0] == "analysis_progress":
                    _, index, total, silent, generation = event
                    if generation != self.analysis_generation:
                        continue
                    if not silent and hasattr(self, "loading_label"):
                        self.loading_label.config(text=f"分析中 {index}/{total}")
                    continue
                if event[0] == "analysis_done":
                    _, entries, error, silent, generation = event
                    if generation != self.analysis_generation:
                        continue
                    self.analysis_in_progress = False
                    if not silent:
                        self.stop_spinner()
                    if error:
                        safe_message("error", "分词失败", error)
                        self.set_status("课文分词失败，请检查文本内容或 Janome 环境。", "error")
                    else:
                        self.current_word_entries = entries
                        self.current_words = [item["surface"] for item in entries]
                        if silent and hasattr(self, "text_editor"):
                            self.last_auto_analyzed_content = self.text_editor.get("1.0", "end").strip()
                        self.render_text_analysis()
                        self.refresh_annotated_preview()
                        if not silent:
                            self.set_status(f"课文分析完成，提取到 {len(entries)} 个有效词。", "success")
        except queue.Empty:
            pass
        self.after(120, self.poll_async_queue)

    def load_text_file(self):
        path = filedialog.askopenfilename(title="选择文本文件", filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")], initialdir=str(BASE_DIR))
        if not path:
            return
        try:
            content = Path(path).read_text(encoding="utf-8")
            self.text_editor.delete("1.0", "end")
            self.text_editor.insert("1.0", content)
            self.current_note["content"] = content
            self.save_current_note()
            self.refresh_annotated_preview()
            self.schedule_text_analysis()
            self.set_status("课文已导入，系统正在自动分析。", "success")
        except UnicodeDecodeError:
            safe_message("error", "读取失败", "文本文件编码不正确，请确认文件为 UTF-8 格式。")
        except Exception as exc:
            logging.exception("读取文本失败")
            safe_message("error", "读取失败", f"文本读取失败：{exc}")

    def render_text_analysis(self):
        content = self.text_editor.get("1.0", "end").strip() if hasattr(self, "text_editor") else self.current_note["content"]
        if content and not self.current_word_entries:
            self.current_word_entries = self.engine.analyze_text_tokens(content)
        if not content:
            self.current_word_entries = []
        filtered_entries = [entry for entry in self.current_word_entries if self.is_meaningful_token(entry)]
        counter = Counter(entry["surface"] for entry in filtered_entries)
        freq_rows = [(word, count) for word, count in counter.most_common(20)]
        if hasattr(self, "freq_tree"):
            self.sync_tree(self.freq_tree, "freq", freq_rows)
        system_dict = self.db.load_system_dict()
        vocab_rows = self.db.list_vocab(order_by="v.created_at DESC")
        vocab_words = {row["word"] for row in vocab_rows}
        rows = []
        seen = set()
        for entry in filtered_entries:
            word = entry["surface"]
            if word in seen:
                continue
            seen.add(word)
            detail = {
                "reading": entry.get("reading", "-"),
                "base_form": entry.get("base", word),
                "pos": entry.get("pos", "未知"),
            }
            meaning = system_dict.get(word) or system_dict.get(detail["base_form"]) or self.db.get_vocab(word) or self.db.get_vocab(detail["base_form"])
            if isinstance(meaning, dict):
                meaning = meaning.get("meaning", "未收录")
            meaning = meaning or "未收录"
            state = "已收录" if word in vocab_words else "未收录"
            filter_pos = self.text_filter_pos.get()
            if filter_pos != "全部词性":
                target_pos = POS_MAP_UI_TO_DB.get(filter_pos, filter_pos)
                if detail["pos"] != target_pos:
                    continue
            if self.text_filter_state.get() != "全部状态" and state != self.text_filter_state.get():
                continue
            rows.append((word, meaning, detail["reading"], detail["pos"], state))
        if hasattr(self, "focus_tree"):
            self.sync_tree(self.focus_tree, "focus", rows)

    def get_untracked_focus_rows(self):
        if not hasattr(self, "focus_tree"):
            return []
        rows = []
        for item_id in self.focus_tree.get_children():
            values = self.focus_tree.item(item_id, "values")
            if len(values) >= 5 and values[4] == "未收录":
                rows.append(values)
        return rows

    def add_all_untracked_focus_words(self):
        rows = self.get_untracked_focus_rows()
        if not rows:
            safe_message("info", "提示", "当前筛选结果里没有未收录词。")
            return
        preview = "\n".join(f"{word} = {meaning}" for word, meaning, *_ in rows[:15])
        if not messagebox.askyesno("批量收录确认", f"将要加入 {len(rows)} 个未收录词。\n\n前 15 项预览：\n{preview}\n\n是否继续？"):
            return
        count = 0
        skipped = 0
        for word, meaning, *_rest in rows:
            if not self.db.get_vocab(word):
                self.add_focus_word(word, meaning, silent=True)
                count += 1
            else:
                skipped += 1
        self.render_text_analysis()
        self.toast("批量收录完成", f"已加入 {count} 个，跳过 {skipped} 个。", "success")

    def show_selected_word_detail(self):
        selected = self.focus_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择一个单词。")
            return
        values = self.focus_tree.item(selected[0], "values")
        word = values[0]
        vocab = self.db.get_vocab(word)
        detail = self.engine.get_word_detail(word)
        meaning = vocab["meaning"] if vocab else values[1]
        top = tk.Toplevel(self)
        top.title(f"词条详情 | {word}")
        top.configure(bg=self.theme["panel"])
        width = 420
        height = 360
        screen_width = top.winfo_screenwidth()
        screen_height = top.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        top.geometry(f"{width}x{height}+{x}+{y}")
        top.transient(self)
        top.grab_set()
        box = tk.Frame(top, bg=self.theme["panel"], padx=22, pady=22)
        box.pack(fill="both", expand=True)
        tk.Label(box, text=word, bg=self.theme["panel"], fg=self.theme["text"], font=("Meiryo", self.jp_font_size + 8, "bold")).pack(anchor="w")
        tk.Label(box, text=f"读音：{detail['reading']}", bg=self.theme["panel"], fg=self.theme["muted"]).pack(anchor="w", pady=(10, 0))
        tk.Label(box, text=f"原形：{detail['base_form']}", bg=self.theme["panel"], fg=self.theme["muted"]).pack(anchor="w", pady=(4, 0))
        tk.Label(box, text=f"词性：{detail['pos']}", bg=self.theme["panel"], fg=self.theme["muted"]).pack(anchor="w", pady=(4, 0))
        tk.Label(box, text=f"释义：{meaning}", bg=self.theme["panel"], fg=self.theme["text"], wraplength=340, justify="left").pack(anchor="w", pady=(12, 0))
        if vocab and vocab.get("example"):
            tk.Label(box, text=f"例句：{vocab['example']}", bg=self.theme["panel"], fg=self.theme["muted"], wraplength=340, justify="left").pack(anchor="w", pady=(12, 0))
        actions = tk.Frame(box, bg=self.theme["panel"])
        actions.pack(anchor="w", pady=(18, 0))
        self.make_action_button(actions, "加入词汇本", lambda: [self.add_focus_word(word, meaning), top.destroy()], "accent").pack(side="left")
        self.make_action_button(actions, "直接编辑", lambda: [top.destroy(), self.open_word_editor(vocab or {"word": word, "meaning": meaning, "reading": detail["reading"], "base_form": detail["base_form"], "pos": detail["pos"], "tags": "", "example": "", "notes": "", "priority": 1})], "soft").pack(side="left", padx=8)
        self.make_action_button(actions, "关闭", top.destroy, "wood").pack(side="left", padx=8)

    def add_focus_word(self, word, meaning, silent=False):
        if self.db.get_vocab(word):
            if not silent:
                self.set_status(f"{word} 已经在词汇本里。", "warning")
            return
        entry_detail = next((item for item in self.current_word_entries if item["surface"] == word), None)
        detail = self.engine.get_word_detail(word)
        reading = entry_detail.get("reading") if entry_detail else detail["reading"]
        base_form = entry_detail.get("base") if entry_detail else detail["base_form"]
        pos = entry_detail.get("pos") if entry_detail else detail["pos"]
        forms = self.engine.infer_verb_forms(base_form, pos)
        current = now_ts()
        payload = {
            "word": word,
            "meaning": meaning,
            "reading": reading or detail["reading"],
            "base_form": base_form or detail["base_form"],
            "pos": pos or detail["pos"],
            "tags": "",
            "example": "",
            "priority": 1,
            "notes": "",
            "created_at": current,
            "updated_at": current,
            "polite_form": forms["polite"],
            "te_form": forms["te"],
            "ta_form": forms["ta"],
        }
        self.db.upsert_vocab(payload)
        logging.info("新增单词: %s", word)
        if not silent:
            self.toast("收录成功", f"{word} 已加入词汇本。", "success")

    def add_selected_focus_words(self):
        selected = self.focus_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择至少一个单词。")
            return
        count = 0
        skipped = 0
        for item_id in selected:
            word, meaning, _, _, _ = self.focus_tree.item(item_id, "values")
            if not self.db.get_vocab(word):
                self.add_focus_word(word, meaning, silent=True)
                count += 1
            else:
                skipped += 1
        self.render_text_analysis()
        if count:
            self.toast("加入完成", f"已把 {count} 个课文词加入词汇本。它们现在会进入后续复习和自测。", "success")
            if messagebox.askyesno("已加入词汇本", f"已加入 {count} 个单词，跳过 {skipped} 个已存在词。\n\n这些词现在会进入复习和自测。是否立刻前往复习计划？"):
                self.transition_to("review")
        else:
            safe_message("info", "提示", "选中的单词已经都在词汇本里了，它们后续会直接参与复习。")

    def quick_add_from_textlab(self):
        selected = self.focus_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择一个课文词。")
            return
        word, current_meaning, *_rest = self.focus_tree.item(selected[0], "values")
        meaning = self.simple_prompt("快速加入词汇本", f"为「{word}」补一个中文意思，保存后它就会进入复习：", current_meaning if current_meaning != "未收录" else "")
        if meaning is None:
            return
        if not meaning.strip():
            safe_message("warning", "提示", "释义不能为空。")
            return
        self.add_focus_word(word, meaning.strip())
        self.render_text_analysis()

    def show_vocab(self):
        self.set_header("词汇本", "从简单存储升级到精细化管理，标签、例句、排序和批量操作都可以从这里完成。")
        body = self.create_scroll_page()
        card = self.make_card(body, "我的词汇本", "支持标签筛选、批量管理、拓展信息和排序。")
        card.pack(fill="both", expand=True)
        tk.Label(card, text="新手先用上面的搜索框就够了；下面更多筛选和排序是进阶工具。双击词条或按 F2 可编辑，右键可做批量操作。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9), wraplength=1100, justify="left").pack(anchor="w", padx=18, pady=(8, 0))
        filter_bar = tk.Frame(card, bg=self.theme["card"])
        filter_bar.pack(fill="x", padx=18, pady=(8, 10))
        tk.Entry(filter_bar, textvariable=self.vocab_search_var, width=16).pack(side="left")
        self.make_action_button(filter_bar, "更多筛选", self.toggle_vocab_filters, "soft").pack(side="left", padx=8)
        self.make_action_button(filter_bar, "新增单词", self.open_word_editor, "accent").pack(side="left", padx=(16, 8))
        self.make_action_button(filter_bar, "快速添加", self.quick_add_vocab, "wood").pack(side="left")
        self.make_action_button(filter_bar, "导出 CSV", self.export_vocab, "wood").pack(side="right", padx=(8, 0))
        self.make_action_button(filter_bar, "导入 Excel", self.import_excel, "soft").pack(side="right")
        self.advanced_filter_bar = tk.Frame(card, bg=self.theme["card"])
        self.advanced_filter_bar.pack(fill="x", padx=18, pady=(0, 10))
        tk.Label(self.advanced_filter_bar, text="标签", bg=self.theme["card"], fg=self.theme["muted"]).pack(side="left", padx=(0, 4))
        tk.Entry(self.advanced_filter_bar, textvariable=self.vocab_filter_tag, width=12).pack(side="left")
        ttk.Combobox(self.advanced_filter_bar, textvariable=self.vocab_filter_pos, values=POS_CHOICES, state="readonly", width=10).pack(side="left", padx=8)
        ttk.Combobox(self.advanced_filter_bar, textvariable=self.vocab_filter_state, values=["全部状态", "learning", "mastered"], state="readonly", width=10).pack(side="left", padx=8)
        ttk.Combobox(self.advanced_filter_bar, textvariable=self.vocab_scope_var, values=["全部范围", "今天新增", "今天复习", "高频错词", "高优先级", "久未复习", "例句为空"], state="readonly", width=12).pack(side="left", padx=8)
        ttk.Combobox(self.advanced_filter_bar, textvariable=self.vocab_sort_key, values=["加入时间", "复习次数", "熟悉度", "标签"], state="readonly", width=10).pack(side="left", padx=8)
        tk.Checkbutton(self.advanced_filter_bar, text="默认隐藏已掌握", variable=self.hide_mastered_var, bg=self.theme["card"], fg=self.theme["text"], selectcolor=self.theme["card"]).pack(side="left", padx=8)
        if not self.show_advanced_filters_var.get():
            self.advanced_filter_bar.pack_forget()
        batch = tk.Frame(card, bg=self.theme["card"])
        batch.pack(fill="x", padx=18, pady=(0, 10))
        self.make_action_button(batch, "编辑选中", self.edit_selected_vocab, "soft").pack(side="left")
        self.make_action_button(batch, "批量删除", self.batch_delete_vocab, "danger").pack(side="left", padx=8)
        self.make_action_button(batch, "批量标签", self.batch_tag_vocab, "wood").pack(side="left", padx=8)
        self.make_action_button(batch, "提高优先级", lambda: self.batch_priority(3), "soft").pack(side="left", padx=8)
        self.make_action_button(batch, "标记已掌握", lambda: self.batch_set_mastery(True), "accent").pack(side="left", padx=8)
        self.make_action_button(batch, "标记未掌握", lambda: self.batch_set_mastery(False), "wood").pack(side="left", padx=8)
        self.vocab_tree = ttk.Treeview(card, columns=("word", "meaning", "reading", "pos", "tags", "stage", "review", "priority"), show="headings", style="Journal.Treeview", selectmode="extended", height=18)
        columns = [("word", "单词", 120), ("meaning", "释义", 320), ("reading", "读音", 110), ("pos", "词性", 80), ("tags", "标签", 220), ("stage", "熟悉度", 90), ("review", "复习次数", 80), ("priority", "优先级", 70)]
        self.apply_tree_sorting(self.vocab_tree, columns, lambda col, rev: f"vocab:{col}:{int(rev)}")
        self.vocab_tree.pack(fill="both", expand=True, padx=18, pady=(0, 18))
        self.vocab_tree.bind("<Double-1>", lambda e: self.edit_selected_vocab())
        self.bind("<F2>", lambda e: self.edit_selected_vocab())
        self.attach_treeview_menu(self.vocab_tree, "vocab")
        tk.Label(card, text="提示：双击词条或按 F2 可编辑；“重要程度”越高，越容易在复习和测试里提前出现。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9)).pack(anchor="w", padx=18, pady=(0, 18))
        for var in [self.vocab_search_var, self.vocab_filter_tag, self.vocab_filter_pos, self.vocab_filter_state, self.vocab_scope_var, self.vocab_sort_key, self.hide_mastered_var]:
            var.trace_add("write", lambda *_args: self.schedule_vocab_refresh())
        self.render_vocab_tree()

    def get_vocab_rows(self):
        order = {
            "加入时间": "v.created_at DESC",
            "复习次数": "IFNULL(r.review_count, 0) DESC",
            "熟悉度": "IFNULL(r.correct_count, 0) DESC",
            "标签": "v.tags ASC, v.created_at DESC",
        }.get(self.vocab_sort_key.get(), "v.created_at DESC")
        filters = {}
        if self.vocab_filter_tag.get().strip():
            filters["tag"] = self.vocab_filter_tag.get().strip()
        pos_text = self.vocab_filter_pos.get()
        if pos_text != "全部词性":
            db_pos = POS_MAP_UI_TO_DB.get(pos_text)
            if db_pos:
                filters["pos"] = db_pos
        if self.vocab_filter_state.get() != "全部状态":
            filters["state"] = self.vocab_filter_state.get()
        rows = self.db.list_vocab(order_by=order, filters=filters)
        if self.hide_mastered_var.get() and self.vocab_filter_state.get() == "全部状态":
            rows = [row for row in rows if not row.get("mastered")]
        keyword = self.vocab_search_var.get().strip().lower()
        if keyword:
            rows = [
                row for row in rows
                if keyword in row["word"].lower()
                or keyword in str(row.get("meaning", "")).lower()
                or keyword in str(row.get("reading", "")).lower()
                or keyword in str(row.get("tags", "")).lower()
            ]
        today = date.today().isoformat()
        scope = self.vocab_scope_var.get()
        if scope == "今天新增":
            rows = [row for row in rows if fmt_ts(row.get("created_at", 0)).startswith(today)]
        elif scope == "今天复习":
            rows = [row for row in rows if fmt_ts(row.get("last_review_at", 0)).startswith(today)]
        elif scope == "高频错词":
            rows = [row for row in rows if (row.get("wrong_count") or 0) >= 2]
        elif scope == "高优先级":
            rows = [row for row in rows if (row.get("priority") or 1) >= 3]
        elif scope == "久未复习":
            rows = [row for row in rows if not row.get("last_review_at") or (now_ts() - (row.get("last_review_at") or 0)) > 7 * 24 * 60 * 60]
        elif scope == "例句为空":
            rows = [row for row in rows if not str(row.get("example") or "").strip()]
        return rows

    def schedule_vocab_refresh(self):
        if hasattr(self, "_vocab_refresh_job") and self._vocab_refresh_job:
            self.after_cancel(self._vocab_refresh_job)
        self._vocab_refresh_job = self.after(180, self.safe_render_vocab_tree)

    def toggle_vocab_filters(self):
        self.show_advanced_filters_var.set(not self.show_advanced_filters_var.get())
        if self.show_advanced_filters_var.get():
            self.advanced_filter_bar.pack(fill="x", padx=18, pady=(0, 10), before=self.vocab_tree)
        else:
            self.advanced_filter_bar.pack_forget()

    def safe_render_vocab_tree(self):
        self._vocab_refresh_job = None
        if hasattr(self, "vocab_tree") and self.vocab_tree.winfo_exists():
            self.render_vocab_tree()

    def render_vocab_tree(self):
        rows = self.get_vocab_rows()
        payload = []
        for row in rows:
            payload.append((row["word"], row["meaning"], row.get("reading", ""), row.get("pos", ""), row.get("tags", ""), self.stage_text(row), row.get("review_count", 0), row.get("priority", 1)))
        self.sync_tree(self.vocab_tree, "vocab", payload)
        self.vocab_tree.selection_remove(*self.vocab_tree.selection())

    def open_word_editor(self, payload=None, mode="simple"):
        dialog = WordEditor(self, self.theme, "新增单词" if not payload else f"编辑词条 | {payload.get('word', '')}", payload=payload, mode=mode if not payload else "full")
        self.wait_window(dialog)
        if not dialog.result:
            return
        data = dialog.result
        existing = self.db.get_vocab(data["word"])
        if not payload and existing:
            if messagebox.askyesno("单词已存在", f"单词「{data['word']}」已存在。是否打开已有词条进行编辑？"):
                merged = existing.copy()
                merged.update(data)
                self.open_word_editor(merged)
            return

        detail = self.engine.get_word_detail(data["word"])
        reading = data["reading"] or detail["reading"]
        base_form = data["base_form"] or detail["base_form"]
        pos = data["pos"] or detail["pos"]
        forms = self.engine.infer_verb_forms(base_form, pos)
        current = now_ts()
        item = {
            "word": data["word"],
            "meaning": data["meaning"],
            "reading": reading,
            "base_form": base_form,
            "pos": pos,
            "tags": data["tags"],
            "example": data["example"],
            "priority": data["priority"],
            "notes": data["notes"],
            "created_at": payload.get("created_at", current) if payload else current,
            "updated_at": current,
            "polite_form": data.get("polite_form") or forms["polite"],
            "te_form": data.get("te_form") or forms["te"],
            "ta_form": data.get("ta_form") or forms["ta"],
        }
        try:
            self.db.upsert_vocab(item)
            logging.info("保存词条: %s", item["word"])
            if hasattr(self, "vocab_tree"):
                self.render_vocab_tree()
            if hasattr(self, "focus_tree"):
                self.render_text_analysis()
            self.toast("保存成功", f"{item['word']} 已保存到词汇本。", "success")
        except Exception as exc:
            logging.exception("保存词条失败")
            safe_message("error", "保存失败", f"保存单词失败：{exc}")

    def quick_add_vocab(self):
        raw = self.simple_prompt("快速添加", "请输入“单词=释义”或“单词 释义”：")
        if raw is None:
            return
        text = raw.strip()
        if "=" in text:
            word, meaning = [part.strip() for part in text.split("=", 1)]
        else:
            parts = text.split(maxsplit=1)
            word = parts[0].strip() if parts else ""
            meaning = parts[1].strip() if len(parts) > 1 else ""
        if not word or not meaning:
            safe_message("warning", "提示", "请输入完整的“单词 + 释义”。")
            return
        payload = {"word": word, "meaning": meaning, "tags": "", "example": "", "notes": "", "priority": 1, "reading": "", "base_form": "", "pos": ""}
        self.open_word_editor(payload, mode="simple")

    def edit_selected_vocab(self):
        selected = self.vocab_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择一个单词。")
            return
        word = self.vocab_tree.item(selected[0], "values")[0]
        payload = self.db.get_vocab(word)
        if not payload:
            safe_message("warning", "提示", "找不到该词条。")
            return
        self.open_word_editor(payload)

    def batch_delete_vocab(self):
        selected = self.vocab_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择至少一个单词。")
            return
        words = [self.vocab_tree.item(item, "values")[0] for item in selected]
        if not messagebox.askyesno("确认删除", f"确定删除选中的 {len(words)} 个单词吗？此操作不可撤销。"):
            return
        self.db.delete_vocab_words(words)
        logging.info("批量删除单词: %s", ",".join(words))
        self.render_vocab_tree()
        self.toast("删除完成", f"已删除 {len(words)} 个单词。", "success")

    def batch_tag_vocab(self):
        selected = self.vocab_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择至少一个单词。")
            return
        tag = self.simple_prompt("批量标签", "请输入要设置的标签（例如 N3,考试重点）：")
        if tag is None:
            return
        words = [self.vocab_tree.item(item, "values")[0] for item in selected]
        self.db.batch_update_tags(words, tag)
        logging.info("批量标签更新: %s -> %s", tag, words)
        self.render_vocab_tree()
        self.toast("标签更新完成", f"已更新 {len(words)} 个词条的标签。", "success")

    def batch_priority(self, priority):
        selected = self.vocab_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择至少一个单词。")
            return
        for item in selected:
            word = self.vocab_tree.item(item, "values")[0]
            self.db.update_priority(word, priority)
        self.render_vocab_tree()
        self.toast("优先级更新完成", "已更新选中词条的复习优先级。", "success")

    def batch_set_mastery(self, mastered):
        selected = self.vocab_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择至少一个单词。")
            return
        for item in selected:
            word = self.vocab_tree.item(item, "values")[0]
            self.db.set_word_mastery(word, mastered)
            if mastered:
                self.db.clear_mistake(word)
        self.render_vocab_tree()
        self.toast("掌握状态已更新", f"已将 {len(selected)} 个词标记为{'已掌握' if mastered else '未掌握'}。", "success")

    def simple_prompt(self, title, prompt, initial=""):
        top = PromptDialog(self, self.theme, title, prompt, initial=initial)
        self.wait_window(top)
        return top.result

    def export_vocab(self):
        path = filedialog.asksaveasfilename(title="导出词汇本", defaultextension=".csv", filetypes=[("CSV 文件", "*.csv")], initialdir=str(BASE_DIR), initialfile="词汇本导出.csv")
        if not path:
            return
        rows = self.db.list_vocab(order_by="v.created_at DESC")
        seen = set()
        unique_rows = []
        for row in rows:
            if row["word"] in seen:
                continue
            seen.add(row["word"])
            unique_rows.append(row)
        try:
            with open(path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["单词", "释义", "读音", "原形", "词性", "标签", "例句", "优先级"])
                for row in unique_rows:
                    writer.writerow([row["word"], row["meaning"], row.get("reading", ""), row.get("base_form", ""), row.get("pos", ""), row.get("tags", ""), row.get("example", ""), row.get("priority", 1)])
            logging.info("导出词汇本: %s", path)
            self.status_var.set(f"词汇本导出完成，共 {len(unique_rows)} 条。")
        except Exception as exc:
            logging.exception("导出词汇本失败")
            safe_message("error", "导出失败", f"导出失败：{exc}")

    def import_excel(self):
        if not pd:
            safe_message("warning", "导入提示", "未安装 pandas，将自动切换到 CSV 导入。")
            return self.import_csv()
        path = filedialog.askopenfilename(title="选择 Excel 文件", filetypes=[("Excel 文件", "*.xlsx;*.xls")], initialdir=str(BASE_DIR))
        if not path:
            return
        try:
            df = pd.read_excel(path)
            if df.empty:
                safe_message("warning", "导入提示", "所选 Excel 文件是空白表格，没有任何数据。")
                self.status_var.set("导入失败：Excel 文件为空")
                return
            if len(df.columns) < 1:
                safe_message("warning", "导入提示", "Excel 里没有可读取的列。")
                return
        except ValueError:
            safe_message("error", "导入失败", "Excel 导入失败：文件格式错误，请选择 .xlsx 或 .xls 文件。")
            return
        except Exception as exc:
            logging.exception("Excel 读取失败")
            safe_message("error", "导入失败", f"Excel 导入失败：{exc}")
            return

        rows_to_import = []
        seen_in_file = set()
        duplicate_existing = []
        duplicate_in_file = []
        invalid_rows = 0

        for _, row in df.iterrows():
            word = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ""
            meaning = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ""
            if not word or word.lower() == "nan":
                invalid_rows += 1
                continue
            if word in seen_in_file:
                duplicate_in_file.append(word)
                continue
            seen_in_file.add(word)
            if self.db.get_vocab(word):
                duplicate_existing.append(word)
            rows_to_import.append((word, meaning))

        if not rows_to_import and duplicate_existing:
            safe_message("info", "导入结果", "文件中的单词都已存在于词汇本，没有新增内容。")
            return

        preview_sample = "\n".join(f"{word} = {meaning}" for word, meaning in rows_to_import[:8])
        preview_message = (
            f"即将导入 {len(rows_to_import)} 行。\n"
            f"已存在词条：{len(duplicate_existing)}\n"
            f"文件内重复：{len(duplicate_in_file)}\n"
            f"空行/无效行：{invalid_rows}\n\n"
            f"前 8 行预览：\n{preview_sample or '（无）'}\n\n"
            "是否继续？"
        )
        if not messagebox.askyesno("导入预览", preview_message):
            return

        overwrite_existing = False
        if duplicate_existing:
            decision = messagebox.askyesnocancel(
                "发现重复单词",
                f"Excel 中有 {len(duplicate_existing)} 个单词已存在于词汇本。\n\n选择“是”将覆盖已有词条。\n选择“否”将跳过这些重复词。\n选择“取消”终止导入。",
                default="no",
            )
            if decision is None:
                return
            overwrite_existing = bool(decision)

        count = 0
        updated = 0
        skipped = 0
        try:
            for word, meaning in rows_to_import:
                exists = self.db.get_vocab(word)
                if exists and not overwrite_existing:
                    skipped += 1
                    continue
                detail = self.engine.get_word_detail(word)
                forms = self.engine.infer_verb_forms(word, detail["pos"])
                current = now_ts()
                payload = {
                    "word": word,
                    "meaning": meaning,
                    "reading": detail["reading"],
                    "base_form": detail["base_form"],
                    "pos": detail["pos"],
                    "tags": exists.get("tags", "") if exists else "",
                    "example": exists.get("example", "") if exists else "",
                    "priority": exists.get("priority", 1) if exists else 1,
                    "notes": exists.get("notes", "") if exists else "",
                    "created_at": exists.get("created_at", current) if exists else current,
                    "updated_at": current,
                    "polite_form": forms["polite"],
                    "te_form": forms["te"],
                    "ta_form": forms["ta"],
                }
                self.db.upsert_vocab(payload)
                if exists:
                    updated += 1
                else:
                    count += 1
            if hasattr(self, "vocab_tree"):
                self.render_vocab_tree()
            logging.info("导入 Excel 成功: 新增 %s 条，覆盖 %s 条，跳过 %s 条", count, updated, skipped)
            summary = f"新增 {count} 个，覆盖 {updated} 个，跳过 {skipped} 个。"
            if duplicate_in_file:
                summary += f"\n文件内重复已忽略 {len(duplicate_in_file)} 个。"
            if invalid_rows:
                summary += f"\n空行或无效行已忽略 {invalid_rows} 行。"
            self.toast("导入完成", summary, "success")
        except Exception as exc:
            logging.exception("Excel 导入处理失败")
            safe_message("error", "导入失败", f"导入处理失败：{exc}")

    def import_csv(self):
        path = filedialog.askopenfilename(title="选择 CSV 文件", filetypes=[("CSV 文件", "*.csv"), ("所有文件", "*.*")], initialdir=str(BASE_DIR))
        if not path:
            return
        rows = []
        seen_in_file = set()
        duplicate_existing = []
        duplicate_in_file = []
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                for line in reader:
                    if not line:
                        continue
                    word = line[0].strip() if len(line) > 0 else ""
                    meaning = line[1].strip() if len(line) > 1 else ""
                    if word and word != "单词":
                        if word in seen_in_file:
                            duplicate_in_file.append(word)
                            continue
                        seen_in_file.add(word)
                        if self.db.get_vocab(word):
                            duplicate_existing.append(word)
                        rows.append((word, meaning))
        except Exception as exc:
            safe_message("error", "导入失败", f"CSV 读取失败：{exc}")
            return
        if not rows:
            safe_message("info", "提示", "CSV 中没有可导入的数据。")
            return
        preview = "\n".join(f"{word} = {meaning}" for word, meaning in rows[:10])
        if not messagebox.askyesno("CSV 导入预览", f"将要导入 {len(rows)} 行。\n已存在词条：{len(duplicate_existing)}（默认跳过）\n文件内重复：{len(duplicate_in_file)}\n\n前 10 行：\n{preview}\n\n是否继续？"):
            return
        count = 0
        skipped = 0
        for word, meaning in rows:
            if self.db.get_vocab(word):
                skipped += 1
                continue
            detail = self.engine.get_word_detail(word)
            forms = self.engine.infer_verb_forms(detail["base_form"], detail["pos"])
            current = now_ts()
            self.db.upsert_vocab(
                {
                    "word": word,
                    "meaning": meaning,
                    "reading": detail["reading"],
                    "base_form": detail["base_form"],
                    "pos": detail["pos"],
                    "tags": "",
                    "example": "",
                    "priority": 1,
                    "notes": "",
                    "created_at": current,
                    "updated_at": current,
                    "polite_form": forms["polite"],
                    "te_form": forms["te"],
                    "ta_form": forms["ta"],
                }
            )
            count += 1
        if hasattr(self, "vocab_tree"):
            self.render_vocab_tree()
        self.toast("CSV 导入完成", f"新增 {count} 个，跳过 {skipped} 个；文件内重复忽略 {len(duplicate_in_file)} 个。", "success")

    def show_review(self):
        self.set_header("复习计划", "从固定阶段升级到个性化节奏，让系统根据你的表现做更自然的安排。")
        body = self.create_scroll_page()
        body.grid_columnconfigure(0, weight=3)
        body.grid_columnconfigure(1, weight=2)
        left = tk.Frame(body, bg=self.theme["bg"])
        right = tk.Frame(body, bg=self.theme["bg"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
        right.grid(row=0, column=1, sticky="nsew", padx=(12, 0))
        overview = self.make_card(left, "今日待复习", "这里是系统今天帮你排好的词。答错会自动进入错题追踪，答对会慢慢把错题清掉。")
        overview.pack(fill="both", expand=True)
        actions = tk.Frame(overview, bg=self.theme["card"])
        actions.pack(fill="x", padx=18, pady=(8, 10))
        self.make_action_button(actions, "开始今天的复习", self.start_review_session, "accent").pack(side="left")
        self.make_action_button(actions, "刷新队列", lambda: self.populate_review_list(self.review_list), "soft").pack(side="left", padx=8)
        self.make_action_button(actions, "延后选中到明天", self.snooze_selected_review_words, "wood").pack(side="left", padx=8)
        self.make_action_button(actions, "延后全部到明天", self.snooze_all_review_words, "wood").pack(side="left", padx=8)
        ttk.Combobox(actions, textvariable=self.review_direction_var, values=["日语 -> 中文", "中文 -> 日语"], state="readonly", width=10).pack(side="right")
        tk.Checkbutton(actions, text="题卡注音", variable=self.furigana_var, command=self.render_review_card, bg=self.theme["card"], fg=self.theme["text"], selectcolor=self.theme["card"]).pack(side="right", padx=(0, 10))
        self.review_list = ttk.Treeview(overview, columns=("word", "stage", "priority", "due"), show="headings", style="Journal.Treeview", height=18)
        self.apply_tree_sorting(self.review_list, [("word", "单词", 130), ("stage", "阶段", 100), ("priority", "优先级", 80), ("due", "应复习时间", 190)], lambda col, rev: f"review_list:{col}:{int(rev)}")
        self.review_list.pack(fill="both", expand=True, padx=18, pady=(0, 18))
        self.attach_treeview_menu(self.review_list, "generic")
        self.populate_review_list(self.review_list)
        tk.Label(overview, text="使用顺序：先看左边今天待复习的词，再点“开始今天的复习”，右边才会开始出题。复习来自词汇本；答错会进错题本，答对会慢慢清理错题。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9), wraplength=720, justify="left").pack(anchor="w", padx=18, pady=(0, 18))

        self.review_card = self.make_card(right, "复习卡片", "四选一点击作答，答后可停留回看正确答案。")
        self.review_card.pack(fill="both", expand=True)
        self.review_status = tk.Label(self.review_card, text="准备好后点击左侧“开始今天的复习”。", bg=self.theme["card"], fg=self.theme["muted"])
        self.review_status.pack(anchor="w", padx=18, pady=(8, 0))
        self.review_question = tk.Label(self.review_card, text="今日还没有开始。", bg=self.theme["card"], fg=self.theme["text"], font=("Meiryo", self.jp_font_size + 8, "bold"))
        self.review_question.pack(anchor="w", padx=18, pady=(22, 6))
        self.review_hint = tk.Label(self.review_card, text="", bg=self.theme["card"], fg=self.theme["muted"], wraplength=420, justify="left")
        self.review_hint.pack(anchor="w", padx=18)
        self.review_options = tk.Frame(self.review_card, bg=self.theme["card"])
        self.review_options.pack(fill="x", padx=18, pady=(22, 10))
        self.review_feedback = tk.Label(self.review_card, text="", bg=self.theme["card"], fg=self.theme["wood"], wraplength=420, justify="left")
        self.review_feedback.pack(anchor="w", padx=18, pady=(0, 10))
        self.review_controls = tk.Frame(self.review_card, bg=self.theme["card"])
        self.review_controls.pack(fill="x", padx=18, pady=(0, 18))
        self.review_next_button = self.make_action_button(self.review_controls, "下一题", self.advance_review, "accent")
        self.review_retry_button = self.make_action_button(self.review_controls, "重答本题", self.retry_review_answer, "soft")
        self.review_skip_button = self.make_action_button(self.review_controls, "跳过并延后 30 分钟", self.skip_current_review, "wood")
        self.review_next_button.pack(side="right")
        self.review_next_button.pack_forget()
        self.review_retry_button.pack_forget()
        self.review_skip_button.pack_forget()
        tk.Label(self.review_card, text="提示：直接选答案即可；空格可先看答案，答题后按回车或空格可继续下一题，也可以先跳过当前词。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9)).pack(anchor="w", padx=18, pady=(0, 12))

    def populate_review_list(self, tree):
        rows = self.db.get_today_review(self.review_stages, 200)
        payload = [(row["word"], self.stage_text(row), row["priority"], fmt_ts(row["due_at"])) for row in rows]
        self.sync_tree(tree, "review_list", payload)
        tree.selection_remove(*tree.selection())

    def snooze_selected_review_words(self):
        selected = self.review_list.selection()
        if not selected:
            safe_message("info", "提示", "请先选择要延后的单词。")
            return
        for item in selected:
            word = self.review_list.item(item, "values")[0]
            self.db.snooze_review_word(word, 24 * 60)
        self.populate_review_list(self.review_list)
        self.toast("延后完成", "已将选中单词延后到明天，仅影响当前待复习队列。", "success")

    def snooze_all_review_words(self):
        rows = self.db.get_today_review(self.review_stages, 9999)
        if not rows:
            safe_message("info", "提示", "今天没有可延后的复习词。")
            return
        if not messagebox.askyesno("确认延后全部", f"将今天待复习的 {len(rows)} 个词全部延后到明天，是否继续？"):
            return
        for row in rows:
            self.db.snooze_review_word(row["word"], 24 * 60)
        self.populate_review_list(self.review_list)
        self.toast("延后完成", f"已将 {len(rows)} 个复习词全部延后到明天。", "success")

    def start_review_session(self):
        review_list = self.db.get_today_review(self.review_stages, 1)
        if not review_list:
            safe_message("info", "无法复习", "当前没有需要复习的单词，请先添加单词。")
            self.status_var.set("暂无需要复习的单词")
            return
        limit = int(self.settings.get("daily_review_limit", "15"))
        self.review_session = self.db.get_today_review(self.review_stages, limit)
        self.review_session = self.get_quiz_rows(self.review_session)
        if not self.review_session:
            safe_message("warning", "无法复习", "当前可复习词条的释义太少或大量是“未收录”，暂时不能生成有效选择题。请先补全释义。")
            return
        self.review_index = 0
        self.review_correct = 0
        self.review_total = 0
        self.pending_next_callback = None
        self.render_review_card()

    def render_review_card(self):
        self.answer_lock = False
        self.pending_answer = None
        self.hide_answer_controls()
        if self.review_index >= len(self.review_session):
            accuracy = self.review_correct / self.review_total * 100 if self.review_total else 0
            self.db.add_test_record(self.review_total, self.review_correct, accuracy, "review")
            self.review_status.config(text=f"复习完成：{self.review_correct}/{self.review_total}，正确率 {accuracy:.1f}%")
            self.review_question.config(text="今日任务完成")
            self.review_hint.config(text=ENCOURAGEMENTS["finish"][self.review_total % len(ENCOURAGEMENTS["finish"])] if self.review_total else "")
            self.review_feedback.config(text="今天安排给你的复习已经完成了。\n你可以继续做错题专项练习，或者回到课文手札收新词。")
            self.clear_option_buttons(self.review_options)
            self.populate_review_list(self.review_list)
            return
        row = self.review_session[self.review_index]
        self.review_status.config(text=f"第 {self.review_index + 1} / {len(self.review_session)} 题")
        prompt, hint = self.get_quiz_prompt(row, "review")
        self.review_question.config(text=prompt)
        extra = ""
        if self.furigana_var.get() and row.get("reading"):
            extra = f"\n振假名：{self.engine.annotate_text(row['word'], True)}"
        self.review_hint.config(text=f"{hint}{extra}")
        choices = self.build_quiz_choices(row, "review")
        self.review_feedback.config(text="请选择最合适的中文意思。")
        self.review_skip_button.pack(side="left")
        self.render_option_buttons(self.review_options, choices, lambda choice, data=row: self.answer_review(choice, data))
        self.focus_set()

    def answer_review(self, choice, row):
        if self.answer_lock:
            return
        self.answer_lock = True
        self.disable_option_buttons(self.review_options)
        self.review_skip_button.pack_forget()
        result = self.evaluate_quiz_answer(choice, row, "review")
        memory_choice = "记得" if result == "exact" else ("模糊" if result == "close" else "不记得")
        self.review_memory_choice.set(memory_choice)
        is_correct = memory_choice in {"记得", "模糊"}
        self.pending_answer = {"mode": "review", "row": row, "choice": choice, "result": result, "memory_choice": memory_choice}
        if is_correct:
            memo = "这次记忆判断：模糊" if memory_choice == "模糊" else "这次记忆判断：记得"
            self.review_feedback.config(text=f"{ENCOURAGEMENTS['correct'][self.review_index % len(ENCOURAGEMENTS['correct'])]}\n{memo}\n正确答案：{row['meaning']}", fg=self.theme["good"])
        else:
            self.review_feedback.config(text=f"{ENCOURAGEMENTS['wrong'][self.review_index % len(ENCOURAGEMENTS['wrong'])]}\n这次记忆判断：不记得\n你的选择：{choice}\n正确答案：{row['meaning']}", fg=self.theme["danger"])
        self.pending_next_callback = self.advance_review
        self.show_review_answer_controls()

    def show_review_answer_controls(self):
        self.answer_lock = False
        self.review_retry_button.pack(side="right", padx=(0, 8))
        self.review_next_button.pack(side="right")
        self.review_next_button.focus_set()

    def commit_pending_review_answer(self):
        if not self.pending_answer or self.pending_answer.get("mode") != "review":
            return
        row = self.pending_answer["row"]
        memory_choice = self.pending_answer["memory_choice"]
        is_correct = memory_choice in {"记得", "模糊"}
        self.review_total += 1
        if is_correct:
            self.review_correct += 1
            self.db.apply_review_result(row["word"], True, self.review_stages)
            self.maybe_clear_mistake_after_success(row["word"])
        else:
            self.db.apply_review_result(row["word"], False, self.review_stages)
            if self.should_add_to_mistakes(row["word"]):
                self.db.mark_mistake(row["word"], row["meaning"])
        self.review_index += 1
        self.pending_answer = None

    def advance_review(self):
        self.commit_pending_review_answer()
        self.pending_next_callback = None
        self.render_review_card()

    def retry_review_answer(self):
        self.pending_answer = None
        self.render_review_card()

    def skip_current_review(self):
        if self.review_index >= len(self.review_session):
            return
        row = self.review_session[self.review_index]
        self.db.snooze_review_word(row["word"], 30)
        self.review_feedback.config(text=f"已将「{row['word']}」延后 30 分钟。", fg=self.theme["warn"])
        self.review_index += 1
        self.pending_next_callback = self.advance_review
        self.show_review_answer_controls()
        self.populate_review_list(self.review_list)

    def hide_answer_controls(self):
        self.review_next_button.pack_forget()
        if hasattr(self, "review_retry_button"):
            self.review_retry_button.pack_forget()
        self.review_skip_button.pack_forget()
        if hasattr(self, "test_next_button"):
            self.test_next_button.pack_forget()
        if hasattr(self, "test_retry_button"):
            self.test_retry_button.pack_forget()
        if hasattr(self, "practice_next_button"):
            self.practice_next_button.pack_forget()
        if hasattr(self, "practice_retry_button"):
            self.practice_retry_button.pack_forget()

    def weighted_vocab_sample(self, rows, count):
        pool = list(rows)
        picked = []
        while pool and len(picked) < count:
            weights = [max(1, int(item.get("priority", 1))) + max(0, int(item.get("wrong_count", 0))) for item in pool]
            index = random.choices(range(len(pool)), weights=weights, k=1)[0]
            picked.append(pool.pop(index))
        return picked

    def show_test(self):
        self.set_header("随机自测", "高频操作也要保持流畅和轻量，点击作答就够了。")
        body = self.create_scroll_page()
        body.grid_columnconfigure(0, weight=2)
        body.grid_columnconfigure(1, weight=3)
        left = tk.Frame(body, bg=self.theme["bg"])
        right = tk.Frame(body, bg=self.theme["bg"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
        right.grid(row=0, column=1, sticky="nsew", padx=(12, 0))
        setup = self.make_card(left, "测试设置", "支持自定义题量，错题会自动进入错题本。")
        setup.pack(fill="x")
        tk.Label(setup, text="自测是随机抽题，用来查漏补缺；答错的词会自动进入错题本，之后可以去“错题本”专项回看。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9), wraplength=320, justify="left").pack(anchor="w", padx=18, pady=(8, 0))
        row = tk.Frame(setup, bg=self.theme["card"])
        row.pack(fill="x", padx=18, pady=(10, 18))
        vocab_len = len(self.db.list_vocab(order_by="v.created_at DESC"))
        self.test_count = tk.IntVar(value=min(5, max(1, vocab_len)))
        tk.Label(row, text="题量", bg=self.theme["card"], fg=self.theme["text"]).pack(side="left")
        tk.Spinbox(row, from_=1, to=max(1, vocab_len), textvariable=self.test_count, width=6, relief="flat", bg=self.theme["panel"], fg=self.theme["text"], justify="center").pack(side="left", padx=10)
        self.make_action_button(row, "开始自测", self.start_test_session, "accent").pack(side="left", padx=(10, 0))
        ttk.Combobox(row, textvariable=self.test_direction_var, values=["日语 -> 中文", "中文 -> 日语"], state="readonly", width=10).pack(side="right")
        tk.Checkbutton(row, text="题卡注音", variable=self.furigana_var, command=self.render_test_card, bg=self.theme["card"], fg=self.theme["text"], selectcolor=self.theme["card"]).pack(side="right", padx=(0, 10))
        note = self.make_card(left, "操作效率", "支持快捷键：Ctrl+N 新增、Ctrl+E 导出、Ctrl+I 导入、Esc 关闭弹窗。")
        note.pack(fill="both", expand=True, pady=18)
        tk.Label(note, text="测试支持“日语→中文”和“中文→日语”双向练习。答错的词会自动进入错题本，答对则会逐步消化最近的错误压力。", bg=self.theme["card"], fg=self.theme["text"], wraplength=320, justify="left").pack(fill="both", expand=True, padx=18, pady=(14, 20), anchor="nw")
        self.test_card = self.make_card(right, "测试卡片", "点选作答，答后可停留回看。")
        self.test_card.pack(fill="both", expand=True)
        self.test_status = tk.Label(self.test_card, text="点击左侧按钮开始。", bg=self.theme["card"], fg=self.theme["muted"])
        self.test_status.pack(anchor="w", padx=18, pady=(8, 0))
        self.test_question = tk.Label(self.test_card, text="还没有开始测试。", bg=self.theme["card"], fg=self.theme["text"], font=("Meiryo", self.jp_font_size + 8, "bold"))
        self.test_question.pack(anchor="w", padx=18, pady=(22, 6))
        self.test_hint = tk.Label(self.test_card, text="", bg=self.theme["card"], fg=self.theme["muted"])
        self.test_hint.pack(anchor="w", padx=18)
        self.test_options = tk.Frame(self.test_card, bg=self.theme["card"])
        self.test_options.pack(fill="x", padx=18, pady=(22, 10))
        self.test_feedback = tk.Label(self.test_card, text="", bg=self.theme["card"], fg=self.theme["wood"], wraplength=420, justify="left")
        self.test_feedback.pack(anchor="w", padx=18, pady=(0, 10))
        self.test_controls = tk.Frame(self.test_card, bg=self.theme["card"])
        self.test_controls.pack(fill="x", padx=18, pady=(0, 18))
        self.test_next_button = self.make_action_button(self.test_controls, "下一题", self.advance_test, "accent")
        self.test_retry_button = self.make_action_button(self.test_controls, "重答本题", self.retry_test_answer, "soft")
        self.test_next_button.pack_forget()
        self.test_retry_button.pack_forget()
        tk.Label(self.test_card, text="提示：优先级高和易错词会更容易出现在这里；空格可先看答案，答题后按回车或空格可继续下一题。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9)).pack(anchor="w", padx=18, pady=(0, 12))

    def start_test_session(self):
        vocab_rows = self.db.list_vocab(order_by="v.priority DESC, RANDOM()")
        if not vocab_rows:
            safe_message("info", "无法自测", "单词库为空，请先导入或添加单词再进行自测。")
            self.status_var.set("暂无单词，无法自测")
            return
        count = min(max(1, int(self.test_count.get())), len(vocab_rows))
        self.test_session = self.weighted_vocab_sample(vocab_rows, count)
        self.test_session = self.get_quiz_rows(self.test_session)
        if not self.test_session:
            safe_message("warning", "无法自测", "当前词库里有效释义太少，暂时无法生成有区分度的选择题。请先补全词义。")
            return
        self.test_index = 0
        self.test_correct = 0
        self.test_total = 0
        self.pending_next_callback = None
        self.render_test_card()

    def render_test_card(self):
        self.answer_lock = False
        self.pending_answer = None
        self.test_next_button.pack_forget()
        if self.test_index >= len(self.test_session):
            accuracy = self.test_correct / self.test_total * 100 if self.test_total else 0
            self.db.add_test_record(self.test_total, self.test_correct, accuracy, "test")
            self.test_status.config(text=f"测试完成：{self.test_correct}/{self.test_total}，正确率 {accuracy:.1f}%")
            self.test_question.config(text="这一轮测试结束")
            self.test_hint.config(text=ENCOURAGEMENTS["finish"][self.test_total % len(ENCOURAGEMENTS["finish"])] if self.test_total else "")
            self.test_feedback.config(text="可以去错题本查看最近波动的词。")
            self.clear_option_buttons(self.test_options)
            return
        row = self.test_session[self.test_index]
        self.test_status.config(text=f"第 {self.test_index + 1} / {len(self.test_session)} 题")
        prompt, hint = self.get_quiz_prompt(row, "test")
        self.test_question.config(text=prompt)
        extra = ""
        if self.furigana_var.get() and row.get("reading"):
            extra = f"\n振假名：{self.engine.annotate_text(row['word'], True)}"
        self.test_hint.config(text=f"{hint}{extra}")
        choices = self.build_quiz_choices(row, "test")
        self.test_feedback.config(text="请选择最合适的中文意思。")
        self.render_option_buttons(self.test_options, choices, lambda choice, data=row: self.answer_test(choice, data))
        self.focus_set()

    def answer_test(self, choice, row):
        if self.answer_lock:
            return
        self.answer_lock = True
        self.disable_option_buttons(self.test_options)
        result = self.evaluate_quiz_answer(choice, row, "test")
        self.pending_answer = {"mode": "test", "row": row, "choice": choice, "result": result}
        if result in ("exact", "close"):
            self.test_feedback.config(text=f"回答正确。\n正确答案：{row['meaning']}", fg=self.theme["good"])
        else:
            self.test_feedback.config(text=f"这题答错了。\n你的选择：{choice}\n正确答案：{row['meaning']}", fg=self.theme["danger"])
        self.pending_next_callback = self.advance_test
        self.show_test_answer_controls()

    def show_test_answer_controls(self):
        self.answer_lock = False
        self.test_retry_button.pack(side="right", padx=(0, 8))
        self.test_next_button.pack(side="right")
        self.test_next_button.focus_set()

    def commit_pending_test_answer(self):
        if not self.pending_answer or self.pending_answer.get("mode") != "test":
            return
        row = self.pending_answer["row"]
        result = self.pending_answer["result"]
        self.test_total += 1
        if result in ("exact", "close"):
            self.test_correct += 1
            self.db.apply_review_result(row["word"], True, self.review_stages)
            self.maybe_clear_mistake_after_success(row["word"])
        else:
            self.db.apply_review_result(row["word"], False, self.review_stages)
            if self.should_add_to_mistakes(row["word"]):
                self.db.mark_mistake(row["word"], row["meaning"])
        self.test_index += 1
        self.pending_answer = None

    def advance_test(self):
        self.commit_pending_test_answer()
        self.pending_next_callback = None
        self.render_test_card()

    def retry_test_answer(self):
        self.pending_answer = None
        self.render_test_card()

    def show_mistakes(self):
        self.set_header("错题本", "把容易混淆的词重新捡回来，让系统帮你减少重复的挫败感。")
        body = self.create_scroll_page()
        card = self.make_card(body, "易错词清单", "支持批量选择、专项练习和自动联动词汇熟悉度。")
        card.pack(fill="both", expand=True)
        tk.Label(card, text="这里会自动收集你在复习和自测里答错的词；专项练习答对后，它们会逐步从这里消退。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9), wraplength=1100, justify="left").pack(anchor="w", padx=18, pady=(8, 0))
        tools = tk.Frame(card, bg=self.theme["card"])
        tools.pack(fill="x", padx=18, pady=(8, 10))
        self.make_action_button(tools, "开始专项练习", self.start_practice_session, "accent").pack(side="left")
        self.make_action_button(tools, "删除选中", self.delete_selected_mistakes, "danger").pack(side="left", padx=8)
        self.make_action_button(tools, "清空全部错题", self.clear_all_mistakes, "danger").pack(side="left", padx=8)
        ttk.Combobox(tools, textvariable=self.practice_direction_var, values=["日语 -> 中文", "中文 -> 日语"], state="readonly", width=10).pack(side="right")
        tk.Checkbutton(tools, text="题卡注音", variable=self.furigana_var, command=self.render_practice_card, bg=self.theme["card"], fg=self.theme["text"], selectcolor=self.theme["card"]).pack(side="right", padx=(0, 10))
        self.mistake_tree = ttk.Treeview(card, columns=("word", "meaning", "count", "pos", "tags", "time"), show="headings", style="Journal.Treeview", selectmode="extended", height=11)
        for key, label, width in [("word", "单词", 120), ("meaning", "释义", 260), ("count", "错误次数", 80), ("pos", "词性", 90), ("tags", "标签", 180), ("time", "最近错误时间", 180)]:
            self.mistake_tree.heading(key, text=label)
            self.mistake_tree.column(key, width=width, anchor="w", stretch=True)
        self.mistake_tree.pack(fill="both", expand=True, padx=18, pady=(0, 18))
        self.attach_treeview_menu(self.mistake_tree, "generic")
        self.render_mistake_tree()
        self.practice_card = self.make_card(body, "专项练习卡片", "让最容易失误的词在这里被重新整理好。")
        self.practice_card.pack(fill="both", expand=True)
        self.practice_status = tk.Label(self.practice_card, text="点击上方按钮开始。", bg=self.theme["card"], fg=self.theme["muted"])
        self.practice_status.pack(anchor="w", padx=18, pady=(8, 0))
        self.practice_question = tk.Label(self.practice_card, text="还没有开始。", bg=self.theme["card"], fg=self.theme["text"], font=("Meiryo", self.jp_font_size + 8, "bold"))
        self.practice_question.pack(anchor="w", padx=18, pady=(22, 6))
        self.practice_hint = tk.Label(self.practice_card, text="", bg=self.theme["card"], fg=self.theme["muted"])
        self.practice_hint.pack(anchor="w", padx=18)
        self.practice_options = tk.Frame(self.practice_card, bg=self.theme["card"])
        self.practice_options.pack(fill="x", padx=18, pady=(22, 10))
        self.practice_feedback = tk.Label(self.practice_card, text="", bg=self.theme["card"], fg=self.theme["wood"], wraplength=420, justify="left")
        self.practice_feedback.pack(anchor="w", padx=18, pady=(0, 10))
        self.practice_controls = tk.Frame(self.practice_card, bg=self.theme["card"])
        self.practice_controls.pack(fill="x", padx=18, pady=(0, 18))
        self.practice_next_button = self.make_action_button(self.practice_controls, "下一题", self.advance_practice, "accent")
        self.practice_retry_button = self.make_action_button(self.practice_controls, "重答本题", self.retry_practice_answer, "soft")
        self.practice_next_button.pack_forget()
        self.practice_retry_button.pack_forget()
        tk.Label(self.practice_card, text="提示：专项练习会优先处理高优先级和高错误次数的词；空格可先看答案，答题后按回车或空格可继续。", bg=self.theme["card"], fg=self.theme["muted"], font=("SimHei UI", 9)).pack(anchor="w", padx=18, pady=(0, 12))

    def render_mistake_tree(self):
        rows = self.db.list_mistakes()
        payload = [(row["word"], row["meaning"], row["wrong_count"], row.get("pos", ""), row.get("tags", ""), fmt_ts(row["last_wrong_at"])) for row in rows]
        self.sync_tree(self.mistake_tree, "mistakes", payload)
        self.mistake_tree.selection_remove(*self.mistake_tree.selection())

    def delete_selected_mistakes(self):
        selected = self.mistake_tree.selection()
        if not selected:
            safe_message("info", "提示", "请先选择至少一个错题。")
            return
        if not messagebox.askyesno("确认删除", f"确定清空选中的 {len(selected)} 个错题记录吗？"):
            return
        for item in selected:
            word = self.mistake_tree.item(item, "values")[0]
            self.db.clear_mistake(word)
        self.render_mistake_tree()
        self.toast("错题已清空", f"已清空 {len(selected)} 条错题记录。", "success")

    def start_practice_session(self):
        self.practice_session = sorted(self.db.list_mistakes(), key=lambda row: (-int(row.get("priority") or 1), -int(row.get("wrong_count") or 0), -float(row.get("last_wrong_at") or 0)))
        self.practice_session = self.get_quiz_rows(self.practice_session)
        total_candidates = len(self.practice_session)
        practice_limit = 50
        self.practice_session = self.practice_session[:practice_limit]
        self.practice_index = 0
        self.practice_correct = 0
        self.practice_total = 0
        self.pending_next_callback = None
        if not self.practice_session:
            self.practice_status.config(text="当前没有错题，说明最近状态不错。")
            self.practice_question.config(text="暂时不用专项练习")
            self.practice_hint.config(text="")
            self.practice_feedback.config(text="可以做一轮自测，或者继续收新词。")
            self.clear_option_buttons(self.practice_options)
            self.practice_next_button.pack_forget()
            return
        if total_candidates > practice_limit:
            self.practice_feedback.config(text=f"错题较多，本轮先练前 {practice_limit} 题，避免卡顿。剩余内容下次继续。", fg=self.theme["warn"])
        self.render_practice_card()

    def render_practice_card(self):
        self.answer_lock = False
        self.pending_answer = None
        self.practice_next_button.pack_forget()
        if self.practice_index >= len(self.practice_session):
            self.practice_status.config(text=f"专项练习完成：{self.practice_correct}/{self.practice_total}")
            self.practice_question.config(text="这轮错题回看结束")
            self.practice_hint.config(text=ENCOURAGEMENTS["finish"][self.practice_total % len(ENCOURAGEMENTS["finish"])] if self.practice_total else "")
            self.practice_feedback.config(text="错题本已经根据这轮结果自动更新。")
            self.clear_option_buttons(self.practice_options)
            self.render_mistake_tree()
            return
        row = self.practice_session[self.practice_index]
        vocab = self.db.get_vocab(row["word"])
        meaning = vocab["meaning"] if vocab else row["meaning"]
        self.practice_status.config(text=f"第 {self.practice_index + 1} / {len(self.practice_session)} 题")
        prompt, hint = self.get_quiz_prompt({"word": row["word"], "meaning": meaning, "reading": vocab.get("reading", row.get("reading", "")) if vocab else row.get("reading", ""), "pos": row.get("pos", "")}, "practice")
        self.practice_question.config(text=prompt)
        extra = ""
        if self.furigana_var.get():
            extra = f"\n振假名：{self.engine.annotate_text(row['word'], True)}"
        self.practice_hint.config(text=f"历史错误 {row['wrong_count']} 次\n{hint}{extra}")
        choices = self.build_quiz_choices({"word": row["word"], "meaning": meaning, "reading": vocab.get("reading", row.get("reading", "")) if vocab else row.get("reading", ""), "pos": row.get("pos", "")}, "practice")
        self.practice_feedback.config(text="把容易错的词慢慢重新捡回来。")
        self.render_option_buttons(self.practice_options, choices, lambda choice, word=row["word"], real_meaning=meaning: self.answer_practice(choice, word, real_meaning))
        self.focus_set()

    def answer_practice(self, choice, word, meaning):
        if self.answer_lock:
            return
        self.answer_lock = True
        self.disable_option_buttons(self.practice_options)
        result = self.evaluate_quiz_answer(choice, {"word": word, "meaning": meaning}, "practice")
        self.pending_answer = {"mode": "practice", "word": word, "meaning": meaning, "choice": choice, "result": result}
        if result in ("exact", "close"):
            self.practice_feedback.config(text=f"这一题稳住了。\n正确答案：{meaning}", fg=self.theme["good"])
        else:
            self.practice_feedback.config(text=f"再看一眼。\n你的选择：{choice}\n正确答案：{meaning}", fg=self.theme["danger"])
        self.pending_next_callback = self.advance_practice
        self.show_practice_answer_controls()

    def show_practice_answer_controls(self):
        self.answer_lock = False
        self.practice_retry_button.pack(side="right", padx=(0, 8))
        self.practice_next_button.pack(side="right")
        self.practice_next_button.focus_set()

    def commit_pending_practice_answer(self):
        if not self.pending_answer or self.pending_answer.get("mode") != "practice":
            return
        word = self.pending_answer["word"]
        meaning = self.pending_answer["meaning"]
        result = self.pending_answer["result"]
        self.practice_total += 1
        if result in ("exact", "close"):
            self.practice_correct += 1
            self.db.apply_review_result(word, True, self.review_stages)
            self.maybe_clear_mistake_after_success(word)
        else:
            self.db.apply_review_result(word, False, self.review_stages)
            if self.should_add_to_mistakes(word):
                self.db.mark_mistake(word, meaning)
        self.practice_index += 1
        self.pending_answer = None

    def advance_practice(self):
        self.commit_pending_practice_answer()
        self.pending_next_callback = None
        self.render_practice_card()

    def retry_practice_answer(self):
        self.pending_answer = None
        self.render_practice_card()

    def clear_all_mistakes(self):
        rows = self.db.list_mistakes()
        if not rows:
            safe_message("info", "提示", "当前没有可清空的错题记录。")
            return
        if not messagebox.askyesno("确认清空错题", f"将清空全部 {len(rows)} 条错题记录。是否继续？"):
            return
        for row in rows:
            self.db.clear_mistake(row["word"])
        self.render_mistake_tree()
        self.toast("错题已清空", f"已清空 {len(rows)} 条错题记录。", "success")

    def open_quick_study_window(self):
        rows = self.db.get_today_review(self.review_stages, 5)
        rows = self.get_quiz_rows(rows)
        if not rows:
            safe_message("info", "提示", "当前没有需要快速复习的词。")
            return
        top = tk.Toplevel(self)
        top.title("快速学习模式")
        top.configure(bg=self.theme["panel"])
        top.geometry("480x420")
        top.transient(self)
        state = {"rows": rows, "index": 0}
        card = tk.Frame(top, bg=self.theme["card"], padx=20, pady=20)
        card.pack(fill="both", expand=True, padx=16, pady=16)
        title = tk.Label(card, text="", bg=self.theme["card"], fg=self.theme["text"], font=("Meiryo", self.jp_font_size + 6, "bold"))
        title.pack(anchor="w")
        hint = tk.Label(card, text="", bg=self.theme["card"], fg=self.theme["muted"], justify="left")
        hint.pack(anchor="w", pady=(8, 12))
        options = tk.Frame(card, bg=self.theme["card"])
        options.pack(fill="x")
        feedback = tk.Label(card, text="", bg=self.theme["card"], fg=self.theme["wood"], wraplength=380, justify="left")
        feedback.pack(anchor="w", pady=(12, 12))

        def render():
            if state["index"] >= len(state["rows"]):
                title.config(text="这轮快速学习完成")
                hint.config(text="你可以继续正常复习或自测。")
                feedback.config(text="")
                self.clear_option_buttons(options)
                return
            row = state["rows"][state["index"]]
            title.config(text=self.display_word(row["word"]))
            hint.config(text=f"读音：{row['reading']}\n词性：{row['pos']}")
            choices = self.engine.build_choices(row["meaning"], self.get_quiz_meanings())
            self.render_option_buttons(options, choices, lambda choice, data=row: answer(choice, data))
            feedback.config(text="")

        def answer(choice, row):
            result = self.engine.answer_matches(choice, row["meaning"])
            self.db.apply_review_result(row["word"], result in ("exact", "close"), self.review_stages)
            if result in ("exact", "close"):
                feedback.config(text=f"答对了。\n正确答案：{row['meaning']}", fg=self.theme["good"])
            else:
                if self.should_add_to_mistakes(row["word"]):
                    self.db.mark_mistake(row["word"], row["meaning"])
                feedback.config(text=f"答错了。\n正确答案：{row['meaning']}", fg=self.theme["danger"])
            state["index"] += 1
            top.after(300, render)

        render()

    def show_report(self):
        self.set_header("学习报告", "不只是统计，还会结合错题和标签给出更具体的学习方向。")
        body = self.create_scroll_page()
        rows = self.db.list_vocab(order_by="v.created_at DESC")
        tests = self.db.list_test_records(200)
        mistakes = self.db.list_mistakes()
        top = tk.Frame(body, bg=self.theme["bg"])
        top.pack(fill="x")
        stats = [("词汇总数", len(rows), self.theme["wood"]), ("已掌握", sum(1 for row in rows if row.get("mastered")), self.theme["good"]), ("学习中", sum(1 for row in rows if not row.get("mastered")), self.theme["accent"]), ("错题数", len(mistakes), self.theme["warn"])]
        for idx, (title, value, color) in enumerate(stats):
            card = self.make_card(top, title)
            row_idx = 0 if idx < 2 else 1
            col_idx = idx % 2
            card.grid(row=row_idx, column=col_idx, sticky="nsew", padx=(0 if col_idx == 0 else 8, 0), pady=(0 if row_idx == 0 else 8, 0))
            top.grid_columnconfigure(col_idx, weight=1)
            tk.Label(card, text=str(value), bg=self.theme["card"], fg=color, font=("SimHei UI", 28, "bold")).pack(anchor="w", padx=18, pady=(16, 14))
        insight = self.make_card(body, "薄弱点分析", "系统会优先从词性和标签两个维度观察最近的波动。")
        insight.pack(fill="x", pady=18)
        box = tk.Frame(insight, bg=self.theme["card"])
        box.pack(fill="x", padx=18, pady=(8, 18))
        for line in self.engine.weak_point_analysis(rows, mistakes):
            tk.Label(box, text=line, bg=self.theme["card"], fg=self.theme["text"], wraplength=1180, justify="left").pack(anchor="w", pady=4)
        tk.Label(box, text="建议：把高频失误标签设为“高频复习”，每天额外抽 2-3 个词做专项回看。", bg=self.theme["card"], fg=self.theme["muted"], wraplength=1180, justify="left").pack(anchor="w", pady=(8, 0))
        plan = self.make_card(body, "学习计划", "根据当前节奏自动生成周学习方向。")
        plan.pack(fill="x")
        week = tk.Frame(plan, bg=self.theme["card"])
        week.pack(fill="x", padx=18, pady=(8, 18))
        daily_review = int(self.settings.get("daily_review_limit", "15"))
        daily_new = int(self.settings.get("daily_new_limit", "5"))
        tk.Label(week, text=f"建议周计划：本周目标复习 {daily_review * 7} 个词，新学 {daily_new * 7} 个词。", bg=self.theme["card"], fg=self.theme["text"]).pack(anchor="w")
        tk.Label(week, text=f"当前连续学习 {self.streak} 天，只要保持每天小幅推进，掌握量会继续滚动增长。", bg=self.theme["card"], fg=self.theme["muted"]).pack(anchor="w", pady=(6, 0))
        charts = tk.Frame(body, bg=self.theme["bg"])
        charts.pack(fill="both", expand=True, pady=18)
        charts.grid_columnconfigure(0, weight=1)
        charts.grid_columnconfigure(1, weight=1)
        trend = self.make_card(charts, "复习趋势图", "最近 12 次记录的正确率。")
        trend.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 8))
        self.draw_bar_chart(trend, tests[-12:], "accuracy")
        dist = self.make_card(charts, "掌握分布", "按词性统计当前掌握情况。")
        dist.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 8))
        pos_counter = Counter(row.get("pos") or "未分类" for row in rows)
        self.draw_counter_chart(dist, pos_counter)

    def draw_bar_chart(self, parent, records, metric):
        canvas = tk.Canvas(parent, height=260, bg=self.theme["card"], highlightthickness=0)
        canvas.pack(fill="both", expand=True, padx=18, pady=(8, 18))
        width = 540
        height = 250
        canvas.config(width=width, height=height)
        if not records:
            canvas.create_text(width / 2, height / 2, text="还没有足够的记录。", fill=self.theme["muted"])
            return
        left, top, right, bottom = 50, 24, width - 20, height - 36
        canvas.create_line(left, bottom, right, bottom, fill=self.theme["line"], width=2)
        canvas.create_line(left, top, left, bottom, fill=self.theme["line"], width=2)
        step = (right - left) / max(1, len(records))
        for idx, row in enumerate(records):
            value = row.get(metric, 0)
            x0 = left + idx * step + 12
            x1 = x0 + max(18, step * 0.45)
            y0 = bottom - (bottom - top) * value / 100
            canvas.create_rectangle(x0, y0, x1, bottom, fill=self.theme["accent"], outline="")
            canvas.create_text((x0 + x1) / 2, y0 - 10, text=f"{value:.0f}", fill=self.theme["text"], font=("SimHei UI", 8))

    def draw_counter_chart(self, parent, counter):
        canvas = tk.Canvas(parent, height=260, bg=self.theme["card"], highlightthickness=0)
        canvas.pack(fill="both", expand=True, padx=18, pady=(8, 18))
        width = 540
        height = 250
        canvas.config(width=width, height=height)
        if not counter:
            canvas.create_text(width / 2, height / 2, text="还没有足够的词汇数据。", fill=self.theme["muted"])
            return
        items = counter.most_common(6)
        max_value = max(count for _, count in items)
        left, top = 30, 22
        for idx, (label, value) in enumerate(items):
            y = top + idx * 34
            bar_w = 360 * value / max_value
            canvas.create_text(left, y + 10, text=label, fill=self.theme["text"], anchor="w", font=("SimHei UI", 9))
            canvas.create_rectangle(130, y, 130 + bar_w, y + 18, fill=self.theme["wood"], outline="")
            canvas.create_text(130 + bar_w + 12, y + 9, text=str(value), fill=self.theme["muted"], anchor="w", font=("SimHei UI", 9))

    def show_settings(self):
        self.set_header("设置中心", "主题、字体、复习节奏、备份恢复都可以在这里完成。")
        body = self.create_scroll_page()
        compact = (not self.sidebar_collapsed) or self.cn_font_size >= 13
        body.grid_columnconfigure(0, weight=1)
        if compact:
            body.grid_columnconfigure(1, weight=0)
        else:
            body.grid_columnconfigure(1, weight=1)
        left = tk.Frame(body, bg=self.theme["bg"])
        right = tk.Frame(body, bg=self.theme["bg"])
        if compact:
            left.grid(row=0, column=0, sticky="nsew")
            right.grid(row=1, column=0, sticky="nsew", pady=(18, 0))
        else:
            left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            right.grid(row=0, column=1, sticky="nsew", padx=(12, 0))
        theme_card = self.make_card(left, "主题与字体", "支持主题切换、中文字号和日文字号调整。")
        theme_card.pack(fill="x")
        wrap = tk.Frame(theme_card, bg=self.theme["card"])
        wrap.pack(fill="x", padx=18, pady=(8, 18))
        wrap.grid_columnconfigure(0, weight=0)
        wrap.grid_columnconfigure(1, weight=1)
        self.theme_var = tk.StringVar(value=self.theme_key)
        ttk.Combobox(wrap, textvariable=self.theme_var, values=list(THEMES.keys()), state="readonly", width=16).grid(row=0, column=0, columnspan=2, sticky="ew")
        tk.Label(wrap, text="中文字号", bg=self.theme["card"], fg=self.theme["text"]).grid(row=1, column=0, sticky="w", pady=(16, 6))
        self.cn_size_var = tk.IntVar(value=self.cn_font_size)
        tk.Spinbox(wrap, from_=9, to=18, textvariable=self.cn_size_var, width=5).grid(row=1, column=1, sticky="w")
        tk.Label(wrap, text="日文字号", bg=self.theme["card"], fg=self.theme["text"]).grid(row=2, column=0, sticky="w", pady=(12, 6))
        self.jp_size_var = tk.IntVar(value=self.jp_font_size)
        tk.Spinbox(wrap, from_=10, to=22, textvariable=self.jp_size_var, width=5).grid(row=2, column=1, sticky="w")
        tk.Checkbutton(wrap, text="页面淡入淡出", variable=self.fade_var, command=self.toggle_fade, bg=self.theme["card"], fg=self.theme["text"], selectcolor=self.theme["card"]).grid(row=3, column=0, columnspan=2, sticky="w", pady=(12, 0))
        self.make_action_button(wrap, "立即应用外观", self.apply_appearance_settings, "accent").grid(row=4, column=0, columnspan=2, pady=(18, 0), sticky="w")
        review_card = self.make_card(left, "复习设置", "支持每日上限和艾宾浩斯阶段自定义。")
        review_card.pack(fill="x", pady=18)
        row = tk.Frame(review_card, bg=self.theme["card"])
        row.pack(fill="x", padx=18, pady=(8, 18))
        row.grid_columnconfigure(0, weight=0)
        row.grid_columnconfigure(1, weight=1)
        self.daily_review_var = tk.IntVar(value=int(self.settings.get("daily_review_limit", "15")))
        self.daily_new_var = tk.IntVar(value=int(self.settings.get("daily_new_limit", "5")))
        self.stages_var = tk.StringVar(value=",".join(map(str, self.review_stages)))
        labels = [("每日复习上限", self.daily_review_var), ("每日新学上限", self.daily_new_var)]
        for idx, (label, var) in enumerate(labels):
            tk.Label(row, text=label, bg=self.theme["card"], fg=self.theme["text"]).grid(row=idx, column=0, sticky="w", pady=(0 if idx == 0 else 12, 6))
            tk.Spinbox(row, from_=1, to=50, textvariable=var, width=6).grid(row=idx, column=1, sticky="w")
        tk.Label(row, text="复习阶段（分钟，\n用逗号分隔）", bg=self.theme["card"], fg=self.theme["text"], justify="left").grid(row=2, column=0, sticky="nw", pady=(12, 6))
        tk.Entry(row, textvariable=self.stages_var).grid(row=2, column=1, sticky="ew")
        tk.Label(row, text="示例：10=10分钟，1440=1天，2880=2天，10080=7天", bg=self.theme["card"], fg=self.theme["muted"], wraplength=420, justify="left").grid(row=3, column=0, columnspan=2, sticky="w")
        self.make_action_button(row, "保存复习设置", self.save_review_settings, "accent").grid(row=4, column=0, columnspan=2, pady=(18, 0), sticky="w")
        backup_card = self.make_card(right, "数据备份与恢复", "基于 SQLite 的一键备份与恢复。")
        backup_card.pack(fill="x")
        tools = tk.Frame(backup_card, bg=self.theme["card"])
        tools.pack(fill="x", padx=18, pady=(8, 18))
        for idx, (label, command, kind) in enumerate([
            ("手动备份", self.backup_database, "accent"),
            ("一键恢复", self.restore_database, "danger"),
            ("恢复最近删除", self.restore_recent_deleted_vocab, "soft"),
        ]):
            btn = self.make_action_button(tools, label, command, kind)
            btn.grid(row=0, column=idx, sticky="ew", padx=(0 if idx == 0 else 8, 0), pady=(0, 8))
            tools.grid_columnconfigure(idx, weight=1)
        tk.Label(tools, text="恢复前会先自动备份当前数据库，避免误覆盖。", bg=self.theme["card"], fg=self.theme["muted"], wraplength=520, justify="left").grid(row=1, column=0, columnspan=3, sticky="w")
        log_card = self.make_card(right, "日志与诊断", "普通用户也可以直接查看最近日志。")
        log_card.pack(fill="x", pady=18)
        log_tools = tk.Frame(log_card, bg=self.theme["card"])
        log_tools.pack(fill="x", padx=18, pady=(8, 18))
        self.make_action_button(log_tools, "查看最近日志", self.show_recent_logs, "soft").grid(row=0, column=0, sticky="w")
        tk.Checkbutton(log_tools, text="安静模式", variable=self.quiet_mode_var, bg=self.theme["card"], fg=self.theme["text"], selectcolor=self.theme["card"]).grid(row=0, column=1, sticky="w", padx=12)
        reset_card = self.make_card(right, "重置与清理", "支持重置学习进度或清空词汇本。")
        reset_card.pack(fill="x", pady=18)
        reset_tools = tk.Frame(reset_card, bg=self.theme["card"])
        reset_tools.pack(fill="x", padx=18, pady=(8, 18))
        self.make_action_button(reset_tools, "重置学习进度", self.reset_learning_progress, "wood").grid(row=0, column=0, sticky="ew")
        self.make_action_button(reset_tools, "清空所有词汇", self.clear_all_vocab, "danger").grid(row=0, column=1, sticky="ew", padx=(8, 0))
        reset_tools.grid_columnconfigure(0, weight=1)
        reset_tools.grid_columnconfigure(1, weight=1)
        info_card = self.make_card(right, "系统说明", "日志按日期记录在 logs 目录中，默认隐藏在项目目录下。")
        info_card.pack(fill="both", expand=True)
        tk.Label(info_card, text="系统会记录新增单词、导入导出、复习记录与异常信息，方便后续排查问题。数据库采用 SQLite + WAL 模式，兼顾安全性和流畅度。", bg=self.theme["card"], fg=self.theme["text"], wraplength=520 if compact else 420, justify="left").pack(fill="both", expand=True, padx=18, pady=(10, 18), anchor="nw")

    def toggle_fade(self):
        self.fade_enabled = self.fade_var.get()
        self.db.set_setting("fade_enabled", "1" if self.fade_enabled else "0")

    def apply_appearance_settings(self):
        self.db.set_setting("theme", self.theme_var.get())
        self.db.set_setting("font_size_cn", self.cn_size_var.get())
        self.db.set_setting("font_size_jp", self.jp_size_var.get())
        self.settings = self.db.get_all_settings()
        self.theme_key = self.theme_var.get()
        self.theme = THEMES.get(self.theme_key, THEMES["mist_blue"])
        self.cn_font_size = int(self.cn_size_var.get())
        self.jp_font_size = int(self.jp_size_var.get())
        self.option_add("*Font", ("SimHei UI", self.cn_font_size))
        self.refresh_window_metrics()
        self.reload_current_theme()
        self.status_var.set("外观设置已立即应用。")

    def reload_current_theme(self):
        self.tree_cache = {}
        current_page = self.selected_nav.get()
        self.flush_textlab_autosave()
        self.configure(bg=self.theme["bg"])
        if hasattr(self, "sidebar"):
            self.sidebar.destroy()
        if hasattr(self, "content"):
            self.content.destroy()
        self.setup_styles()
        self.build_shell()
        self.clear_page()
        mapping = {
            "dashboard": self.show_dashboard,
            "textlab": self.show_textlab,
            "vocab": self.show_vocab,
            "review": self.show_review,
            "test": self.show_test,
            "mistakes": self.show_mistakes,
            "report": self.show_report,
            "settings": self.show_settings,
        }
        mapping[current_page]()
        self.refresh_nav_style()

    def save_review_settings(self):
        stages = self.parse_stages(self.stages_var.get())
        if not stages:
            safe_message("warning", "提示", "复习阶段不能为空。")
            return
        self.db.set_setting("daily_review_limit", self.daily_review_var.get())
        self.db.set_setting("daily_new_limit", self.daily_new_var.get())
        self.db.set_setting("review_stages", ",".join(map(str, stages)))
        self.settings = self.db.get_all_settings()
        self.review_stages = stages
        self.status_var.set("复习设置已保存。")

    def backup_database(self):
        path = filedialog.asksaveasfilename(title="保存备份文件", defaultextension=".db", filetypes=[("SQLite 数据库", "*.db")], initialdir=str(BACKUP_DIR), initialfile=f"kotoba_journal_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
        if not path:
            return
        try:
            result = self.db.backup_database(path)
            safe_message("info", "备份完成", f"数据库已备份到：\n{result}")
        except Exception as exc:
            logging.exception("数据库备份失败")
            safe_message("error", "备份失败", f"数据库备份失败：{exc}")

    def restore_database(self):
        path = filedialog.askopenfilename(title="选择备份文件", filetypes=[("SQLite 数据库", "*.db")], initialdir=str(BACKUP_DIR))
        if not path:
            return
        if not messagebox.askyesno("确认恢复", "恢复会覆盖当前数据库。系统会先自动备份当前数据库。是否继续？"):
            return
        try:
            current_backup = self.db.backup_database()
            self.db.restore_database(path)
            self.current_note = self.db.get_text_note()
            self.settings = self.db.get_all_settings()
            self.reload_current_theme()
            safe_message("info", "恢复完成", f"数据库恢复成功。\n当前库已先备份到：\n{current_backup}\n界面已自动刷新。")
        except Exception as exc:
            logging.exception("数据库恢复失败")
            safe_message("error", "恢复失败", f"数据库恢复失败：{exc}")

    def restore_recent_deleted_vocab(self):
        rows = self.db.list_deleted_vocab(20)
        if not rows:
            safe_message("info", "提示", "最近没有可恢复的删除词条。")
            return
        preview = "\n".join(f"{row['id']}. {row['word']}  ({fmt_ts(row['deleted_at'])})" for row in rows[:10])
        if not messagebox.askyesno("恢复最近删除", f"最近删除的前 10 项：\n{preview}\n\n是否恢复这些条目？"):
            return
        restored = self.db.restore_deleted_words([row["id"] for row in rows[:10]])
        self.reload_current_theme()
        self.toast("恢复完成", f"已恢复 {restored} 个最近删除的词条。", "success")

    def reset_learning_progress(self):
        if not messagebox.askyesno("确认重置", "将重置全部复习进度和错题记录，但不会删除词汇。是否继续？"):
            return
        self.db.reset_learning_progress()
        self.reload_current_theme()
        self.toast("重置完成", "学习进度已重置，词汇保留不变。", "success")

    def clear_all_vocab(self):
        rows = self.db.list_vocab(order_by="v.created_at DESC")
        if not rows:
            safe_message("info", "提示", "当前词汇本已为空。")
            return
        if not messagebox.askyesno("确认清空", f"将软删除全部 {len(rows)} 个词条，可在设置里恢复最近删除。是否继续？"):
            return
        self.db.delete_vocab_words([row["word"] for row in rows])
        self.reload_current_theme()
        self.toast("已清空", f"已清空 {len(rows)} 个词条，可从“恢复最近删除”找回。", "success")

    def show_recent_logs(self):
        log_files = sorted(LOG_DIR.glob("*.log"), reverse=True)
        if not log_files:
            safe_message("info", "提示", "当前还没有日志文件。")
            return
        top = tk.Toplevel(self)
        top.title("最近日志")
        top.configure(bg=self.theme["panel"])
        top.geometry("840x520")
        box = tk.Frame(top, bg=self.theme["panel"], padx=16, pady=16)
        box.pack(fill="both", expand=True)
        viewer = tk.Text(box, wrap="word", bg=self.theme["card"], fg=self.theme["text"], relief="flat")
        viewer.pack(fill="both", expand=True)
        content = log_files[0].read_text(encoding="utf-8", errors="ignore")[-12000:]
        viewer.insert("1.0", content)
        viewer.configure(state="disabled")

    def sync_tree(self, tree, cache_key, rows):
        new_map = {}
        children = tree.get_children()
        child_map = {iid: tree.item(iid, "values") for iid in children}
        desired_ids = [f"{cache_key}:{index}" for index in range(len(rows))]
        for iid in children:
            if iid not in desired_ids:
                tree.delete(iid)
        for index, values in enumerate(rows):
            iid = f"{cache_key}:{index}"
            new_map[iid] = tuple(values)
            if iid in child_map:
                if tuple(child_map[iid]) != tuple(values):
                    tree.item(iid, values=values)
            else:
                tree.insert("", "end", iid=iid, values=values)
        self.tree_cache[cache_key] = new_map

    def render_option_buttons(self, frame, options, callback):
        self.clear_option_buttons(frame)
        self.active_option_buttons = []
        for idx, option in enumerate(options):
            holder = tk.Frame(frame, bg=self.theme["card"], height=76)
            holder.grid(row=idx, column=0, sticky="ew", pady=8)
            holder.grid_propagate(False)
            btn = tk.Button(
                holder,
                text=option,
                command=lambda text=option: callback(text),
                bg=self.theme["panel"],
                fg=self.theme["text"],
                relief="flat",
                activebackground=self.theme["accent_soft"],
                activeforeground=self.theme["text"],
                anchor="w",
                justify="left",
                wraplength=460,
                padx=16,
                pady=12,
                cursor="hand2",
                bd=1,
                height=2,
                font=("SimHei UI", self.cn_font_size),
            )
            self.decorate_button(btn)
            btn.pack(fill="both", expand=True)
            self.active_option_buttons.append(btn)
        frame.grid_columnconfigure(0, weight=1)

    def disable_option_buttons(self, frame):
        for child in frame.winfo_children():
            if isinstance(child, tk.Button):
                child.configure(state="disabled")

    def clear_option_buttons(self, frame):
        self.active_option_buttons = []
        for child in frame.winfo_children():
            child.destroy()

    def on_close(self):
        try:
            with self.db.transaction() as conn:
                conn.execute("PRAGMA wal_checkpoint;")
            self.flush_textlab_autosave()
        finally:
            self.destroy()


if __name__ == "__main__":
    try:
        app = JournalApp()
        app.mainloop()
    except Exception as exc:
        logging.exception("程序启动失败")
        safe_message("error", "启动失败", f"程序启动失败：{exc}")
