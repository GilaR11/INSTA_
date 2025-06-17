import asyncio
import json
from pathlib import Path
from typing import Dict, Optional, Any
from instagrapi import Client
from datetime import datetime
import random

from db import update_account_status

class IGWorker:
    def __init__(self, account: Dict[str, Any]):
        self.account = account
        self.session_path = Path(__file__).parent / "sessions" / f"{account['username']}.json"
        self.session_path.parent.mkdir(exist_ok=True)
        self.client = Client()
        if account.get("proxy"):
            self.client.set_proxy(account["proxy"])
        self.task: Optional[asyncio.Task] = None

    async def run(self):
        try:
            # Пытаемся загрузить существующую сессию
            if self.session_path.exists():
                try:
                    self.client.load_settings(self.session_path)
                    # Проверяем валидность сессии
                    try:
                        self.client.get_timeline_feed()
                    except Exception:
                        # Если сессия невалидна, создаем новую
                        self.client = Client()
                        if self.account.get("proxy"):
                            self.client.set_proxy(self.account["proxy"])
                        self.client.login(self.account["username"], self.account["password"])
                except Exception:
                    # Если не удалось загрузить сессию, создаем новую
                    self.client = Client()
                    if self.account.get("proxy"):
                        self.client.set_proxy(self.account["proxy"])
                    self.client.login(self.account["username"], self.account["password"])
            else:
                # Если сессии нет, создаем новую
                self.client.login(self.account["username"], self.account["password"])
            
            # Сохраняем сессию
            self.client.dump_settings(self.session_path)
            
            await update_account_status(
                self.account["id"],
                "logged_in",
                datetime.utcnow().isoformat(timespec="seconds")
            )
            
            while True:
                await self._simulate_activity()
                await asyncio.sleep(random.randint(60, 120))
        except Exception as e:
            await update_account_status(
                self.account["id"],
                f"error:{str(e)}",
                datetime.utcnow().isoformat(timespec="seconds")
            )

    async def _simulate_activity(self):
        try:
            # Получаем ленту
            feed = self.client.get_timeline_feed()
            if not feed:
                return
                
            # Выбираем случайный пост
            post = random.choice(feed[:5])
            
            # Лайкаем пост
            self.client.media_like(post.id)
            
            # Случайная задержка
            await asyncio.sleep(random.randint(2, 5))
            
            # Подписываемся на автора
            self.client.user_follow(post.user.pk)
            
        except Exception as e:
            print(f"Error in _simulate_activity: {e}")
            raise
