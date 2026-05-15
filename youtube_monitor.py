# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "fastapi>=0.115.0",
#     "uvicorn>=0.34.0",
#     "httpx>=0.28.0",
#     "loguru>=0.7.0",
#     "python-dotenv>=1.0.0",
# ]
# ///

"""
YouTube 博主视频发布监控服务 v1.0
==================================
基于 WebSub (PubSubHubbub) 的实时推送架构：

- Google Hub 秒级推送 → FastAPI 异步接收
- Atom XML 解析 → video_id 级别去重
- 租期自动续约（5天周期）
- 动态管理 API（增删监控频道）
- 推送协议完全对齐 InsClawer Webhook 格式
"""

import asyncio
import os
import sqlite3
import sys
import threading
import time
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger

# ======================== 配置区域 ========================

load_dotenv(Path(__file__).parent / ".env")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
APP_HOST = os.getenv("APP_HOST", "")
INTERNAL_KEY = os.getenv("INTERNAL_KEY", "")
YT_WORKER_TOKEN = os.getenv("YT_WORKER_TOKEN", "")
YT_ADMIN_KEY = os.getenv("YT_ADMIN_KEY", "")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

# 推送通路开关
ENABLE_WEBHOOK_PUSH = os.getenv("ENABLE_WEBHOOK_PUSH", "true").lower() == "true"
ENABLE_TG_PUSH = os.getenv("ENABLE_TG_PUSH", "false").lower() == "true"

# WebSub 配置
CALLBACK_DOMAIN = os.getenv("YT_CALLBACK_DOMAIN", "")  # 公网域名，如 yt.yourdomain.com
LISTEN_PORT = int(os.getenv("YT_MONITOR_PORT", "8000"))
LEASE_SECONDS = 432000  # 5 天

# Google PubSubHubbub Hub
HUB_URL = "https://pubsubhubbub.appspot.com/subscribe"
TOPIC_TEMPLATE = "https://www.youtube.com/xml/feeds/videos.xml?channel_id={}"

# SQLite
DB_PATH = Path(__file__).parent / "youtube_monitor.db"

# 初始监控目标
INITIAL_CHANNELS = [
    {"handle": "@BinanceYoutube", "name": "Binance"},
]

# ======================== 日志配置 ========================

logger.remove()
if sys.stderr.isatty():
    logger.add(
        sys.stderr,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        ),
    )
logger.add(
    "youtube_monitor.log",
    rotation="10 MB",
    retention="7 days",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
)

# ======================== SQLite 持久化层 ========================


class YouTubeDB:
    """SQLite 持久化，存储订阅信息和已知视频 ID"""

    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_tables()

    def _init_tables(self):
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    channel_id    TEXT PRIMARY KEY,
                    handle        TEXT NOT NULL,
                    name          TEXT NOT NULL,
                    topic_url     TEXT NOT NULL,
                    lease_expires REAL DEFAULT 0,
                    subscribed_at TEXT DEFAULT (datetime('now', 'localtime'))
                );
                CREATE TABLE IF NOT EXISTS known_videos (
                    video_id    TEXT PRIMARY KEY,
                    channel_id  TEXT NOT NULL,
                    title       TEXT,
                    author      TEXT,
                    link        TEXT,
                    published   TEXT,
                    updated     TEXT,
                    inserted_at TEXT DEFAULT (datetime('now', 'localtime'))
                );
                CREATE INDEX IF NOT EXISTS idx_kv_cid ON known_videos(channel_id);
            """)
            self._conn.commit()
        logger.debug(f"💾 SQLite 初始化完成: {self._db_path}")

    # ---- 订阅管理 ----

    def upsert_subscription(self, channel_id: str, handle: str, name: str):
        topic = TOPIC_TEMPLATE.format(channel_id)
        with self._lock:
            self._conn.execute(
                """INSERT INTO subscriptions (channel_id, handle, name, topic_url)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(channel_id) DO UPDATE SET
                       handle = excluded.handle,
                       name = excluded.name,
                       topic_url = excluded.topic_url""",
                (channel_id, handle, name, topic),
            )
            self._conn.commit()

    def update_subscription_name(self, channel_id: str, new_name: str) -> bool:
        with self._lock:
            cur = self._conn.execute("UPDATE subscriptions SET name = ? WHERE channel_id = ?", (new_name, channel_id))
            self._conn.commit()
            return cur.rowcount > 0

    def update_lease(self, topic_url: str, lease_seconds: int):
        expires = time.time() + lease_seconds
        with self._lock:
            self._conn.execute(
                "UPDATE subscriptions SET lease_expires = ? WHERE topic_url = ?",
                (expires, topic_url),
            )
            self._conn.commit()

    def get_all_subscriptions(self) -> list[dict]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM subscriptions")
            return [dict(row) for row in cur.fetchall()]

    def get_expiring_subscriptions(self, threshold: float = 86400) -> list[dict]:
        deadline = time.time() + threshold
        with self._lock:
            cur = self._conn.execute("SELECT * FROM subscriptions WHERE lease_expires < ?", (deadline,))
            return [dict(row) for row in cur.fetchall()]

    def delete_subscription(self, channel_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM subscriptions WHERE channel_id = ?", (channel_id,))
            self._conn.commit()
            return cur.rowcount > 0

    def get_subscription_by_topic(self, topic_url: str) -> dict | None:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM subscriptions WHERE topic_url = ?", (topic_url,))
            row = cur.fetchone()
            return dict(row) if row else None

    # ---- 视频去重 ----

    def is_video_known(self, video_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("SELECT 1 FROM known_videos WHERE video_id = ?", (video_id,))
            return cur.fetchone() is not None

    def add_known_video(self, entry: dict):
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO known_videos
                   (video_id, channel_id, title, author, link, published, updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    entry["video_id"],
                    entry["channel_id"],
                    entry.get("title", ""),
                    entry.get("author", ""),
                    entry.get("link", ""),
                    entry.get("published", ""),
                    entry.get("updated", ""),
                ),
            )
            self._conn.commit()

    def prune_old_videos(self, channel_id: str, keep: int = 100):
        with self._lock:
            self._conn.execute(
                """DELETE FROM known_videos
                   WHERE channel_id = ? AND video_id NOT IN (
                       SELECT video_id FROM known_videos
                       WHERE channel_id = ?
                       ORDER BY inserted_at DESC LIMIT ?
                   )""",
                (channel_id, channel_id, keep),
            )
            self._conn.commit()

    def close(self):
        with self._lock:
            self._conn.close()


# ======================== Atom XML 解析器 ========================

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}


def parse_atom_feed(xml_bytes: bytes) -> list[dict]:
    """解析 Google 推送的 Atom XML → 结构化视频列表"""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.error(f"❌ XML 解析失败: {e}")
        return []

    entries = []
    for entry in root.findall("atom:entry", ATOM_NS):
        link_el = entry.find("atom:link[@rel='alternate']", ATOM_NS)
        entries.append(
            {
                "video_id": entry.findtext("yt:videoId", "", ATOM_NS),
                "channel_id": entry.findtext("yt:channelId", "", ATOM_NS),
                "title": entry.findtext("atom:title", "", ATOM_NS),
                "link": link_el.get("href", "") if link_el is not None else "",
                "author": entry.findtext("atom:author/atom:name", "", ATOM_NS),
                "published": entry.findtext("atom:published", "", ATOM_NS),
                "updated": entry.findtext("atom:updated", "", ATOM_NS),
            }
        )
    return entries


# ======================== 异步推送器 ========================


class AsyncNotifier:
    """异步推送守护线程（支持 Webhook 和 Telegram 多通道）"""

    def __init__(self):
        import queue as _q

        self._queue: _q.Queue = _q.Queue()
        self._stop = threading.Event()
        self._session = httpx.Client(timeout=15.0)
        self._thread = threading.Thread(target=self._loop, name="yt-notifier", daemon=True)
        self._thread.start()
        logger.info("📤 异步推送守护线程已启动")

    def enqueue(self, payload: dict):
        self._queue.put_nowait(payload)

    def _loop(self):
        import queue as _q

        while not self._stop.is_set():
            try:
                payload = self._queue.get(timeout=1.0)
                self._dispatch(payload)
            except _q.Empty:
                continue
            except Exception as e:
                logger.exception(f"❌ 推送守护线程异常: {e}")
                time.sleep(2)

    def _dispatch(self, payload: dict):
        # 1. 聚合端 Webhook
        if ENABLE_WEBHOOK_PUSH and APP_HOST and YT_WORKER_TOKEN:
            self._push_webhook(payload)

        # 2. Telegram 直推
        if ENABLE_TG_PUSH and TG_BOT_TOKEN and TG_CHAT_ID:
            self._push_tg(payload)

    def _push_webhook(self, payload: dict):
        url = f"{APP_HOST}/internal/receive_youtube"
        try:
            resp = self._session.post(
                url,
                json=payload,
                headers={"X-Worker-Token": YT_WORKER_TOKEN, "Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                logger.success(f"✅ Webhook推送成功: [{payload.get('user', '')}] {payload.get('content', '')[:40]}")
            else:
                logger.error(f"❌ Webhook推送失败 (HTTP {resp.status_code}): {resp.text[:200]}")
        except httpx.HTTPError as e:
            logger.error(f"❌ Webhook推送连接异常: {e}")

    def _push_tg(self, payload: dict):
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

        # 构建 TG 消息体
        title = payload.get("content", "").split("\n")[0]
        text = "🚨 <b>新视频发布</b>\n\n"
        text += f"👤 <b>博主:</b> {payload.get('user', '')}\n"
        text += f"🎬 <b>标题:</b> {title}\n"
        text += f"🔗 <a href='{payload.get('link', '')}'>点击观看视频</a>\n"
        text += f"🕒 <b>时间:</b> {payload.get('time', '')}"

        data = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": False}

        try:
            resp = self._session.post(url, json=data)
            if resp.status_code == 200:
                logger.success(f"✅ TG推送成功: [{payload.get('user', '')}] {title[:40]}")
            else:
                logger.error(f"❌ TG推送失败 (HTTP {resp.status_code}): {resp.text[:200]}")
        except httpx.HTTPError as e:
            logger.error(f"❌ TG推送连接异常: {e}")

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)
        self._session.close()


# ======================== 订阅管理器 ========================


class SubscriptionManager:
    """管理 YouTube 频道的 WebSub 订阅生命周期"""

    def __init__(self, db: YouTubeDB, callback_url: str):
        self.db = db
        self.callback_url = callback_url
        self._client = httpx.AsyncClient(timeout=30.0)

    async def resolve_channel_id(self, handle: str) -> str | None:
        """@handle → UC... Channel ID（消耗 1 单位 API 配额）"""
        clean = handle.lstrip("@")
        params = {"part": "id", "forHandle": clean, "key": YOUTUBE_API_KEY}
        try:
            resp = await self._client.get("https://www.googleapis.com/youtube/v3/channels", params=params)
            if resp.status_code != 200:
                logger.error(f"❌ YouTube API 错误 {resp.status_code}: {resp.text[:200]}")
                return None
            items = resp.json().get("items", [])
            if not items:
                logger.warning(f"⚠️ 未找到频道: {handle}")
                return None
            cid = items[0]["id"]
            logger.info(f"🔍 频道解析: {handle} → {cid}")
            return cid
        except httpx.HTTPError as e:
            logger.error(f"❌ 频道解析网络异常: {e}")
            return None

    async def subscribe(self, channel_id: str) -> bool:
        """向 Google Hub 发起 WebSub 订阅"""
        topic = TOPIC_TEMPLATE.format(channel_id)
        data = {
            "hub.callback": self.callback_url,
            "hub.topic": topic,
            "hub.verify": "async",
            "hub.mode": "subscribe",
            "hub.lease_seconds": str(LEASE_SECONDS),
        }
        try:
            resp = await self._client.post(HUB_URL, data=data)
            if resp.status_code == 202:
                logger.success(f"✅ 订阅请求已受理: {channel_id}")
                return True
            logger.error(f"❌ 订阅失败 HTTP {resp.status_code}: {resp.text[:200]}")
            return False
        except httpx.HTTPError as e:
            logger.error(f"❌ 订阅网络异常: {e}")
            return False

    async def unsubscribe(self, channel_id: str) -> bool:
        """向 Google Hub 取消订阅"""
        topic = TOPIC_TEMPLATE.format(channel_id)
        data = {
            "hub.callback": self.callback_url,
            "hub.topic": topic,
            "hub.verify": "async",
            "hub.mode": "unsubscribe",
        }
        try:
            resp = await self._client.post(HUB_URL, data=data)
            return resp.status_code == 202
        except httpx.HTTPError as e:
            logger.error(f"❌ 取消订阅异常: {e}")
            return False

    async def fetch_video_details(self, video_id: str) -> dict | None:
        """Fetch video description and high-res thumbnail"""
        params = {"part": "snippet", "id": video_id, "key": YOUTUBE_API_KEY}
        try:
            resp = await self._client.get("https://www.googleapis.com/youtube/v3/videos", params=params)
            if resp.status_code == 200:
                items = resp.json().get("items", [])
                if items:
                    return items[0]["snippet"]
        except Exception as e:
            logger.error(f"❌ 获取视频详情失败: {e}")
        return None

    async def fetch_channel_avatar(self, channel_id: str) -> str | None:
        """Fetch channel avatar"""
        params = {"part": "snippet", "id": channel_id, "key": YOUTUBE_API_KEY}
        try:
            resp = await self._client.get("https://www.googleapis.com/youtube/v3/channels", params=params)
            if resp.status_code == 200:
                items = resp.json().get("items", [])
                if items:
                    # YouTube returns different sizes, high is 800x800, default is 88x88. high or medium is better.
                    thumbnails = items[0]["snippet"]["thumbnails"]
                    for res in ["high", "medium", "default"]:
                        if res in thumbnails:
                            return thumbnails[res]["url"]
        except Exception as e:
            logger.error(f"❌ 获取频道详情失败: {e}")
        return None

    async def close(self):
        await self._client.aclose()


# ======================== 全局实例 ========================

db = YouTubeDB(DB_PATH)
notifier: AsyncNotifier | None = None
sub_manager: SubscriptionManager | None = None


def build_payload(entry: dict, sub: dict | None) -> dict:
    """构建 InsClawer 兼容的推送 payload"""
    video_id = entry["video_id"]
    title = entry.get("title", "")
    link = entry.get("link", f"https://www.youtube.com/watch?v={video_id}")
    author = entry.get("author", "")
    published = entry.get("published", "")
    updated = entry.get("updated", "")

    # 解析发布时间
    pub_ts = 0.0
    if published:
        try:
            from datetime import datetime as _dt

            dt = _dt.fromisoformat(published.replace("Z", "+00:00"))
            pub_ts = dt.timestamp()
        except (ValueError, OSError):
            pub_ts = time.time()
    else:
        pub_ts = time.time()

    pub_str = datetime.fromtimestamp(pub_ts).strftime("%Y-%m-%d %H:%M:%S") if pub_ts else ""
    now = datetime.now()
    now_iso = now.astimezone().isoformat()

    handle = sub["handle"] if sub else ""
    name = sub["name"] if sub else author
    display_name = f"{name} ({handle})" if handle else name

    # 封面图：API提供或使用fallback
    thumbnail = entry.get("cover_url") or f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"

    # 头像
    avatar_url = entry.get("avatar_url", "")

    # 组合内容
    description = entry.get("description", "")
    content = title
    if description:
        # 限制描述长度，防止太长
        if len(description) > 300:
            description = description[:300] + "..."
        content = f"{title}\n\n{description}"

    return {
        # InsClawer 标准字段
        "username": handle,
        "note": name,
        "shortcode": video_id,
        "content": content,
        "cover_url": thumbnail,
        "avatar_url": avatar_url,
        "taken_at": pub_ts,
        "post_url": link,
        "detected_at": now_iso,
        # 兼容字段
        "time": pub_str,
        "timestamp": pub_ts,
        "sys_time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "avatar": avatar_url,
        "cover": thumbnail,
        "link": link,
        "user": display_name,
        "display_name": display_name,
        # 来源标识
        "type": "youtube",
        # YouTube 专有扩展
        "youtube_extra": {
            "channel_id": entry.get("channel_id", ""),
            "author": author,
            "published": published,
            "updated": updated,
        },
    }


_avatar_cache: dict[str, str] = {}


async def process_atom_feed(xml_bytes: bytes):
    """解析推送的 Atom XML → 区分事件类型 → 新视频推送 / 其他事件仅记日志"""

    # 🔍 调试：记录原始 XML 前 500 字符
    raw_preview = xml_bytes.decode("utf-8", errors="replace")[:500]
    logger.debug(f"📄 原始 XML:\n{raw_preview}")

    entries = parse_atom_feed(xml_bytes)

    if not entries:
        # 空 Feed = 视频被删除/私有化/取消发布
        # 尝试从 XML 中提取频道信息以便日志更有意义
        try:
            root = ET.fromstring(xml_bytes)
            channel_id = root.findtext("{http://www.youtube.com/xml/schemas/2015}channelId", "未知")
            channel_name = root.findtext("{http://www.w3.org/2005/Atom}title", "未知")
        except ET.ParseError:
            channel_id, channel_name = "未知", "未知"
        logger.info(f"🗑️ [事件] 视频删除/私有化 | 频道: {channel_name} ({channel_id})")
        return

    for entry in entries:
        vid = entry["video_id"]
        cid = entry["channel_id"]
        title = entry.get("title", "")
        updated = entry.get("updated", "")

        if not vid:
            continue

        # 查找订阅信息
        topic = TOPIC_TEMPLATE.format(cid)
        sub = db.get_subscription_by_topic(topic)
        display = sub["name"] if sub else cid

        if db.is_video_known(vid):
            # 已知视频 → 标题/描述更新事件，仅记日志不推送
            logger.info(f"📝 [事件] 视频元数据更新 | [{display}] {title[:60]} (updated: {updated})")
            continue

        # 🔥【核心修复】过滤老视频
        # WebSub 在首次订阅或者 Hub 重新同步时，可能推送频道的最新 15 个视频列表
        # 如果数据库没有记录，就会被当成全新视频全部推出去
        is_too_old = False
        published = entry.get("published", "")
        if published:
            try:
                from datetime import datetime as _dt

                pub_dt = _dt.fromisoformat(published.replace("Z", "+00:00"))
                pub_ts = pub_dt.timestamp()
                # 如果发布时间超过 5 分钟（300秒），判定为老视频
                if time.time() - pub_ts > 300:
                    is_too_old = True
            except Exception:
                pass

        if is_too_old:
            logger.info(f"🕰️ [忽略老视频] [{display}] {title[:40]}... (published: {published})")
            db.add_known_video(entry)  # 记录到库中防止下次再判，但不推送
            continue

        # 尝试通过 YouTube API 丰富元数据 (高清封面/描述/头像)
        if sub_manager:
            details = await sub_manager.fetch_video_details(vid)
            if details:
                entry["title"] = details.get("title", title)
                entry["description"] = details.get("description", "")

                thumbnails = details.get("thumbnails", {})
                for res in ["maxres", "standard", "high", "medium", "default"]:
                    if res in thumbnails:
                        entry["cover_url"] = thumbnails[res]["url"]
                        break

            # 获取头像
            if cid not in _avatar_cache:
                avatar_url = await sub_manager.fetch_channel_avatar(cid)
                if avatar_url:
                    _avatar_cache[cid] = avatar_url
            if cid in _avatar_cache:
                entry["avatar_url"] = _avatar_cache[cid]

        # ✅ 全新视频 → 入库 + 推送到聚合端
        logger.success(f"🎬 [新视频] [{display}] {title}")

        # 入库
        db.add_known_video(entry)

        # 推送到聚合端
        if notifier:
            payload = build_payload(entry, sub)
            notifier.enqueue(payload)

        # 清理旧记录
        db.prune_old_videos(cid, keep=100)


# ======================== 后台任务 ========================


async def init_subscriptions():
    """
    启动时初始化所有频道的 WebSub 订阅。

    优化策略：channel_id 是 YouTube 的永久标识（终身不变），
    已存入 SQLite 的频道在重启时直接从库中读取，实现
    零 API 消耗。仅当数据库中不存在时才调用一次 YouTube Data API。
    """
    if not sub_manager:
        logger.warning("⚠️ SubscriptionManager 未就绪，跳过订阅初始化")
        return

    for ch in INITIAL_CHANNELS:
        handle = ch["handle"]
        name = ch["name"]

        # 优先从数据库读取已有的 channel_id
        existing = db.get_subscription_by_handle(handle)

        if existing and existing.get("channel_id"):
            channel_id = existing["channel_id"]
            logger.debug(f"📦 [缓存命中] {name} ({handle}) → {channel_id}")
        else:
            # 数据库无记录，调用 YouTube API 解析
            channel_id = await sub_manager.resolve_channel_id(handle)
            if not channel_id:
                logger.error(f"❌ 无法解析频道: {handle}，跳过")
                continue

        db.upsert_subscription(channel_id, handle, name)
        await sub_manager.subscribe(channel_id)

    total = len(db.get_all_subscriptions())
    logger.info(f"📋 当前共 {total} 个订阅频道")


async def lease_renewal_loop():
    """定期检查并续订即将过期的 WebSub 订阅"""
    if not sub_manager:
        return
    while True:
        await asyncio.sleep(3600)  # 每小时检查一次
        try:
            subs = db.get_all_subscriptions()
            now = time.time()
            for s in subs:
                expiry = s.get("lease_expiry", 0) or 0
                if isinstance(expiry, str):
                    try:
                        from datetime import datetime as _dt

                        expiry = _dt.fromisoformat(expiry).timestamp()
                    except (ValueError, OSError):
                        expiry = 0
                remaining = expiry - now
                if remaining < 86400:  # 不到 1 天就续约
                    logger.info(f"🔄 续订: {s['name']} ({s['channel_id']})")
                    await sub_manager.subscribe(s["channel_id"])
        except Exception as e:
            logger.error(f"❌ 续订循环异常: {e}")


async def db_cleanup_loop():
    """定时清理数据库中的过旧记录，防止 SQLite 无限膨胀"""
    while True:
        await asyncio.sleep(86400)  # 每天执行一次清理
        try:
            with db._lock:
                # 物理删除 7 天前的已知视频记录（保持表轻量）
                cur = db._conn.execute("DELETE FROM known_videos WHERE inserted_at < datetime('now', '-7 days')")
                db._conn.commit()
                deleted = cur.rowcount
            if deleted > 0:
                logger.info(f"🧹 数据库清理完成: 已删除 {deleted} 条 7 天前的历史视频记录")
        except Exception as e:
            logger.error(f"❌ 数据库清理异常: {e}")


# ======================== API (管理接口) ========================

auth_scheme = HTTPBearer()


def verify_token(credentials: HTTPAuthorizationCredentials = Security(auth_scheme)):
    if credentials.credentials != YT_ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid Token")
    return credentials.credentials


# ======================== FastAPI 应用 ========================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global notifier, sub_manager

    # 环境检查
    missing = [
        k for k, v in {"YOUTUBE_API_KEY": YOUTUBE_API_KEY, "YT_CALLBACK_DOMAIN": CALLBACK_DOMAIN}.items() if not v
    ]
    if missing:
        logger.error(f"❌ 缺少必要环境变量: {', '.join(missing)}")
        raise RuntimeError("Missing config")

    callback_url = f"https://{CALLBACK_DOMAIN}/websub/callback"
    logger.info(f"🌐 WebSub 回调地址: {callback_url}")

    # 初始化组件
    is_webhook_ready = ENABLE_WEBHOOK_PUSH and APP_HOST and YT_WORKER_TOKEN
    is_tg_ready = ENABLE_TG_PUSH and TG_BOT_TOKEN and TG_CHAT_ID

    if is_webhook_ready or is_tg_ready:
        notifier = AsyncNotifier()
    else:
        logger.warning("⚠️ 没有配置任何有效的推送通道（Webhook / TG），推送功能将禁用")

    sub_manager = SubscriptionManager(db, callback_url)

    # 启动后台任务
    asyncio.create_task(init_subscriptions())
    asyncio.create_task(lease_renewal_loop())
    asyncio.create_task(db_cleanup_loop())

    logger.success("🚀 YouTube Monitor v1.0 启动完成")

    yield

    # 清理
    if notifier:
        notifier.stop()
    if sub_manager:
        await sub_manager.close()
    db.close()
    logger.info("👋 YouTube Monitor 已关闭")


app = FastAPI(title="YouTube Monitor", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    try:
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000

        # 拦截扫描器的垃圾请求，如果是 404 且不是我们预期的路径，则降级为 DEBUG 或直接忽略
        path = request.url.path
        if response.status_code == 404 and not path.startswith(("/websub", "/api")):
            logger.debug(f"🛡️ [拦截扫描] {request.method} {path} | 404")
        else:
            logger.info(f"🌐 [API] {request.method} {path} | {response.status_code} | {process_time:.1f}ms")

        return response
    except Exception as e:
        logger.error(f"🌐 [API] {request.method} {request.url.path} | 500 | 异常: {e}")
        raise


# ---- WebSub 回调路由 ----


@app.get("/websub/callback")
async def websub_verify(
    request: Request,
):
    """Google Hub 订阅验证（GET）"""
    params = request.query_params
    mode = params.get("hub.mode", "")
    topic = params.get("hub.topic", "")
    challenge = params.get("hub.challenge", "")
    lease = int(params.get("hub.lease_seconds", "0") or "0")

    if mode == "subscribe" and challenge:
        logger.info(f"✅ 挑战验证通过: topic={topic}, lease={lease}s")
        if lease > 0:
            db.update_lease(topic, lease)
        return PlainTextResponse(challenge)

    if mode == "unsubscribe" and challenge:
        logger.info(f"🗑️ 取消订阅验证: topic={topic}")
        return PlainTextResponse(challenge)

    logger.warning(f"⚠️ 未知的验证请求: mode={mode}")
    return PlainTextResponse("rejected", status_code=404)


@app.post("/websub/callback")
async def websub_push(request: Request, background_tasks: BackgroundTasks):
    """接收 Google Hub 推送的 Atom XML（POST）→ 立即 204 → 后台处理"""
    body = await request.body()
    logger.info(f"📩 收到 WebSub 推送 ({len(body)} bytes)")
    background_tasks.add_task(process_atom_feed, body)
    return Response(status_code=204)


# ---- 管理 API ----


@app.get("/api/channels")
async def list_channels(token: str = Depends(verify_token)):
    """列出所有监控频道"""
    subs = db.get_all_subscriptions()
    return JSONResponse(
        {
            "count": len(subs),
            "channels": [
                {
                    "channel_id": s["channel_id"],
                    "handle": s["handle"],
                    "name": s["name"],
                    "lease_expires": s["lease_expires"],
                    "lease_remaining_hours": max(0, (s["lease_expires"] - time.time()) / 3600),
                    "subscribed_at": s["subscribed_at"],
                }
                for s in subs
            ],
        }
    )


@app.post("/api/channels")
async def add_channel(request: Request, token: str = Depends(verify_token)):
    """添加监控频道。Body: {"handle": "@xxx", "name": "显示名"}"""
    body = await request.json()
    handle = body.get("handle", "").strip()
    name = body.get("name", "").strip()

    if not handle:
        return JSONResponse({"error": "handle 不能为空"}, status_code=400)
    if not handle.startswith("@"):
        handle = f"@{handle}"
    if not name:
        name = handle

    if not sub_manager:
        return JSONResponse({"error": "订阅管理器未初始化"}, status_code=500)

    # 解析 Channel ID
    cid = await sub_manager.resolve_channel_id(handle)
    if not cid:
        return JSONResponse({"error": f"无法解析频道: {handle}"}, status_code=404)

    # 入库 + 订阅
    db.upsert_subscription(cid, handle, name)
    ok = await sub_manager.subscribe(cid)

    return JSONResponse(
        {
            "success": ok,
            "channel_id": cid,
            "handle": handle,
            "name": name,
            "message": "订阅请求已发送，等待 Google Hub 挑战验证" if ok else "订阅请求发送失败",
        }
    )


@app.put("/api/channels/{channel_id}")
async def update_channel(channel_id: str, request: Request, token: str = Depends(verify_token)):
    """修改频道别名。Body: {"name": "新显示名"}"""
    body = await request.json()
    new_name = body.get("name", "").strip()

    if not new_name:
        return JSONResponse({"error": "名称不能为空"}, status_code=400)

    updated = db.update_subscription_name(channel_id, new_name)
    if updated:
        return JSONResponse({"success": True, "message": "频道别名已更新"})
    return JSONResponse({"error": "频道不存在"}, status_code=404)


@app.delete("/api/channels/{channel_id}")
async def remove_channel(channel_id: str, token: str = Depends(verify_token)):
    """移除监控频道"""
    if sub_manager:
        await sub_manager.unsubscribe(channel_id)

    deleted = db.delete_subscription(channel_id)
    if deleted:
        return JSONResponse({"success": True, "message": f"已移除: {channel_id}"})
    return JSONResponse({"error": "频道不存在"}, status_code=404)


# ---- 健康检查 ----


@app.get("/health")
async def health():
    subs = db.get_all_subscriptions()
    return JSONResponse(
        {
            "status": "ok",
            "service": "youtube-monitor",
            "version": "1.0.0",
            "subscriptions": len(subs),
            "uptime_check": datetime.now().isoformat(),
        }
    )


# ======================== 入口 ========================


if __name__ == "__main__":
    logger.info(f"🎬 YouTube Monitor 启动 | 端口: {LISTEN_PORT}")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=LISTEN_PORT,
        log_level="warning",
    )
