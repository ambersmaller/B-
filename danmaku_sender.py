"""B站直播弹幕发送器：HTTP接口 + 队列 + 限速"""
import asyncio
import time

import aiohttp

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT

SEND_URL = "https://api.live.bilibili.com/msg/send"
ROOM_INIT_URL = "https://api.live.bilibili.com/room/v1/Room/room_init"


class DanmakuSender:
    """
    向直播间发送弹幕的发送器，内置发送队列与限速。
    弹幕经 send_nowait 入队，由发送循环按最小间隔逐条发出。

    :param room_id: 直播间ID（可为短号，启动时解析真实房间号）
    :param cookie_str: 登录cookie字符串，必须包含 SESSDATA 和 bili_jct
    :param min_interval: 两条弹幕的最小发送间隔（秒），下限1.0
    :param max_queue: 发送队列最大长度，满时丢弃新弹幕
    """

    def __init__(
        self,
        room_id: int,
        cookie_str: str,
        min_interval: float = 1.5,
        max_queue: int = 50,
    ):
        self._tmp_room_id = room_id
        self._cookie_str = cookie_str
        self._cookies = self._parse_cookie_str(cookie_str)
        missing = {"SESSDATA", "bili_jct"} - self._cookies.keys()
        if missing:
            raise ValueError(
                f"发送弹幕需要cookie中包含 {'、'.join(sorted(missing))}，"
                "请在cookie_str中补齐（bili_jct用于CSRF鉴权）"
            )
        self._csrf = self._cookies["bili_jct"]
        self._min_interval = max(1.0, min_interval)
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue)
        self._room_id: int | None = None
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None

    @staticmethod
    def _parse_cookie_str(cookie_str: str) -> dict:
        cookies = {}
        for item in cookie_str.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                cookies[key.strip()] = value.strip()
        return cookies

    async def start(self):
        """启动发送循环。解析真实房间号失败会抛异常"""
        if self._task is not None:
            return
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT, "Cookie": self._cookie_str},
            timeout=aiohttp.ClientTimeout(total=10),
        )
        try:
            self._room_id = await self._resolve_room_id()
        except Exception:
            await self._session.close()
            self._session = None
            raise
        self._task = asyncio.create_task(self._send_loop())
        logger.info(f"弹幕发送器已启动，房间 {self._room_id}，间隔 {self._min_interval}s")

    async def stop(self):
        """停止发送循环并释放资源，队列中未发送的弹幕将被丢弃"""
        if self._task:
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._task = None
        if self._session:
            await self._session.close()
            self._session = None

    def send_nowait(self, text: str):
        """弹幕入队（非阻塞），队列满则丢弃并记日志"""
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            logger.warning(f"弹幕发送队列已满，丢弃: {text[:20]}")

    async def _resolve_room_id(self) -> int:
        """将短号解析为真实房间号（发送接口要求真实房间号）"""
        async with self._session.get(
            ROOM_INIT_URL, params={"id": self._tmp_room_id}
        ) as resp:
            result = await resp.json()
        if result.get("code") != 0:
            raise RuntimeError(f"房间号解析失败: {result.get('message')}")
        return result["data"]["room_id"]

    async def _send_loop(self):
        """发送循环：逐条取队列弹幕发送，每条之间间隔最小发送间隔"""
        while True:
            text = await self._queue.get()
            try:
                await self._do_send(text)
            except Exception as e:
                logger.error(f"弹幕发送异常: {e}")
            await asyncio.sleep(self._min_interval)

    async def _do_send(self, text: str):
        """调用 msg/send 接口发送一条弹幕"""
        data = {
            "bubble": "0",
            "msg": text,
            "color": "16777215",
            "mode": "1",
            "fontsize": "25",
            "rnd": str(int(time.time())),
            "roomid": str(self._room_id),
            "csrf": self._csrf,
            "csrf_token": self._csrf,
        }
        async with self._session.post(SEND_URL, data=data) as resp:
            result = await resp.json()
        if result.get("code") == 0:
            logger.info(f"弹幕已发送: {text}")
        else:
            # 常见错误码：-101未登录 / -111 csrf校验失败 / 1003212超出长度 / 10031频率过快
            logger.error(
                f"弹幕发送失败: code={result.get('code')}, "
                f"message={result.get('message')}, 内容: {text[:30]}"
            )
