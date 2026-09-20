"""B站扫码登录：生成二维码 → 经AstrBot推送 → 轮询扫码状态 → 提取全套登录凭证

实现参考 bilibili-API-collect 的 扫码登录 文档：
申请二维码(qrcode_key) → 渲染二维码图片 → 轮询扫码状态
（未扫码/已扫码待确认/二维码失效/成功）→ 成功后从响应Cookie中提取
SESSDATA/bili_jct，从响应体中提取 refresh_token
"""
import asyncio
import base64
import io
import time

import aiohttp
import segno
from yarl import URL

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT

QRCODE_GENERATE_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
QRCODE_POLL_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
FINGER_SPI_URL = "https://api.bilibili.com/x/frontend/finger/spi"

# 扫码状态轮询返回的二维码状态码（bilibili-API-collect 扫码登录文档）
POLL_NOT_SCANNED = 86101  # 未扫码
POLL_SCANNED_NOT_CONFIRMED = 86090  # 已扫码，待手机端确认
POLL_EXPIRED = 86038  # 二维码已失效（约180秒过期）
# 同一轮登录中二维码过期后自动换新重发的最大次数
MAX_REGENERATE = 3


class QRLoginError(Exception):
    """扫码登录失败（超时、二维码反复失效等）"""


def render_qr_base64(content: str) -> str:
    """把扫码内容渲染为PNG二维码图片，返回base64编码"""
    buf = io.BytesIO()
    segno.make(content, error="m").save(buf, kind="png", scale=6, border=2)
    return base64.b64encode(buf.getvalue()).decode()


async def _prepare_device_id(session, jar):
    """预取 buvid3/buvid4 设备标识（模拟浏览器首次访问），登录成功后随凭证一并返回"""
    try:
        async with session.get(FINGER_SPI_URL) as resp:
            result = await resp.json()
        data = result.get("data") or {}
        cookies = {}
        if data.get("b_3"):
            cookies["buvid3"] = data["b_3"]
        if data.get("b_4"):
            cookies["buvid4"] = data["b_4"]
        if cookies:
            jar.update_cookies(cookies, resp.url)
    except Exception as e:
        logger.warning(f"扫码登录获取buvid3失败（不影响登录，可稍后补充）: {e}")


async def _generate_qrcode(session) -> tuple[str, str]:
    """申请登录二维码，返回 (扫码内容, qrcode_key)"""
    async with session.get(QRCODE_GENERATE_URL) as resp:
        result = await resp.json()
    if result.get("code") != 0 or not result.get("data"):
        raise QRLoginError(
            f"申请二维码失败: code={result.get('code')}, message={result.get('message')}"
        )
    data = result["data"]
    if not data.get("url") or not data.get("qrcode_key"):
        raise QRLoginError("申请二维码失败: 响应缺少url或qrcode_key")
    return data["url"], data["qrcode_key"]


async def qrcode_login(deliver, timeout: float = 600.0, poll_interval: float = 3.0) -> dict:
    """执行扫码登录全流程，成功返回凭证字典。

    :param deliver: 异步回调 deliver(image_base64, message)，用于推送二维码图片与状态提示；
                    image_base64 为 None 时表示仅发送文字提示
    :param timeout: 整体超时时间（秒），超时未完成登录则抛 QRLoginError
    :param poll_interval: 扫码状态轮询间隔（秒）
    :return: 凭证字典，含 SESSDATA / bili_jct / buvid3 / buvid4 / refresh_token
    """
    deadline = time.monotonic() + timeout
    jar = aiohttp.CookieJar()
    headers = {"User-Agent": USER_AGENT, "Referer": "https://www.bilibili.com/"}
    async with aiohttp.ClientSession(
        cookie_jar=jar, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
    ) as session:
        await _prepare_device_id(session, jar)
        url, qrcode_key = await _generate_qrcode(session)
        await deliver(
            render_qr_base64(url),
            "请使用B站手机客户端扫描下方二维码完成登录"
            "（二维码约180秒过期，过期后会自动更换重发）。"
            f"\n若图片无法显示，可尝试在手机浏览器打开：{url}",
        )
        notified_scanned = False
        regenerated = 0
        while True:
            if time.monotonic() > deadline:
                raise QRLoginError(f"等待扫码超时（{int(timeout)}秒），本次登录已取消")
            await asyncio.sleep(poll_interval)
            async with session.get(
                QRCODE_POLL_URL, params={"qrcode_key": qrcode_key}
            ) as resp:
                result = await resp.json()
            # 扫码状态在 data.code（外层code恒为0）
            data = result.get("data") or {}
            code = data.get("code", result.get("code"))
            if code == 0:
                # 登录成功：响应Set-Cookie已写入会话（SESSDATA/bili_jct等）
                if not any(morsel.key == "SESSDATA" for morsel in jar) and data.get("url"):
                    # 个别情况下需访问一次交换链接才能拿到Cookie
                    async with session.get(data["url"]):
                        pass
                cookies = {morsel.key: morsel.value for morsel in jar}
                refresh_token = data.get("refresh_token") or ""
                if not cookies.get("SESSDATA") or not cookies.get("bili_jct") or not refresh_token:
                    raise QRLoginError("扫码登录成功但响应缺少SESSDATA/bili_jct/refresh_token")
                return {
                    "SESSDATA": cookies["SESSDATA"],
                    "bili_jct": cookies["bili_jct"],
                    "buvid3": cookies.get("buvid3", ""),
                    "buvid4": cookies.get("buvid4", ""),
                    "refresh_token": refresh_token,
                }
            if code == POLL_NOT_SCANNED:
                continue
            if code == POLL_SCANNED_NOT_CONFIRMED:
                if not notified_scanned:
                    notified_scanned = True
                    await deliver(None, "已扫码，请在手机上确认登录")
                continue
            if code == POLL_EXPIRED:
                if time.monotonic() > deadline or regenerated >= MAX_REGENERATE:
                    raise QRLoginError("二维码已失效，且自动更换次数已用尽")
                regenerated += 1
                url, qrcode_key = await _generate_qrcode(session)
                notified_scanned = False
                await deliver(
                    render_qr_base64(url),
                    f"二维码已过期，已自动更换（第{regenerated}次），请扫描下方新二维码：\n{url}",
                )
                continue
            raise QRLoginError(
                f"查询扫码状态失败: code={code}, message={data.get('message')}"
            )
