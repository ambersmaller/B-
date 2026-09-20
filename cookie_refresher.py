"""B站Web端Cookie自动刷新：周期检测刷新时机并完成官方刷新流程

实现参考 bilibili-API-collect 的 Web端Cookie刷新 文档：
检查是否需要刷新 → RSA-OAEP生成CorrespondPath → 换取refresh_csrf →
刷新Cookie（下发新SESSDATA/bili_jct等） → 确认更新（作废旧Cookie）
"""
import asyncio
import re
import time
from http.cookies import SimpleCookie

import aiohttp

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT

try:
    from Crypto.Cipher import PKCS1_OAEP
    from Crypto.Hash import SHA256
    from Crypto.PublicKey import RSA

    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False

# B站Web端Cookie刷新用RSA公钥（逆向自官方首页wasm，见 bilibili-API-collect#524）
REFRESH_RSA_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDLgd2OAkcGVtoE3ThUREbio0Eg
Uc/prcajMKXvkCKFCWhJYJcLkcM2DKKcSeFpD/j6Boy538YXnR6VhcuUJOhH2x71
nzPjfdTcqMz7djHum0qSZA0AyCBDABUqCrfNgCiJ00Ra7GmRj+YCK1NJEuewlb40
JNrRuoEUXpabUzGB8QIDAQAB
-----END PUBLIC KEY-----"""

COOKIE_INFO_URL = "https://passport.bilibili.com/x/passport-login/web/cookie/info"
CORRESPOND_URL = "https://www.bilibili.com/correspond/1/"
COOKIE_REFRESH_URL = "https://passport.bilibili.com/x/passport-login/web/cookie/refresh"
CONFIRM_REFRESH_URL = "https://passport.bilibili.com/x/passport-login/web/confirm/refresh"

# 刷新接口下发的cookie项（buvid3不在其中，保持原值）
REFRESHED_COOKIE_NAMES = ("SESSDATA", "bili_jct", "DedeUserID", "DedeUserID__ckMd5", "sid")

# correspond页面中refresh_csrf所在的div id
_REFRESH_CSRF_RE = re.compile(r'<div\s+id="1-name"[^>]*>([^<]+)</div>')


class CookieRefreshError(Exception):
    """Cookie刷新失败。fatal=True表示凭证已彻底失效，继续重试无意义"""

    def __init__(self, message: str, fatal: bool = False):
        super().__init__(message)
        self.fatal = fatal


def _build_correspond_path(timestamp_ms: int) -> str:
    """用官方公钥对 refresh_{毫秒时间戳} 做RSA-OAEP(SHA-256)加密，小写Base16输出"""
    key = RSA.import_key(REFRESH_RSA_PUBLIC_KEY)
    cipher = PKCS1_OAEP.new(key, SHA256)
    return cipher.encrypt(f"refresh_{timestamp_ms}".encode()).hex()


def _parse_cookie_str(cookie_str: str) -> dict:
    cookies = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            name, value = item.split("=", 1)
            cookies[name.strip()] = value.strip()
    return cookies


class CookieRefresher:
    """Cookie自动刷新器：周期检测刷新时机，需要时执行完整官方刷新流程。

    通过回调与宿主解耦：cookie与refresh_token的读取、刷新结果的持久化都由宿主完成。

    :param get_cookie: 获取当前cookie字符串的回调
    :param get_refresh_token: 获取当前refresh_token的回调
    :param on_refreshed: 刷新成功后的异步回调，入参为新cookie字典与新refresh_token
    :param check_interval: 检测间隔（秒），每次检测仅一次轻量接口调用
    :param on_fatal: 凭证彻底失效（fatal错误）时的异步回调，入参为错误信息。
                     提供时由宿主接管失效后的处理（如扫码重登），检测循环不终止
    """

    def __init__(self, get_cookie, get_refresh_token, on_refreshed, check_interval: float = 6 * 3600, on_fatal=None):
        self._get_cookie = get_cookie
        self._get_refresh_token = get_refresh_token
        self._on_refreshed = on_refreshed
        self._check_interval = check_interval
        self._on_fatal = on_fatal
        self._task: asyncio.Task | None = None

    async def start(self):
        """启动周期检测任务"""
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def stop(self):
        """停止检测任务"""
        if self._task:
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._task = None

    async def _loop(self):
        # 启动1分钟后先检测一次（覆盖插件离线多日导致cookie已到期的场景），之后按间隔周期检测
        await asyncio.sleep(60)
        while True:
            try:
                await self.refresh_once()
            except CookieRefreshError as e:
                log = logger.error if e.fatal else logger.warning
                log(f"Cookie自动刷新: {e}")
                if e.fatal:
                    if self._on_fatal:
                        # 由宿主接管凭证彻底失效的处理（如扫码重登），检测循环继续
                        try:
                            await self._on_fatal(str(e))
                        except Exception as ex:
                            logger.warning(f"Cookie失效后的接管处理出现异常: {ex}")
                    else:
                        break
            except Exception as e:
                logger.warning(f"Cookie自动刷新出现异常: {e}")
            await asyncio.sleep(self._check_interval)

    async def refresh_once(self) -> bool:
        """执行一次检测，需要时完成整个刷新流程。返回是否发生了刷新"""
        if not _CRYPTO_AVAILABLE:
            raise CookieRefreshError("缺少 pycryptodome 依赖，无法执行Cookie刷新")
        cookie_str = self._get_cookie()
        cookies = _parse_cookie_str(cookie_str)
        if not cookies.get("SESSDATA"):
            logger.debug("未填写SESSDATA，跳过Cookie刷新检测")
            return False
        refresh_token = self._get_refresh_token()
        if not refresh_token:
            logger.warning("未填写refresh_token，无法刷新Cookie（获取方法见配置说明）")
            return False

        headers = {
            "User-Agent": USER_AGENT,
            "Cookie": cookie_str,
            "Referer": "https://www.bilibili.com/",
        }
        async with aiohttp.ClientSession(
            headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            # 1. 检查是否需要刷新（timestamp为服务器毫秒时间戳，直接用于生成CorrespondPath）
            need_refresh, timestamp = await self._check_need_refresh(
                session, cookies.get("bili_jct", "")
            )
            if not need_refresh:
                logger.debug("Cookie无需刷新")
                return False
            logger.info("B站Cookie已达到刷新条件，开始自动刷新")
            # 2. 生成CorrespondPath，换取实时刷新口令refresh_csrf
            refresh_csrf = await self._get_refresh_csrf(
                session, _build_correspond_path(timestamp)
            )
            # 3. 刷新Cookie（旧cookie鉴权），务必先保存旧token供确认步骤使用
            old_refresh_token = refresh_token
            new_refresh_token, new_cookies = await self._do_refresh(
                session, cookies.get("bili_jct", ""), refresh_csrf, refresh_token
            )
            # 4. 确认更新（新cookie鉴权 + 旧token），使旧cookie彻底失效
            #    确认失败不影响新cookie生效，仅记日志，仍继续持久化新凭证
            merged = {**cookies, **new_cookies}
            try:
                await self._confirm_refresh(session, merged, old_refresh_token)
            except CookieRefreshError as e:
                logger.error(f"Cookie确认更新失败: {e}")

        await self._on_refreshed(new_cookies, new_refresh_token)
        logger.info("B站Cookie自动刷新成功")
        return True

    @staticmethod
    async def _check_need_refresh(session, csrf: str) -> tuple[bool, int]:
        """询问B站当前cookie是否需要刷新，返回(是否需要刷新, 服务器毫秒时间戳)"""
        params = {"csrf": csrf} if csrf else None
        async with session.get(COOKIE_INFO_URL, params=params) as resp:
            result = await resp.json()
        if result.get("code") == -101:
            raise CookieRefreshError(
                "B站账号未登录（code=-101），Cookie与refresh_token均已失效，"
                "请重新登录获取Cookie和refresh_token",
                fatal=True,
            )
        if result.get("code") != 0 or not result.get("data"):
            raise CookieRefreshError(
                f"查询刷新状态失败: code={result.get('code')}, message={result.get('message')}"
            )
        data = result["data"]
        return bool(data.get("refresh")), int(data.get("timestamp") or time.time() * 1000)

    @staticmethod
    async def _get_refresh_csrf(session, correspond_path: str) -> str:
        """访问correspond页面，从返回HTML中提取实时刷新口令"""
        async with session.get(CORRESPOND_URL + correspond_path) as resp:
            if resp.status == 404:
                raise CookieRefreshError(
                    "获取refresh_csrf失败: correspondPath校验未通过"
                    "（本机时间与B站服务器时间相差过大时可能出现）"
                )
            text = await resp.text()
        match = _REFRESH_CSRF_RE.search(text)
        if not match:
            raise CookieRefreshError("获取refresh_csrf失败: correspond页面中未找到口令")
        return match.group(1).strip()

    @staticmethod
    async def _do_refresh(session, csrf: str, refresh_csrf: str, refresh_token: str) -> tuple[str, dict]:
        """调用刷新接口，返回(新refresh_token, 新cookie字典)"""
        async with session.post(
            COOKIE_REFRESH_URL,
            data={
                "csrf": csrf,
                "refresh_csrf": refresh_csrf,
                "source": "main_web",
                "refresh_token": refresh_token,
            },
        ) as resp:
            new_cookies = CookieRefresher._parse_set_cookies(resp.headers)
            result = await resp.json()
        if result.get("code") != 0:
            raise CookieRefreshError(
                f"刷新Cookie失败: code={result.get('code')}, message={result.get('message')}"
                "（refresh_token与当前Cookie不匹配时，需重新获取两者）"
            )
        new_refresh_token = (result.get("data") or {}).get("refresh_token")
        if not new_refresh_token or "SESSDATA" not in new_cookies or "bili_jct" not in new_cookies:
            raise CookieRefreshError("刷新Cookie失败: 响应缺少新refresh_token或Set-Cookie")
        return new_refresh_token, new_cookies

    @staticmethod
    async def _confirm_refresh(session, new_cookies: dict, old_refresh_token: str):
        """确认更新：作废旧refresh_token对应的旧Cookie"""
        cookie_str = "; ".join(f"{name}={value}" for name, value in new_cookies.items())
        async with session.post(
            CONFIRM_REFRESH_URL,
            data={"csrf": new_cookies["bili_jct"], "refresh_token": old_refresh_token},
            headers={"Cookie": cookie_str},
        ) as resp:
            result = await resp.json()
        if result.get("code") != 0:
            raise CookieRefreshError(
                f"code={result.get('code')}, message={result.get('message')}"
            )

    @staticmethod
    def _parse_set_cookies(headers) -> dict:
        """从响应头解析Set-Cookie，仅保留刷新接口下发的关键cookie项"""
        cookies = {}
        for header in headers.getall("Set-Cookie", ()):
            jar = SimpleCookie()
            jar.load(header)
            for name, morsel in jar.items():
                if name in REFRESHED_COOKIE_NAMES:
                    cookies[name] = morsel.value
        return cookies
