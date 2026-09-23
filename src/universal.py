#!/usr/bin/env python3
"""Safe, generic public-web URL -> stream discovery engine.

This engine discovers explicitly exposed public HLS/M3U/M3U8/media URLs from a
target page or playlist. It does not bypass authentication, DRM, signatures,
anti-bot challenges, or other access controls.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from dataclasses import asdict, dataclass
from typing import Optional
from urllib.parse import parse_qsl, unquote, urljoin, urlparse, urlunparse

import aiohttp
from bs4 import BeautifulSoup
import m3u8


MEDIA_RE = re.compile(
    r"https?://[^\s"'<>\\]+\.(?:m3u8|m3u|mp4|ts|m4s)(?:\?[^\s"'<>\\]*)?",
    re.IGNORECASE,
)
HLS_PATH_RE = re.compile(
    r"https?://[^\s"'<>\\]*(?:/hls/|/live/|/stream/)[^\s"'<>\\]+",
    re.IGNORECASE,
)
PLAYER_KEY_RE = re.compile(
    r"["'](?:file|source|src|hls|m3u8|manifest|stream|url)["']\s*:\s*["']([^"']+)["']",
    re.IGNORECASE,
)


@dataclass(slots=True)
class DiscoveredStream:
    name: str
    url: str
    referer: str = ""
    category: str = "Live"
    logo: str = ""
    source_page: str = ""
    custom_headers: Optional[dict[str, str]] = None

    def as_json(self) -> dict:
        item = asdict(self)
        item["customHeaders"] = item.pop("custom_headers")
        item["sourcePage"] = item.pop("source_page")
        return item


class UniversalScraper:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        timeout: float = 12.0,
        retries: int = 2,
        max_iframes: int = 10,
        max_scripts: int = 12,
    ) -> None:
        self.session = session
        self.timeout = max(3.0, timeout)
        self.retries = max(0, retries)
        self.max_iframes = max(0, max_iframes)
        self.max_scripts = max(0, max_scripts)

    @staticmethod
    def _headers(referer: str) -> dict[str, str]:
        origin = ""
        try:
            parsed = urlparse(referer)
            if parsed.scheme and parsed.netloc:
                origin = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
        except Exception:
            pass

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "application/vnd.apple.mpegurl,application/x-mpegURL,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.8",
            "Cache-Control": "no-cache",
        }
        if referer:
            headers["Referer"] = referer
        if origin:
            headers["Origin"] = origin
        return headers

    @staticmethod
    async def _assert_public_url(url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Only public HTTP(S) URLs are supported")

        host = parsed.hostname.lower()
        try:
            ip = ipaddress.ip_address(host)
            if (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_reserved
                or ip.is_multicast
                or ip.is_unspecified
            ):
                raise ValueError("Private or local network targets are not allowed")
            return
        except ValueError as exc:
            if "not allowed" in str(exc):
                raise

        try:
            infos = await asyncio.to_thread(
                socket.getaddrinfo,
                host,
                443 if parsed.scheme == "https" else 80,
                type=socket.SOCK_STREAM,
            )
        except socket.gaierror as exc:
            raise ValueError(f"DNS lookup failed for {host}") from exc

        addresses = {item[4][0] for item in infos if item and item[4]}
        for raw_ip in addresses:
            try:
                ip = ipaddress.ip_address(raw_ip)
            except ValueError:
                continue
            if (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_reserved
                or ip.is_multicast
                or ip.is_unspecified
            ):
                raise ValueError(f"Target resolves to a private/local address: {host}")

    @staticmethod
    def _clean_media_url(raw: str, base_url: str) -> Optional[str]:
        value = unquote(raw).strip().strip(" 	\"'<>);,")
        value = value.replace("\\\\/", "/").replace("\\/", "/")
        if value.startswith("//"):
            value = "https:" + value
        try:
            absolute = value if value.startswith(("http://", "https://")) else urljoin(base_url, value)
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                return None
            return absolute
        except Exception:
            return None

    @staticmethod
    def _name_from_url(url: str, fallback: str = "Live Stream") -> str:
        try:
            parsed = urlparse(url)
            query = dict(parse_qsl(parsed.query, keep_blank_values=True))
            for key in ("name", "channel", "title", "stream", "id", "tvg-name"):
                value = query.get(key)
                if value and len(value) < 120:
                    return re.sub(r"[_+\-]+", " ", value).strip().title()

            parts = [p for p in parsed.path.split("/") if p]
            base = parts[-1] if parts else ""
            base = re.sub(r"\.(m3u8|m3u|mp4|ts|m4s|php|html?)$", "", base, flags=re.I)
            if base.lower() in {
                "master", "index", "playlist", "live", "stream", "manifest",
                "chunklist", "playlist0", "playlist1",
            } and len(parts) >= 2:
                base = re.sub(r"[-_]+", " ", parts[-2])
            base = re.sub(r"[-_+]+", " ", base).strip()
            if base and len(base) < 100:
                return base.title()
            host = parsed.hostname.split(".")[0] if parsed.hostname else ""
            if host and host not in {"www", "cdn", "api"}:
                return host.title()
        except Exception:
            pass
        return fallback

    def _add(
        self,
        out: list[DiscoveredStream],
        seen: set[str],
        *,
        name: str,
        url: str,
        referer: str,
        category: str = "Live",
        logo: str = "",
        source_page: str = "",
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        cleaned = self._clean_media_url(url, referer or source_page or url)
        if not cleaned:
            return
        key = cleaned.split("#", 1)[0]
        if key in seen:
            return
        seen.add(key)
        out.append(
            DiscoveredStream(
                name=(name or self._name_from_url(cleaned)).strip()[:180],
                url=cleaned,
                referer=referer or source_page,
                category=category or "Live",
                logo=logo,
                source_page=source_page or referer,
                custom_headers=headers,
            )
        )

    def _parse_m3u(self, content: str, base_url: str) -> list[DiscoveredStream]:
        lines = [line.strip() for line in content.splitlines()]
        result: list[DiscoveredStream] = []
        seen: set[str] = set()
        current_name = ""
        current_group = "Live"
        current_logo = ""
        current_headers: dict[str, str] = {}

        for line in lines:
            if not line:
                continue
            upper = line.upper()

            if upper.startswith("#EXTINF:"):
                if "," in line:
                    current_name = line.split(",", 1)[1].strip()
                logo = re.search(r'tvg-logo="([^"]+)"', line, flags=re.I)
                if logo:
                    current_logo = logo.group(1).strip()
                group = re.search(r'group-title="([^"]+)"', line, flags=re.I)
                if group:
                    current_group = group.group(1).strip()
                continue

            if upper.startswith("#EXTVLCOPT:"):
                opt = line.split(":", 1)[1].strip()
                low = opt.lower()
                if low.startswith("http-referrer="):
                    current_headers["referer"] = opt.split("=", 1)[1].strip()
                elif low.startswith("http-user-agent="):
                    current_headers["user-agent"] = opt.split("=", 1)[1].strip()
                continue

            if line.startswith("#") or line.startswith("//"):
                continue

            raw_url = line
            if "|" in raw_url:
                raw_url, pipe = raw_url.split("|", 1)
                params = dict(parse_qsl(pipe.replace(" ", "&"), keep_blank_values=True))
                for key, value in params.items():
                    current_headers[key.lower()] = value

            clean_path = raw_url.split("?", 1)[0].lower()
            if clean_path.endswith((".ts", ".m4s", ".aac", ".vtt")):
                current_name = ""
                current_group = "Live"
                current_logo = ""
                current_headers = {}
                continue

            self._add(
                result,
                seen,
                name=current_name or self._name_from_url(raw_url),
                url=raw_url,
                referer=current_headers.get("referer", base_url),
                category=current_group,
                logo=current_logo,
                source_page=base_url,
                headers=current_headers or None,
            )
            current_name = ""
            current_group = "Live"
            current_logo = ""
            current_headers = {}

        return result

    def _scan_html(self, html: str, page_url: str) -> tuple[list[DiscoveredStream], list[str], list[str]]:
        soup = BeautifulSoup(html, "html.parser")
        result: list[DiscoveredStream] = []
        seen: set[str] = set()
        iframe_urls: list[str] = []
        script_urls: list[str] = []

        # Explicit media/source/data attributes.
        for element in soup.find_all(
            ["video", "source", "a", "embed"],
        ):
            raw = (
                element.get("src")
                or element.get("href")
                or element.get("data-src")
                or element.get("data-url")
                or element.get("data-stream")
                or element.get("data-hls")
                or element.get("data-m3u8")
            )
            if not raw:
                continue

            media_url = self._clean_media_url(raw, page_url)
            if not media_url:
                continue

            lower = media_url.lower()
            looks_media = (
                ".m3u8" in lower
                or ".m3u" in lower
                or ".mp4" in lower
                or ".ts" in lower
                or "workers.dev/" in lower
                or "/hls/" in lower
                or "/stream/" in lower
            )
            if not looks_media:
                continue

            name = (
                element.get("data-title")
                or element.get("data-channel")
                or element.get("title")
                or (element.get_text(" ", strip=True) if element.name == "a" else "")
                or self._name_from_url(media_url)
            )
            self._add(
                result,
                seen,
                name=name,
                url=media_url,
                referer=page_url,
                category="Live",
                source_page=page_url,
            )

        for element in soup.find_all(["iframe"]):
            raw = element.get("src")
            if not raw or raw.lower().startswith(("javascript:", "about:", "data:")):
                continue
            child = self._clean_media_url(raw, page_url)
            if child:
                iframe_urls.append(child)

        for element in soup.find_all("script"):
            src = element.get("src")
            if src:
                child = self._clean_media_url(src, page_url)
                if child:
                    script_urls.append(child)

            script_text = element.string or element.get_text() or ""
            for match in MEDIA_RE.findall(script_text):
                self._add(
                    result,
                    seen,
                    name=self._name_from_url(match),
                    url=match,
                    referer=page_url,
                    category="Discovered",
                    source_page=page_url,
                )

            for match in HLS_PATH_RE.findall(script_text):
                self._add(
                    result,
                    seen,
                    name=self._name_from_url(match),
                    url=match,
                    referer=page_url,
                    category="Discovered",
                    source_page=page_url,
                )

            for match in PLAYER_KEY_RE.findall(script_text):
                candidate = self._clean_media_url(match, page_url)
                if candidate and any(
                    token in candidate.lower()
                    for token in (".m3u8", ".m3u", ".mp4", ".ts", "/hls/", "/stream/")
                ):
                    self._add(
                        result,
                        seen,
                        name=self._name_from_url(candidate),
                        url=candidate,
                        referer=page_url,
                        category="Player Config",
                        source_page=page_url,
                    )

        # Raw page text catches escaped/templated URLs.
        for match in MEDIA_RE.findall(html):
            self._add(
                result,
                seen,
                name=self._name_from_url(match),
                url=match,
                referer=page_url,
                category="Discovered",
                source_page=page_url,
            )

        return result, iframe_urls, script_urls

    async def _fetch_text(self, url: str, referer: str) -> tuple[str, str]:
        await self._assert_public_url(url)
        headers = self._headers(referer)

        last_error: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                async with self.session.get(
                    url,
                    headers=headers,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:
                    final_url = str(response.url)
                    await self._assert_public_url(final_url)
                    body = await response.text(errors="replace")

                    if response.status in {401, 403}:
                        raise PermissionError(
                            f"Upstream access denied ({response.status}) for {url}"
                        )
                    if 200 <= response.status < 300:
                        return body, final_url
                    if response.status in {408, 425, 429, 500, 502, 503, 504} and attempt < self.retries:
                        await asyncio.sleep(0.4 * (2 ** attempt))
                        continue
                    raise RuntimeError(f"Upstream HTTP {response.status} for {url}")
            except (PermissionError, ValueError):
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                await asyncio.sleep(0.4 * (2 ** attempt))

        raise RuntimeError(f"Fetch failed for {url}: {last_error or 'unknown error'}")

    async def scan(self, target_url: str) -> tuple[list[DiscoveredStream], list[str]]:
        target_url = target_url.strip()
        await self._assert_public_url(target_url)

        logs = [f"Target: {target_url}", "Fetching target..."]
        body, final_url = await self._fetch_text(target_url, "")
        streams: list[DiscoveredStream] = []
        seen: set[str] = set()

        # Direct M3U/M3U8 response.
        if body.lstrip().startswith("#EXTM3U") or "#EXTINF:" in body:
            logs.append("Playlist content detected; parsing M3U/M3U8...")
            parsed = self._parse_m3u(body, final_url)
            for item in parsed:
                self._add(
                    streams,
                    seen,
                    name=item.name,
                    url=item.url,
                    referer=item.referer or final_url,
                    category=item.category,
                    logo=item.logo,
                    source_page=item.source_page or final_url,
                    headers=item.custom_headers,
                )

            try:
                playlist = m3u8.loads(body, uri=final_url)
                if playlist.is_variant:
                    self._add(
                        streams,
                        seen,
                        name=f"{self._name_from_url(final_url)} (Master)",
                        url=final_url,
                        referer=final_url,
                        category="Master HLS",
                        source_page=final_url,
                    )
                    for variant in playlist.playlists:
                        absolute = urljoin(final_url, variant.uri)
                        label = (
                            str(variant.stream_info.resolution)
                            if variant.stream_info and variant.stream_info.resolution
                            else f"{(variant.stream_info.bandwidth or 0) // 1000}k"
                        )
                        self._add(
                            streams,
                            seen,
                            name=f"{self._name_from_url(final_url)} [{label}]",
                            url=absolute,
                            referer=final_url,
                            category="HLS Variant",
                            source_page=final_url,
                        )
            except Exception:
                pass

            logs.append(f"Playlist parser found {len(streams)} stream(s).")
        else:
            direct, iframe_urls, script_urls = self._scan_html(body, final_url)
            for item in direct:
                self._add(
                    streams,
                    seen,
                    name=item.name,
                    url=item.url,
                    referer=item.referer or final_url,
                    category=item.category,
                    logo=item.logo,
                    source_page=item.source_page or final_url,
                    headers=item.custom_headers,
                )

            logs.append(
                f"HTML scan found {len(direct)} direct media link(s), "
                f"{len(iframe_urls)} iframe(s), {len(script_urls)} external script(s)."
            )

            # Scan a small number of external JS files concurrently.
            script_urls = list(dict.fromkeys(script_urls))[: self.max_scripts]
            if script_urls:
                async def fetch_script(url: str) -> Optional[tuple[str, str]]:
                    try:
                        return await self._fetch_text(url, final_url)
                    except Exception:
                        return None

                script_results = await asyncio.gather(
                    *(fetch_script(url) for url in script_urls),
                    return_exceptions=False,
                )
                for item in script_results:
                    if not item:
                        continue
                    script_body, script_url = item
                    for match in MEDIA_RE.findall(script_body):
                        self._add(
                            streams,
                            seen,
                            name=self._name_from_url(match),
                            url=match,
                            referer=final_url,
                            category="Script",
                            source_page=script_url,
                        )
                    for match in HLS_PATH_RE.findall(script_body):
                        self._add(
                            streams,
                            seen,
                            name=self._name_from_url(match),
                            url=match,
                            referer=final_url,
                            category="Script",
                            source_page=script_url,
                        )

            # One-level iframe crawling for embedded players.
            iframe_urls = list(dict.fromkeys(iframe_urls))[: self.max_iframes]
            if iframe_urls:
                async def fetch_iframe(url: str) -> Optional[tuple[str, str]]:
                    try:
                        return await self._fetch_text(url, final_url)
                    except Exception:
                        return None

                iframe_results = await asyncio.gather(
                    *(fetch_iframe(url) for url in iframe_urls),
                    return_exceptions=False,
                )
                for item in iframe_results:
                    if not item:
                        continue
                    iframe_body, iframe_url = item
                    if iframe_body.lstrip().startswith("#EXTM3U") or "#EXTINF:" in iframe_body:
                        parsed = self._parse_m3u(iframe_body, iframe_url)
                        for stream in parsed:
                            self._add(
                                streams,
                                seen,
                                name=stream.name,
                                url=stream.url,
                                referer=stream.referer or iframe_url,
                                category="Iframe Playlist",
                                logo=stream.logo,
                                source_page=iframe_url,
                                headers=stream.custom_headers,
                            )
                    else:
                        direct, _, _ = self._scan_html(iframe_body, iframe_url)
                        for stream in direct:
                            self._add(
                                streams,
                                seen,
                                name=stream.name,
                                url=stream.url,
                                referer=stream.referer or iframe_url,
                                category="Iframe",
                                logo=stream.logo,
                                source_page=iframe_url,
                                headers=stream.custom_headers,
                            )

            logs.append(f"Final unique stream count: {len(streams)}.")

        if not streams:
            logs.append("No explicitly exposed playable media URL was found.")
        return streams, logs
