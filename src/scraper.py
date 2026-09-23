#!/usr/bin/env python3
"""Continuous HLS/M3U8 -> TS ingester for authorized/public streams."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import random
import signal
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import aiohttp
import m3u8

LOG = logging.getLogger("livechannels.scraper")
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
ACCESS_DENIED_STATUS = {401, 403}


class UpstreamError(RuntimeError):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class AccessDeniedError(UpstreamError):
    pass


@dataclass(slots=True)
class Channel:
    name: str
    url: str
    output_dir: Path
    headers: dict[str, str]
    enabled: bool = True
    poll_seconds: Optional[float] = None
    max_bandwidth: Optional[int] = None


@dataclass(slots=True)
class ResolvedHLS:
    source_url: str
    media_url: str
    master: bool
    variant_bandwidth: Optional[int] = None
    variant_resolution: Optional[str] = None


def backoff(attempt: int) -> float:
    base = 0.25 * (2 ** attempt)
    return base + random.uniform(0.0, base * 0.25)


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def build_headers(channel: Channel) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.apple.mpegurl, application/x-mpegURL, */*",
        "User-Agent": "livechannels-scraper/1.0",
        "Connection": "keep-alive",
    }
    headers.update(channel.headers)
    return headers


class HLSClient:
    def __init__(self, session: aiohttp.ClientSession, retries: int, timeout: float):
        self.session = session
        self.retries = retries
        self.timeout = timeout

    async def fetch_text(self, url: str, headers: dict[str, str]) -> tuple[str, str]:
        for attempt in range(self.retries + 1):
            try:
                async with self.session.get(
                    url,
                    headers=headers,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:
                    status = response.status
                    final_url = str(response.url)
                    body = await response.text(errors="replace")

                    if status in ACCESS_DENIED_STATUS:
                        raise AccessDeniedError(
                            f"upstream access denied ({status}) for {url}", status
                        )
                    if 200 <= status < 300:
                        return body, final_url
                    if status not in RETRYABLE_STATUS and status < 500:
                        raise UpstreamError(f"upstream HTTP {status} for {url}", status)

                    if attempt >= self.retries:
                        raise UpstreamError(
                            f"upstream HTTP {status} after retries for {url}", status
                        )

                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    delay = retry_after if retry_after is not None else backoff(attempt)
                    LOG.warning("HTTP %s; retrying in %.2fs: %s", status, delay, url)
                    await asyncio.sleep(delay)

            except AccessDeniedError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self.retries:
                    raise UpstreamError(
                        f"network failure after retries for {url}: {exc}"
                    ) from exc
                delay = backoff(attempt)
                LOG.warning("network error; retrying in %.2fs: %s", delay, url)
                await asyncio.sleep(delay)

        raise AssertionError("unreachable")

    async def fetch_bytes(self, url: str, headers: dict[str, str]) -> bytes:
        for attempt in range(self.retries + 1):
            try:
                async with self.session.get(
                    url,
                    headers=headers,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:
                    status = response.status

                    if status in ACCESS_DENIED_STATUS:
                        raise AccessDeniedError(
                            f"upstream access denied ({status}) for {url}", status
                        )
                    if 200 <= status < 300:
                        return await response.read()
                    if status not in RETRYABLE_STATUS and status < 500:
                        raise UpstreamError(f"segment HTTP {status} for {url}", status)

                    if attempt >= self.retries:
                        raise UpstreamError(
                            f"segment HTTP {status} after retries for {url}", status
                        )

                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    delay = retry_after if retry_after is not None else backoff(attempt)
                    LOG.warning("segment HTTP %s; retrying in %.2fs: %s", status, delay, url)
                    await asyncio.sleep(delay)

            except AccessDeniedError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self.retries:
                    raise UpstreamError(
                        f"segment network failure after retries for {url}: {exc}"
                    ) from exc
                delay = backoff(attempt)
                LOG.warning("segment network error; retrying in %.2fs: %s", delay, url)
                await asyncio.sleep(delay)

        raise AssertionError("unreachable")


def resolve_master(master_url: str, body: str, max_bandwidth: Optional[int]) -> ResolvedHLS:
    playlist = m3u8.loads(body, uri=master_url)
    if not playlist.is_variant:
        return ResolvedHLS(master_url, master_url, False)

    variants = []
    for item in playlist.playlists:
        info = item.stream_info
        bandwidth = info.bandwidth or 0
        if max_bandwidth is not None and bandwidth > max_bandwidth:
            continue
        variants.append(
            (
                bandwidth,
                info.resolution,
                urljoin(master_url, item.uri),
            )
        )

    if not variants:
        for item in playlist.playlists:
            info = item.stream_info
            variants.append(
                (
                    info.bandwidth or 0,
                    info.resolution,
                    urljoin(master_url, item.uri),
                )
            )

    if not variants:
        raise UpstreamError(f"master playlist has no variants: {master_url}")

    bandwidth, resolution, media_url = max(variants, key=lambda x: x[0])
    return ResolvedHLS(
        master_url,
        media_url,
        True,
        bandwidth,
        str(resolution) if resolution else None,
    )


def segment_key(sequence: int, uri: str) -> str:
    digest = hashlib.sha1(uri.encode("utf-8"), usedforsecurity=False).hexdigest()[:12]
    return f"{sequence}:{digest}"


class ChannelWorker:
    def __init__(self, channel: Channel, client: HLSClient, stop: asyncio.Event, append_ts: bool):
        self.channel = channel
        self.client = client
        self.stop = stop
        self.append_ts = append_ts
        self.seen: set[str] = set()
        self.last_sequence: Optional[int] = None
        self.output_handle = None

    async def run(self) -> None:
        self.channel.output_dir.mkdir(parents=True, exist_ok=True)
        if self.append_ts:
            self.output_handle = (self.channel.output_dir / "live.ts").open("ab")

        try:
            while not self.stop.is_set():
                try:
                    headers = build_headers(self.channel)
                    body, final_url = await self.client.fetch_text(self.channel.url, headers)
                    resolved = resolve_master(
                        final_url, body, self.channel.max_bandwidth
                    )

                    if resolved.master:
                        LOG.info(
                            "[%s] master -> %s (%s, %s bps)",
                            self.channel.name,
                            resolved.media_url,
                            resolved.variant_resolution or "unknown",
                            resolved.variant_bandwidth or "unknown",
                        )
                    else:
                        LOG.info("[%s] media playlist: %s", self.channel.name, resolved.media_url)

                    await self.consume(resolved.media_url, headers)

                except AccessDeniedError as exc:
                    # Access-control failure is reported, never bypassed.
                    LOG.error("[%s] access denied: %s", self.channel.name, exc)
                    await wait_or_stop(self.stop, 30)
                except UpstreamError as exc:
                    LOG.warning("[%s] upstream failure: %s", self.channel.name, exc)
                    await wait_or_stop(self.stop, 5)
                except Exception:
                    LOG.exception("[%s] unexpected worker failure", self.channel.name)
                    await wait_or_stop(self.stop, 5)
        finally:
            if self.output_handle:
                self.output_handle.close()

    async def consume(self, media_url: str, headers: dict[str, str]) -> None:
        while not self.stop.is_set():
            body, final_url = await self.client.fetch_text(media_url, headers)
            playlist = m3u8.loads(body, uri=final_url)

            if playlist.is_variant:
                resolved = resolve_master(
                    final_url, body, self.channel.max_bandwidth
                )
                media_url = resolved.media_url
                continue

            target = float(playlist.target_duration or self.channel.poll_seconds or 4.0)
            poll = self.channel.poll_seconds or max(1.0, target / 2.0)

            if playlist.media_sequence is not None:
                seq = int(playlist.media_sequence)
                if self.last_sequence is not None and seq < self.last_sequence:
                    LOG.info(
                        "[%s] media sequence reset %s -> %s",
                        self.channel.name,
                        self.last_sequence,
                        seq,
                    )
                    self.seen.clear()
                self.last_sequence = seq

            # Do not attempt to decrypt protected HLS.
            if playlist.keys and any(key is not None for key in playlist.keys):
                raise UpstreamError(
                    f"encrypted HLS detected for {self.channel.name}; key handling disabled"
                )

            for index, segment in enumerate(playlist.segments):
                sequence = int(playlist.media_sequence or 0) + index
                uri = urljoin(final_url, segment.uri)
                key = segment_key(sequence, uri)
                if key in self.seen:
                    continue

                try:
                    payload = await self.client.fetch_bytes(uri, headers)
                    await self.store(sequence, uri, payload)
                    self.seen.add(key)
                except AccessDeniedError:
                    raise
                except UpstreamError as exc:
                    # Leave unseen so the next playlist refresh can retry it.
                    LOG.warning(
                        "[%s] segment failed seq=%s: %s",
                        self.channel.name,
                        sequence,
                        exc,
                    )

            if len(self.seen) > 5000:
                self.seen = set(list(self.seen)[-2500:])

            await wait_or_stop(self.stop, poll)

    async def store(self, sequence: int, uri: str, payload: bytes) -> None:
        clean_path = uri.lower().split("?", 1)[0]
        ext = ".ts" if clean_path.endswith(".ts") else ".bin"
        path = self.channel.output_dir / f"{sequence:012d}{ext}"
        path.write_bytes(payload)

        if self.output_handle and ext == ".ts":
            self.output_handle.write(payload)
            self.output_handle.flush()

        LOG.info(
            "[%s] saved seq=%s bytes=%s",
            self.channel.name,
            sequence,
            len(payload),
        )


async def wait_or_stop(event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


def load_channels(path: Path, output_root: Path) -> list[Channel]:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        raw = os.getenv("CHANNELS_JSON", "").strip()
        if not raw:
            raise FileNotFoundError(
                f"{path} not found and CHANNELS_JSON is not configured"
            )
        data = json.loads(raw)
    result: list[Channel] = []

    for item in data.get("channels", []):
        if not item.get("enabled", True):
            continue

        name = str(item["name"])
        safe = "".join(
            c if c.isalnum() or c in "-_." else "_"
            for c in name
        ).strip("_") or "channel"

        result.append(
            Channel(
                name=name,
                url=str(item["url"]),
                output_dir=output_root / safe,
                headers={str(k): str(v) for k, v in item.get("headers", {}).items()},
                poll_seconds=float(item["poll_seconds"])
                if item.get("poll_seconds") else None,
                max_bandwidth=int(item["max_bandwidth"])
                if item.get("max_bandwidth") else None,
            )
        )

    return result


def install_signals(loop: asyncio.AbstractEventLoop, event: asyncio.Event) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, event.set)
        except (NotImplementedError, RuntimeError):
            pass


async def main_async(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    channels = load_channels(Path(args.config), Path(args.output))
    if not channels:
        raise SystemExit("No enabled channels found in config")

    stop = asyncio.Event()
    install_signals(asyncio.get_running_loop(), stop)

    connector = aiohttp.TCPConnector(
        limit=50,
        limit_per_host=8,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        client = HLSClient(session, retries=args.retries, timeout=args.timeout)
        workers = [
            ChannelWorker(channel, client, stop, args.append_ts)
            for channel in channels
        ]
        tasks = [
            asyncio.create_task(worker.run(), name=f"channel:{worker.channel.name}")
            for worker in workers
        ]

        LOG.info("started %d channel worker(s)", len(tasks))
        await stop.wait()
        LOG.info("stopping workers")

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuous HLS/M3U8 -> TS scraper")
    parser.add_argument("--config", default="channels.json")
    parser.add_argument("--output", default="data")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--append-ts", action="store_true")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    try:
        asyncio.run(main_async(parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
