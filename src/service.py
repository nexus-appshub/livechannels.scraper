#!/usr/bin/env python3
"""Deployment entrypoint: continuous channel workers + lightweight health server."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from aiohttp import web

from scraper import ChannelWorker, HLSClient, load_channels
import aiohttp


LOG = logging.getLogger("livechannels.service")


async def run_service() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    output_dir = Path(os.getenv("OUTPUT_DIR", "data"))
    config_path = Path(os.getenv("CHANNEL_CONFIG", "channels.json"))
    append_ts = os.getenv("APPEND_TS", "false").lower() in {"1", "true", "yes", "on"}

    channels = load_channels(config_path, output_dir)
    if not channels:
        raise RuntimeError("No enabled channels configured")

    stop = asyncio.Event()

    async def health(_: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "ok",
                "service": "livechannels-scraper",
                "workers": len(channels),
            }
        )

    async def root(_: web.Request) -> web.Response:
        return web.json_response(
            {
                "service": "livechannels-scraper",
                "status": "running",
                "health": "/health",
                "workers": len(channels),
            }
        )

    app = web.Application()
    app.router.add_get("/", root)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)

    connector = aiohttp.TCPConnector(
        limit=50,
        limit_per_host=8,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        client = HLSClient(
            session,
            retries=int(os.getenv("UPSTREAM_RETRIES", "4")),
            timeout=float(os.getenv("UPSTREAM_TIMEOUT", "10")),
        )

        workers = [
            ChannelWorker(channel, client, stop, append_ts)
            for channel in channels
        ]

        tasks = [
            asyncio.create_task(worker.run(), name=f"channel:{channel.name}")
            for worker, channel in zip(workers, channels)
        ]

        await site.start()
        LOG.info("health server listening on 0.0.0.0:%s", port)
        LOG.info("running %d channel worker(s)", len(tasks))

        try:
            await asyncio.Event().wait()
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await runner.cleanup()


def main() -> None:
    asyncio.run(run_service())


if __name__ == "__main__":
    main()
