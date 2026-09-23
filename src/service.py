#!/usr/bin/env python3
"""LiveChannels dashboard + controlled HLS gateway."""

from __future__ import annotations

import asyncio
import base64
import html
import logging
import os
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp
import m3u8
from aiohttp import web

from scraper import (
    AccessDeniedError,
    Channel,
    ChannelManager,
    ChannelWorker,
    HLSClient,
    build_headers,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("livechannels.service")

routes = web.RouteTableDef()
channel_manager = ChannelManager()


def encode_target(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode("utf-8")).decode("ascii").rstrip("=")


def decode_target(value: str) -> str:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii")).decode("utf-8")


def origin_for(request: web.Request) -> str:
    forwarded = request.headers.get("X-Forwarded-Proto", "").split(",", 1)[0].strip()
    scheme = forwarded or request.scheme
    return f"{scheme}://{request.host}"


def is_hls_body(body: str) -> bool:
    return body.lstrip().startswith("#EXTM3U")


def safe_source_target(channel: Channel, target: str) -> bool:
    parsed = urlparse(target)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False

    # Only explicitly configured source hosts are accepted here.
    allowed = channel_manager._allowed_hosts.setdefault(channel.id, set())
    return parsed.hostname.lower() in allowed


def rewrite_manifest(
    channel: Channel,
    final_url: str,
    body: str,
    base: str,
) -> str:
    playlist = m3u8.loads(body, uri=final_url)

    if playlist.keys and any(key is not None for key in playlist.keys):
        raise web.HTTPBadGateway(
            text="Encrypted/protected HLS is not handled by this gateway"
        )

    cid = channel.id

    if playlist.is_variant:
        for item in playlist.playlists:
            absolute = urljoin(final_url, item.uri)
            if not safe_observed_child(channel, absolute):
                raise web.HTTPBadGateway(
                    text="HLS child playlist host is not an approved source host"
                )
            item.uri = f"{base}/stream/{cid}/master.m3u8?u={encode_target(absolute)}"
    else:
        for index, segment in enumerate(playlist.segments):
            absolute = urljoin(final_url, segment.uri)
            if not safe_observed_child(channel, absolute):
                raise web.HTTPBadGateway(
                    text="HLS segment host is not an approved source host"
                )
            segment.uri = f"{base}/stream/{cid}/segment?u={encode_target(absolute)}"

    return playlist.dumps()


def safe_observed_child(channel: Channel, target: str) -> bool:
    parsed = urlparse(target)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False

    hostname = parsed.hostname.lower()
    allowed = channel_manager._allowed_hosts.setdefault(channel.id, set())

    if hostname in allowed:
        return True

    # Permit a public host only after it has been observed in a source response.
    # The service caller adds redirected response hosts to the same allowlist.
    if channel_manager._is_public_hostname(hostname):
        return False

    return False


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>LiveChannels Gateway</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
<style>
:root { --bg:#121212; --card:#1e1e1e; --text:#e0e0e0; --muted:#9e9e9e; --accent:#0277bd; }
* { box-sizing:border-box; }
body { font-family:system-ui,-apple-system,sans-serif; background:var(--bg); color:var(--text); margin:0; padding:20px; }
.header { margin-bottom:25px; border-bottom:1px solid #333; padding-bottom:15px; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(320px,1fr)); gap:20px; }
.card { background:var(--card); padding:18px; border-radius:10px; border:1px solid #333; box-shadow:0 4px 6px rgba(0,0,0,.3); }
.card h3 { margin:0 0 15px; display:flex; justify-content:space-between; gap:10px; align-items:center; font-size:1.05rem; }
.status { font-size:.72rem; padding:4px 10px; border-radius:20px; font-weight:bold; text-transform:uppercase; }
.status.healthy,.status.starting { background:rgba(27,94,32,.3); color:#81c784; border:1px solid #2e7d32; }
.status.degraded { background:rgba(245,127,23,.3); color:#fff176; border:1px solid #fbc02d; }
.status.down { background:rgba(183,28,28,.3); color:#e57373; border:1px solid #c62828; }
.status.unknown { background:rgba(90,90,90,.25); color:#bdbdbd; border:1px solid #616161; }
.url-box { background:#000; padding:10px; font-family:monospace; font-size:.78rem; overflow-x:auto; margin-bottom:15px; border-radius:6px; color:#a5d6a7; white-space:nowrap; }
.btn-group { display:flex; gap:10px; }
.btn { flex:1; padding:8px 12px; border:none; border-radius:6px; cursor:pointer; color:#fff; background:var(--accent); font-size:.84rem; font-weight:500; text-align:center; text-decoration:none; }
.btn-play { background:#2e7d32; }
.btn-open { background:#424242; }
#player-modal { display:none; position:fixed; inset:0; width:100%; height:100%; background:rgba(0,0,0,.95); z-index:1000; justify-content:center; align-items:center; flex-direction:column; padding:20px; }
video { width:100%; max-width:900px; aspect-ratio:16/9; background:#000; border-radius:8px; }
.close-btn { margin-top:20px; max-width:200px; flex:none; }
.empty { padding:30px; border:1px dashed #444; border-radius:10px; color:var(--muted); }
</style>
</head>
<body>
<div class="header">
<h1 style="margin:0 0 5px;">LiveChannels Gateway</h1>
<p style="margin:0;color:#9e9e9e;">
Active Channels: <strong id="channel-count" style="color:#fff;">0</strong>
| Network Status: <span id="sys-status" style="color:#81c784;">Online</span>
</p>
</div>

<div class="grid" id="channel-grid">
<div class="empty">Loading channels...</div>
</div>

<div id="player-modal">
<video id="video-player" controls playsinline></video>
<button class="btn close-btn" onclick="closePlayer()">Close Player</button>
</div>

<script>
async function fetchChannels() {
  try {
    const res = await fetch('/api/channels', {cache:'no-store'});
    const data = await res.json();
    document.getElementById('channel-count').innerText = data.channels.length;

    const grid = document.getElementById('channel-grid');
    if (!data.channels.length) {
      grid.innerHTML = '<div class="empty">No channels configured. Set REMOTE_CONFIG_URL or CHANNELS_JSON and redeploy.</div>';
      return;
    }

    grid.innerHTML = data.channels.map(ch => {
      const safeName = escapeHtml(ch.name);
      const fullUrl = new URL(ch.gatewayUrl, window.location.origin).toString();
      const statusClass = (ch.status || 'UNKNOWN').toLowerCase();
      return `
        <div class="card">
          <h3>${safeName}<span class="status ${statusClass}">${ch.status || 'UNKNOWN'}</span></h3>
          <div class="url-box">${escapeHtml(fullUrl)}</div>
          <div class="btn-group">
            <button class="btn" onclick="copyToClipboard(this.dataset.url)" data-url="${escapeAttr(fullUrl)}">Copy M3U8</button>
            <button class="btn btn-play" onclick="playStream(this.dataset.url)" data-url="${escapeAttr(fullUrl)}">Play</button>
            <a href="${escapeAttr(fullUrl)}" target="_blank" class="btn btn-open" rel="noopener">Open</a>
          </div>
        </div>`;
    }).join('');
  } catch (err) {
    document.getElementById('sys-status').innerText = 'Degraded / API Error';
    document.getElementById('sys-status').style.color = '#e57373';
  }
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[c]);
}
function escapeAttr(s) { return escapeHtml(s); }

async function copyToClipboard(text) {
  await navigator.clipboard.writeText(text);
}

let hls = null;
function playStream(url) {
  const modal = document.getElementById('player-modal');
  const video = document.getElementById('video-player');
  modal.style.display = 'flex';

  if (window.Hls && Hls.isSupported()) {
    if (hls) hls.destroy();
    hls = new Hls({maxBufferLength:30, liveSyncDuration:3});
    hls.loadSource(url);
    hls.attachMedia(video);
    hls.on(Hls.Events.MANIFEST_PARSED, () => video.play().catch(() => {}));
    return;
  }

  if (video.canPlayType('application/vnd.apple.mpegurl')) {
    video.src = url;
    video.play().catch(() => {});
  }
}

function closePlayer() {
  const modal = document.getElementById('player-modal');
  const video = document.getElementById('video-player');
  if (hls) { hls.destroy(); hls = null; }
  video.pause();
  video.removeAttribute('src');
  video.load();
  modal.style.display = 'none';
}

fetchChannels();
setInterval(fetchChannels, 15000);
</script>
</body>
</html>
"""


@routes.get("/")
async def handle_root(_: web.Request) -> web.Response:
    return web.Response(text=HTML_TEMPLATE, content_type="text/html")


@routes.get("/health")
async def handle_health(_: web.Request) -> web.Response:
    configured = channel_manager.is_configured
    return web.json_response({
        "status": "ok" if configured else "degraded",
        "service": "livechannels-scraper",
        "workers": len(channel_manager.channels),
        "configured": configured,
    })


@routes.get("/api/channels")
async def handle_api_channels(_: web.Request) -> web.Response:
    return web.json_response({
        "success": True,
        "channels": [
            {
                "id": ch.id,
                "name": ch.name,
                "status": ch.health_status,
                "gatewayUrl": f"/stream/{ch.id}/master.m3u8",
                "sourceUrl": ch.url,
            }
            for ch in channel_manager.channels
        ],
    })


@routes.get("/stream/{channel_id}/master.m3u8")
async def handle_master_m3u8(request: web.Request) -> web.StreamResponse:
    cid = request.match_info["channel_id"]
    channel = channel_manager.get_channel(cid)
    if not channel:
        raise web.HTTPNotFound(text="Channel not found")

    encoded = request.query.get("u")
    target = decode_target(encoded) if encoded else channel.url

    if not safe_source_target(channel, target):
        raise web.HTTPForbidden(text="Source is not configured for this channel")

    try:
        headers = build_headers(channel)
        body, final_url = await request.app["hls_client"].fetch_text(target, headers)

        final_host = urlparse(final_url).hostname
        if final_host:
            channel_manager.register_observed_host(channel, final_url)

        if not is_hls_body(body):
            raise web.HTTPBadGateway(text="Configured source did not return an HLS playlist")

        manifest = rewrite_manifest(channel, final_url, body, origin_for(request))
        return web.Response(
            text=manifest,
            content_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"},
        )
    except AccessDeniedError as exc:
        raise web.HTTPBadGateway(text=str(exc)) from exc


@routes.get("/stream/{channel_id}/segment")
async def handle_segment(request: web.Request) -> web.Response:
    cid = request.match_info["channel_id"]
    encoded = request.query.get("u")
    channel = channel_manager.get_channel(cid)

    if not channel:
        raise web.HTTPNotFound(text="Channel not found")
    if not encoded:
        raise web.HTTPBadRequest(text="Missing segment URL")

    try:
        target = decode_target(encoded)
    except Exception as exc:
        raise web.HTTPBadRequest(text="Invalid segment URL") from exc

    if not safe_source_target(channel, target):
        raise web.HTTPForbidden(text="Segment source is not approved")

    data = await channel_manager.fetch_segment(cid, target)
    if not data:
        raise web.HTTPBadGateway(text="Upstream segment failure")

    content_type = (
        "video/mp2t"
        if target.lower().split("?", 1)[0].endswith(".ts")
        else "application/octet-stream"
    )
    return web.Response(
        body=data,
        content_type=content_type,
        headers={"Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"},
    )


async def init_app() -> web.Application:
    app = web.Application(client_max_size=8 * 1024 * 1024)
    app.add_routes(routes)

    timeout = float(os.getenv("UPSTREAM_TIMEOUT", "10"))
    retries = int(os.getenv("UPSTREAM_RETRIES", "4"))
    refresh_seconds = max(15, int(os.getenv("CONFIG_REFRESH_SECONDS", "60")))

    connector = aiohttp.TCPConnector(limit=50, limit_per_host=8, ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=connector)
    client = HLSClient(session, retries=retries, timeout=timeout)

    channel_manager.configure_client(client)
    await channel_manager.load_config()

    stop = asyncio.Event()
    tasks: dict[str, asyncio.Task] = {}

    def signature(channels: list[Channel]) -> dict[str, str]:
        return {ch.id: ch.url for ch in channels}

    async def reconcile_workers(force: bool = False) -> None:
        channels = channel_manager.channels
        wanted = signature(channels)

        for cid in list(tasks):
            if cid not in wanted:
                tasks[cid].cancel()
                await asyncio.gather(tasks[cid], return_exceptions=True)
                del tasks[cid]

        existing = {cid for cid in tasks}
        if force or existing != set(wanted):
            # For a changed catalog, recreate workers so source URLs/headers are current.
            for task in list(tasks.values()):
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            tasks.clear()

            for channel in channels:
                worker = ChannelWorker(channel, client, stop, False)
                tasks[channel.id] = asyncio.create_task(
                    worker.run(), name=f"channel:{channel.name}"
                )

    await reconcile_workers(force=True)

    async def refresh_loop() -> None:
        while not stop.is_set():
            await asyncio.sleep(refresh_seconds)
            try:
                before = signature(channel_manager.channels)
                await channel_manager.load_config()
                after = signature(channel_manager.channels)

                if before != after:
                    await reconcile_workers(force=True)
                    LOG.info("channel catalog changed; workers reconciled")
                else:
                    LOG.info("channel catalog refreshed; %d channel(s)", len(after))
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("channel catalog refresh failed")

    refresh_task = asyncio.create_task(refresh_loop(), name="channel-catalog-refresh")

    app["http_session"] = session
    app["hls_client"] = client
    app["stop_event"] = stop
    app["worker_tasks"] = tasks
    app["refresh_task"] = refresh_task

    async def cleanup(_: web.Application) -> None:
        stop.set()
        refresh_task.cancel()
        await asyncio.gather(refresh_task, return_exceptions=True)
        for task in list(tasks.values()):
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        await session.close()

    app.on_cleanup.append(cleanup)
    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    logger.info("Starting LiveChannels Gateway on port %s", port)
    web.run_app(init_app(), host="0.0.0.0", port=port)
