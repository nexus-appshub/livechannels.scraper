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
from universal import UniversalScraper

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
            channel_manager.register_observed_host(channel, absolute)
            if not safe_observed_child(channel, absolute):
                raise web.HTTPBadGateway(
                    text="HLS child playlist host is not an approved source host"
                )
            item.uri = f"{base}/stream/{cid}/master.m3u8?u={encode_target(absolute)}"
    else:
        for index, segment in enumerate(playlist.segments):
            absolute = urljoin(final_url, segment.uri)
            channel_manager.register_observed_host(channel, absolute)
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
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LiveChannels Scraper</title>
<script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
<style>
:root { --bg:#0b1120; --panel:#111827; --panel2:#0f172a; --line:#243047; --text:#e5e7eb; --muted:#94a3b8; --accent:#6366f1; --good:#22c55e; --warn:#f59e0b; --bad:#ef4444; }
* { box-sizing:border-box; scrollbar-width:none; } *::-webkit-scrollbar { display:none; width:0; height:0; }
body { margin:0; background:radial-gradient(circle at top,#101b33,#070b14 55%); color:var(--text); font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif; min-height:100vh; }
button,input { font:inherit; } .container { max-width:1180px; margin:0 auto; padding:22px; }
.header { display:flex; align-items:center; justify-content:space-between; gap:16px; padding:6px 0 18px; border-bottom:1px solid var(--line); }
.brand h1 { margin:0; font-size:23px; } .brand p { margin:6px 0 0; color:var(--muted); font-size:13px; }
.badges { display:flex; gap:8px; flex-wrap:wrap; } .badge { padding:6px 10px; border:1px solid var(--line); border-radius:999px; background:#0d1628; color:#cbd5e1; font-size:12px; }
.tabs { display:flex; gap:8px; overflow-x:auto; padding:16px 0 10px; border-bottom:1px solid var(--line); }
.tab { border:1px solid transparent; background:transparent; color:var(--muted); padding:9px 13px; border-radius:9px; cursor:pointer; font-weight:700; font-size:13px; white-space:nowrap; }
.tab.active { color:#fff; background:#17203a; border-color:#334155; }
.view { display:none; padding-top:18px; } .view.active { display:block; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:14px; }
.card,.panel { background:rgba(15,23,42,.88); border:1px solid var(--line); border-radius:14px; padding:16px; box-shadow:0 12px 40px rgba(0,0,0,.25); }
.channel-title { display:flex; justify-content:space-between; gap:10px; align-items:center; font-weight:800; }
.status { font-size:10px; letter-spacing:.08em; padding:4px 7px; border-radius:999px; border:1px solid #334155; }
.status.healthy,.status.starting { color:#86efac; border-color:#166534; background:rgba(22,101,52,.18); }
.status.degraded { color:#fde68a; border-color:#92400e; background:rgba(146,64,14,.18); }
.status.down { color:#fca5a5; border-color:#991b1b; background:rgba(153,27,27,.18); }
.url { margin-top:12px; background:#050a13; border:1px solid #1f2937; border-radius:9px; padding:10px; color:#a7f3d0; font:11px ui-monospace,SFMono-Regular,Menlo,monospace; overflow:auto; white-space:nowrap; }
.actions { display:flex; gap:8px; margin-top:11px; flex-wrap:wrap; }
.btn { border:1px solid #334155; background:#172033; color:#e5e7eb; border-radius:8px; padding:8px 11px; cursor:pointer; font-weight:700; font-size:12px; text-decoration:none; }
.btn:hover { border-color:#64748b; background:#202b43; } .btn.primary { background:#4f46e5; border-color:#6366f1; color:#fff; } .btn.success { background:#166534; border-color:#15803d; color:#fff; }
.form-row { display:flex; gap:9px; } .form-row input { flex:1; min-width:0; border:1px solid #334155; background:#0b1220; color:#fff; border-radius:9px; padding:12px 13px; outline:none; }
.form-row input:focus { border-color:#6366f1; box-shadow:0 0 0 3px rgba(99,102,241,.13); }
.hint { color:var(--muted); font-size:12px; line-height:1.5; } .section-head { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:12px; } .section-head h2 { margin:0; font-size:15px; }
.toolbar { display:flex; gap:8px; flex-wrap:wrap; } .filter { width:100%; border:1px solid #253149; background:#0a1020; color:#fff; padding:10px 12px; border-radius:8px; outline:none; }
.table-wrap { overflow:auto; border:1px solid var(--line); border-radius:10px; margin-top:12px; } table { width:100%; border-collapse:collapse; font-size:12px; min-width:820px; }
th,td { padding:10px; border-bottom:1px solid #1f2937; vertical-align:top; text-align:left; } th { position:sticky; top:0; background:#0c1322; color:#94a3b8; font-size:10px; text-transform:uppercase; letter-spacing:.08em; }
td.url-cell { color:#a7f3d0; word-break:break-all; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; } .empty { padding:36px 18px; text-align:center; color:var(--muted); border:1px dashed #334155; border-radius:10px; }
.log { background:#050a12; border:1px solid #1f2937; border-radius:10px; padding:12px; height:220px; overflow:auto; font:11px ui-monospace,SFMono-Regular,Menlo,monospace; color:#94a3b8; }
.log p { margin:0 0 7px; } .log .ok { color:#86efac; } .log .err { color:#fca5a5; } .log .work { color:#fde68a; }
#player { position:fixed; inset:0; background:rgba(0,0,0,.92); display:none; align-items:center; justify-content:center; padding:20px; z-index:100; } .player-card { width:min(100%,960px); }
video { width:100%; aspect-ratio:16/9; background:#000; border-radius:12px; } .small { color:#64748b; font-size:11px; }
@media(max-width:700px){ .container{padding:14px}.header{align-items:flex-start;flex-direction:column}.form-row{flex-direction:column}.btn{padding:9px 10px} }
</style>
</head>
<body>
<div class="container">
<header class="header"><div class="brand"><h1>LiveChannels Scraper</h1><p>Universal URL discovery + HLS/M3U8 channel gateway</p></div>
<div class="badges"><span class="badge" id="sysBadge">Online</span><span class="badge">Channels: <strong id="channelCount">0</strong></span><span class="badge">Discovered: <strong id="discoveredCount">0</strong></span></div></header>

<nav class="tabs">
<button class="tab active" data-tab="channels">Channels</button>
<button class="tab" data-tab="extractor">Universal URL Scraper</button>
<button class="tab" data-tab="streams">Streams, M3U8 &amp; Logs</button>
</nav>

<section id="view-channels" class="view active"><div class="panel"><div class="section-head"><div><h2>Configured / Remote Channels</h2><div class="hint">Channels load automatically from REMOTE_CONFIG_URL / CHANNELS_URL, channels.json, or CHANNELS_JSON.</div></div><button class="btn" onclick="loadChannels()">Refresh</button></div><div id="channelGrid" class="grid"><div class="empty">Loading channels...</div></div></div></section>

<section id="view-extractor" class="view"><div class="panel"><div class="section-head"><div><h2>Universal URL Scraper</h2><div class="hint">Enter a public website, M3U/M3U8 playlist, or embed page. The scanner checks exposed media URLs, source/video tags, player config, scripts and one level of iframes.</div></div></div>
<form id="scrapeForm"><div class="form-row"><input id="targetUrl" type="url" required placeholder="https://example.com/live or https://example.com/playlist.m3u8"><button id="scrapeBtn" class="btn primary" type="submit">Deep Scan</button></div></form>
<p class="small" style="margin:10px 0 0">Protected/authenticated/DRM-protected media is not bypassed.</p></div></section>

<section id="view-streams" class="view"><div class="panel"><div class="section-head"><div><h2>Discovered Streams (<span id="streamHeadingCount">0</span>)</h2><div class="hint">Copy the extracted M3U8/media URL or open/play it in the browser.</div></div>
<div class="toolbar"><button class="btn primary" onclick="copyAll()">Copy M3U8 List</button><button class="btn" onclick="downloadM3U()">Download .m3u</button><button class="btn" onclick="clearResults()">Clear</button></div></div>
<input id="streamFilter" class="filter" placeholder="Filter by channel name or URL..." oninput="renderResults()">
<div class="table-wrap"><table><thead><tr><th>#</th><th>Name</th><th>Category</th><th>Stream URL</th><th>Actions</th></tr></thead><tbody id="resultsBody"><tr><td colspan="5"><div class="empty">Run a Deep Scan first.</div></td></tr></tbody></table></div></div>
<div class="panel" style="margin-top:14px"><div class="section-head"><h2>Operation Logs</h2><button class="btn" onclick="clearLogs()">Clear Logs</button></div><div id="logBox" class="log"><p>&gt; Universal scanner ready.</p></div></div></section>
</div>
<div id="player"><div class="player-card"><video id="video" controls playsinline></video><div class="actions" style="justify-content:flex-end"><button class="btn" onclick="closePlayer()">Close Player</button></div></div></div>

<script>
let discovered=[]; let hls=null;
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);}
function log(msg,type='info'){const box=document.getElementById('logBox');const p=document.createElement('p');p.className=type==='success'?'ok':type==='error'?'err':type==='working'?'work':'';p.textContent='> ['+new Date().toLocaleTimeString()+'] '+msg;box.appendChild(p);box.scrollTop=box.scrollHeight;}
function clearLogs(){document.getElementById('logBox').innerHTML='<p>&gt; Logs cleared.</p>';}
function clearResults(){discovered=[];document.getElementById('streamFilter').value='';renderResults();}
function switchTab(tab){document.querySelectorAll('.tab').forEach(b=>b.classList.toggle('active',b.dataset.tab===tab));document.querySelectorAll('.view').forEach(v=>v.classList.toggle('active',v.id==='view-'+tab));}
document.querySelectorAll('.tab').forEach(b=>b.addEventListener('click',()=>switchTab(b.dataset.tab)));

async function loadChannels(){try{const refresh=await fetch('/api/config/refresh',{method:'POST'});const refreshData=await refresh.json();if(!refresh.ok||!refreshData.success){log('Remote catalog refresh failed: '+(refreshData.detail||'unknown error'),'error');}const r=await fetch('/api/channels',{cache:'no-store'});const data=await r.json();const list=data.channels||[];document.getElementById('channelCount').innerText=list.length;const grid=document.getElementById('channelGrid');if(!list.length){grid.innerHTML='<div class="empty">No configured channels. Set REMOTE_CONFIG_URL, CHANNELS_URL, CHANNELS_CONFIG_URL, or CHANNELS_JSON.</div>';if(refreshData.lastError)log('Config error: '+refreshData.lastError,'error');return;}
grid.innerHTML=list.map(ch=>{const u=new URL(ch.gatewayUrl,location.origin).href;return '<div class="card"><div class="channel-title"><span>'+esc(ch.name)+'</span><span class="status '+esc((ch.status||'unknown').toLowerCase())+'">'+esc(ch.status||'UNKNOWN')+'</span></div><div class="url">'+esc(u)+'</div><div class="actions"><button class="btn primary" onclick="copyText('+JSON.stringify(u)+')">Copy M3U8</button><button class="btn success" onclick="play('+JSON.stringify(u)+')">Play</button><a class="btn" href="'+esc(u)+'" target="_blank" rel="noopener">Open</a></div></div>';}).join('');
}catch(e){document.getElementById('sysBadge').innerText='API Error';log('Channel API error: '+e.message,'error');}}

async function copyText(value){try{await navigator.clipboard.writeText(value);log('Copied URL.','success');}catch{window.prompt('Copy URL',value);}}
function renderResults(){const q=(document.getElementById('streamFilter').value||'').toLowerCase().trim();const rows=discovered.filter(x=>!q||String(x.name||'').toLowerCase().includes(q)||String(x.url||'').toLowerCase().includes(q));document.getElementById('discoveredCount').innerText=discovered.length;document.getElementById('streamHeadingCount').innerText=q?(rows.length+' / '+discovered.length):rows.length;const body=document.getElementById('resultsBody');if(!rows.length){body.innerHTML='<tr><td colspan="5"><div class="empty">No matching streams.</div></td></tr>';return;}
body.innerHTML=rows.map((c,i)=>{const url=String(c.url||'');const playable=/\.(m3u8|mp4)(\?|$)/i.test(url)||/\/hls\/|\/stream\//i.test(url);return '<tr><td>'+(i+1)+'</td><td><strong>'+esc(c.name||'Live Stream')+'</strong><div class="small">'+esc(c.sourcePage||'')+'</div></td><td>'+esc(c.category||'Live')+'</td><td class="url-cell">'+esc(url)+'</td><td><div class="actions" style="margin:0">'+(playable?'<button class="btn success btn-play" data-url="'+esc(url)+'">Play</button>':'')+'<button class="btn primary btn-copy" data-url="'+esc(url)+'">Copy</button><a class="btn" href="'+esc(url)+'" target="_blank" rel="noopener">Open</a></div></td></tr>';}).join('');
body.querySelectorAll('.btn-copy').forEach(b=>b.addEventListener('click',()=>copyText(b.dataset.url)));body.querySelectorAll('.btn-play').forEach(b=>b.addEventListener('click',()=>play(b.dataset.url)));}

async function copyAll(){const rows=discovered.map(c=>c.url).filter(Boolean);if(!rows.length)return log('No discovered URLs to copy.','error');await copyText(rows.join('\n'));}
function downloadM3U(){if(!discovered.length)return log('No discovered streams to export.','error');const text='#EXTM3U\n'+discovered.map(c=>'#EXTINF:-1,'+(c.name||'Live Stream')+'\n'+c.url).join('\n')+'\n';const blob=new Blob([text],{type:'audio/x-mpegurl'});const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='discovered-streams.m3u';a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);log('Downloaded M3U playlist.','success');}

document.getElementById('scrapeForm').addEventListener('submit',async e=>{e.preventDefault();const target=document.getElementById('targetUrl').value.trim();const btn=document.getElementById('scrapeBtn');btn.disabled=true;btn.innerText='Scanning...';switchTab('streams');log('Starting deep scan: '+target,'working');
try{const r=await fetch('/api/deep-scrape',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:target})});const data=await r.json();if(!r.ok||!data.success)throw new Error(data.detail||'Scraping failed');discovered=data.channels||[];renderResults();(data.logs||[]).forEach(x=>log(x,'info'));log('Completed. Found '+discovered.length+' stream(s).',discovered.length?'success':'error');switchTab('streams');}catch(err){log('Scan error: '+err.message,'error');}finally{btn.disabled=false;btn.innerText='Deep Scan';}});
function play(url){const modal=document.getElementById('player'),video=document.getElementById('video');modal.style.display='flex';if(hls){hls.destroy();hls=null;}video.removeAttribute('src');video.load();if(/\.m3u8(\?|$)/i.test(url)&&window.Hls&&Hls.isSupported()){hls=new Hls({liveSyncDuration:3,maxBufferLength:30});hls.loadSource(url);hls.attachMedia(video);hls.on(Hls.Events.MANIFEST_PARSED,()=>video.play().catch(()=>{}));}else{video.src=url;video.play().catch(()=>{});}}
function closePlayer(){const video=document.getElementById('video');if(hls){hls.destroy();hls=null;}video.pause();video.removeAttribute('src');video.load();document.getElementById('player').style.display='none';}
loadChannels();setInterval(loadChannels,15000);
</script>
</body></html>
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
        "channelCount": len(channel_manager.channels),
        "configSource": channel_manager.config_source,
        "remoteConfigConfigured": channel_manager.remote_config_configured,
        "configError": channel_manager.last_config_error,
    })

@routes.get("/api/config-status")
async def handle_config_status(_: web.Request) -> web.Response:
    return web.json_response({
        "success": True,
        "channelCount": len(channel_manager.channels),
        "configSource": channel_manager.config_source,
        "remoteConfigConfigured": channel_manager.remote_config_configured,
        "lastError": channel_manager.last_config_error,
        "lastFetchAgeSeconds": (
            max(0.0, asyncio.get_running_loop().time() - channel_manager.last_config_at)
            if channel_manager.last_config_at is not None else None
        ),
    })

@routes.post("/api/config/refresh")
async def handle_config_refresh(request: web.Request) -> web.Response:
    try:
        before = {ch.name: (ch.url, tuple(sorted(ch.headers.items()))) for ch in channel_manager.channels}
        await channel_manager.load_config()
        after = {ch.name: (ch.url, tuple(sorted(ch.headers.items()))) for ch in channel_manager.channels}
        refresh_workers = request.app.get("refresh_channels")
        if refresh_workers is not None and before != after:
            await refresh_workers()
        return web.json_response({
            "success": True,
            "channelCount": len(channel_manager.channels),
            "configSource": channel_manager.config_source,
            "lastError": channel_manager.last_config_error,
        })
    except Exception as exc:
        logger.exception("manual channel catalog refresh failed")
        return web.json_response({"success": False, "detail": str(exc)}, status=502)


@routes.post("/api/deep-scrape")
async def handle_deep_scrape(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text="Invalid JSON body") from exc

    target_url = str(payload.get("url") or "").strip()
    if not target_url:
        raise web.HTTPBadRequest(text="Valid target URL is required")

    scraper: UniversalScraper = request.app["universal_scraper"]
    lock: asyncio.Lock = request.app["universal_scrape_lock"]

    if lock.locked():
        raise web.HTTPTooManyRequests(
            text="Another universal scan is already running; please retry shortly"
        )

    async with lock:
        try:
            channels, logs = await scraper.scan(target_url)
        except PermissionError as exc:
            return web.json_response({"success": False, "detail": str(exc), "channels": []}, status=502)
        except ValueError as exc:
            return web.json_response({"success": False, "detail": str(exc), "channels": []}, status=400)
        except Exception as exc:
            logger.exception("Universal deep-scrape failed")
            return web.json_response({"success": False, "detail": str(exc), "channels": []}, status=502)

    return web.json_response({
        "success": True,
        "target": target_url,
        "count": len(channels),
        "channels": [item.as_json() for item in channels[:500]],
        "logs": logs,
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
    universal_scraper = UniversalScraper(
        session,
        timeout=float(os.getenv("UNIVERSAL_TIMEOUT", "12")),
        retries=max(1, retries // 2),
        max_iframes=max(0, int(os.getenv("UNIVERSAL_MAX_IFRAMES", "10"))),
        max_scripts=max(0, int(os.getenv("UNIVERSAL_MAX_SCRIPTS", "12"))),
    )

    try:
        channel_manager.configure_client(client)
        await channel_manager.load_config()
    except Exception:
        await session.close()
        raise

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
                    logger.info("channel catalog changed; workers reconciled")
                else:
                    logger.info("channel catalog refreshed; %d channel(s)", len(after))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("channel catalog refresh failed")

    refresh_task = asyncio.create_task(refresh_loop(), name="channel-catalog-refresh")

    async def refresh_channels_now() -> None:
        before = {ch.name: ch.url for ch in channel_manager.channels}
        await channel_manager.load_config()
        after = {ch.name: ch.url for ch in channel_manager.channels}
        if before != after:
            await reconcile_workers(force=True)

    app["http_session"] = session
    app["hls_client"] = client
    app["stop_event"] = stop
    app["worker_tasks"] = tasks
    app["refresh_task"] = refresh_task
    app["refresh_channels"] = refresh_channels_now

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
