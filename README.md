# livechannels.scraper

Python continuous HLS/M3U8 channel ingester for streams you are authorized to access.

## What is in this repo

- `src/scraper.py` — core continuous HLS/M3U8 -> TS worker.
- `src/service.py` — production dashboard, controlled HLS gateway, remote catalog loader, and Universal URL Scraper API.
- `src/universal.py` — generic public URL -> HLS/M3U/M3U8 discovery engine with HTML, script, media-tag, playlist, and iframe scanning.
- `Dockerfile` — used automatically by Railway and supported by Render.
- `railway.json` — Railway config with Docker build, health check, and automatic restart policy.
- `render.yaml` — Render Web Service configuration for the dashboard/API.
- `channels.example.json` — safe configuration template; real stream URLs should not be committed.

## Deploy to Railway

1. Push this repository to GitHub (already done).
2. In Railway, create a new project and choose **Deploy from GitHub Repo**.
3. Select `nexus-appshub/livechannels.scraper`.
4. Railway will detect the root `Dockerfile` and build it automatically.
5. Add the environment variable:

```text
CHANNELS_JSON=<your channel configuration JSON>
REMOTE_CONFIG_URL=<authorized/public JSON or M3U catalog URL>
REMOTE_CONFIG_TIMEOUT=15
```

You can also set:

```text
OUTPUT_DIR=data
APPEND_TS=false
UPSTREAM_TIMEOUT=10
UPSTREAM_RETRIES=4
LOG_LEVEL=INFO
```

6. Deploy.

The service starts:

```text
python src/service.py
```

Health endpoint:

```text
/health
```

Railway is configured to use the container's `PORT`, run the health check, and restart the service when configured by the project plan.

Railway currently supports GitHub autodeploys and Dockerfile-based deployments. See the official Railway deployment documentation for the current dashboard flow. 

## Deploy to Render

The included `render.yaml` is a **Web Service** because the project exposes a public dashboard and API. For long-running scraping, use a paid service/plan appropriate for continuous execution and persistent storage where needed.

1. In Render, connect GitHub.
2. Select this repository.
3. Use the Blueprint from `render.yaml`, or create a **Background Worker** manually.
4. Build command:

```text
pip install -r requirements.txt
```

5. Start command:

```text
python src/service.py
```

6. Add:

```text
CHANNELS_JSON=<your channel configuration JSON>
```

Optional:

```text
OUTPUT_DIR=data
APPEND_TS=false
UPSTREAM_TIMEOUT=10
UPSTREAM_RETRIES=4
LOG_LEVEL=INFO
```

## Channel configuration

Example:

```json
{
  "channels": [
    {
      "name": "Example Channel",
      "url": "https://YOUR-AUTHORIZED-HOST.example/live/master.m3u8",
      "enabled": true,
      "headers": {
        "Referer": "https://YOUR-AUTHORIZED-WEB-APP.example/",
        "Origin": "https://YOUR-AUTHORIZED-WEB-APP.example"
      },
      "poll_seconds": 2,
      "max_bandwidth": 8000000
    }
  ]
}
```

If `channels.json` is not present, `src/scraper.py` and `src/service.py` read `CHANNELS_JSON` from the environment.

For a remotely managed catalog, set `REMOTE_CONFIG_URL` to an authorized/public JSON or M3U catalog. The service fetches it at startup when no local/configured channel list is available. JSON should contain a `channels` array; simple M3U catalogs with `#EXTINF` entries are also accepted.

Do not put credentials, private cookies, bearer tokens, or other secrets into Git.

## Universal URL Scraper

Open the deployed website and choose **Universal URL Scraper**. Enter a public target URL and press **Deep Scan**. The results are automatically shown in **Streams, M3U8 & Logs** with per-stream Copy, Play, and Open actions, plus bulk M3U export.

The API is:

```text
POST /api/deep-scrape
Content-Type: application/json

{"url":"https://example.com/playlist.m3u8"}
```

The scanner supports public M3U/M3U8 playlists, HTML media/source tags, common player configuration patterns, external script URLs, and one level of iframe crawling. It rejects private/local network targets and does not bypass authentication, DRM, signed URLs, anti-bot challenges, or other access controls.

Optional environment variables:

```text
UNIVERSAL_TIMEOUT=12
UNIVERSAL_MAX_IFRAMES=10
UNIVERSAL_MAX_SCRIPTS=12
CONFIG_REFRESH_SECONDS=60
```

## Continuous TS behavior

The worker repeatedly:

```text
M3U8
  -> refresh live playlist
  -> detect new media sequence numbers
  -> resolve segment URLs
  -> download new TS segments
  -> deduplicate
  -> repeat
```

Transient failures use bounded retries and exponential backoff. A single failed upstream request does not terminate the entire service.

The scraper does not attempt to defeat DRM, authentication, signed access controls, or anti-bot/hotlink protection.

## Storage warning

Container/local filesystems are not durable across every platform restart/redeploy unless you configure persistent storage. If you need an uninterrupted historical TS archive, use a platform volume or object storage and add retention/rotation instead of allowing `live.ts` or individual segments to grow without limit.

## Local development

```bash
pip install -r requirements.txt
cp channels.example.json channels.json
python src/scraper.py --config channels.json --output data --append-ts
```
