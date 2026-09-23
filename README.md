# livechannels.scraper

Python continuous HLS/M3U8 channel ingester for streams you are authorized to access.

## Features

- Resolves HLS master playlists to a media playlist.
- Continuously polls live media playlists.
- Downloads newly available TS segments in sequence.
- Retries transient network and upstream errors with bounded exponential backoff and jitter.
- Preserves explicitly configured request headers such as Referer and Origin.
- Deduplicates overlapping live-playlist segments.
- Writes individual segments and optionally a rolling live.ts file.
- Runs multiple channels concurrently without one slow source blocking others.
- Treats access-control responses as source errors and does not attempt to circumvent them.
- Does not decrypt protected HLS.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp channels.example.json channels.json
```

Edit `channels.json` with streams you are authorized to access.

## Run

```bash
python src/scraper.py --config channels.json --output data --append-ts
```

Output is written per channel under `data/` and can include individual segments plus `live.ts`.

## Reliability

Transient failures are retried. Failed segments remain eligible during the next live-playlist refresh. Overlapping HLS windows are deduplicated. Live playlists are polled based on `#EXT-X-TARGETDURATION`.

For production use, keep credentials outside Git, use a source-approved refresh mechanism for expiring URLs, and do not expose an unrestricted arbitrary-URL proxy.
