import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import httpx

# Ordered list of URL patterns to try for each year.
# Oracle's Elixir moved from public S3 to a login-gated CDN (datalisk.io) in late 2024.
# The S3 URLs are kept as fallback in case they are re-enabled.
# Place pre-downloaded CSVs in data/oracles_elixir/{year}.csv to bypass network entirely.
_URL_TEMPLATES = [
    # Original public S3 bucket (worked until ~late 2024)
    (
        "https://oracleselixir-downloadable-match-data.s3.us-east-2.amazonaws.com"
        "/{year}_LoL_esports_match_data_from_OraclesElixir.csv"
    ),
]

_CACHE_DIR = Path(__file__).parent.parent / "data" / "oracles_elixir"

_DOWNLOAD_TIMEOUT = 60  # seconds — CSVs can be large (2024 is ~80MB)

# OE uses lowercase abbreviated positions; Leaguepedia uses Title case full names
_POSITION_MAP = {
    "top": "Top",
    "jng": "Jungle",
    "mid": "Mid",
    "bot": "Bot",
    "sup": "Support",
    # Occasional variants seen in older years
    "jungle": "Jungle",
    "support": "Support",
    "adc": "Bot",
}

# OE does not have an absolute damage_taken field — only damagetakenperminute.
# damage_taken is computed as: round(damagetakenperminute * gamelength_seconds / 60)
# gamelength is in seconds in OE CSVs.


def _ensure_cache_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(year: int) -> Path:
    return _CACHE_DIR / f"{year}.csv"


def _find_local_csv(year: int) -> Optional[Path]:
    # Prefer the full OE filename (downloaded from the site) over the short cached name
    candidates = [
        _CACHE_DIR / f"{year}_LoL_esports_match_data_from_OraclesElixir.csv",
        _CACHE_DIR / f"{year}.csv",
    ]
    for path in candidates:
        if path.exists() and path.stat().st_size > 0:
            return path
    return None


def _normalize_position(raw: str) -> str:
    return _POSITION_MAP.get(raw.strip().lower(), raw.strip())


def _safe_int(value: str) -> int:
    try:
        return int(float(value)) if value and value.strip() else 0
    except (ValueError, TypeError):
        return 0


def _download_csv(year: int) -> Optional[bytes]:
    for url_template in _URL_TEMPLATES:
        url = url_template.format(year=year)
        print(f"[OE] Downloading {year} CSV from {url}")
        try:
            with httpx.Client(timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                # Reject HTML responses (e.g. login pages returned as 200)
                content_type = r.headers.get("content-type", "")
                if "text/html" in content_type:
                    print(f"[OE] Got HTML response for {year} from {url} — skipping (login gate?)")
                    continue
                return r.content
        except httpx.HTTPStatusError as e:
            print(f"[OE] HTTP error downloading {year} from {url}: {e.response.status_code} {e.response.reason_phrase}")
            continue
        except httpx.RequestError as e:
            print(f"[OE] Request error downloading {year} from {url}: {e}")
            continue
    print(f"[OE] All download URLs failed for {year}")
    return None


def _safe_float(value: str) -> float:
    try:
        return float(value) if value and value.strip() else 0.0
    except (ValueError, TypeError):
        return 0.0


def _parse_csv_bytes(data: bytes) -> Dict[str, List[Dict]]:
    """Parse raw CSV bytes into a dict keyed by OE gameid.

    Each gameid maps to a list of participant dicts (one per player row).
    Team-level summary rows (position == 'team' or empty playername) are skipped.
    """
    result: Dict[str, List[Dict]] = {}

    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    for row in reader:
        gameid = row.get("gameid", "").strip()
        if not gameid:
            continue

        position_raw = row.get("position", "").strip().lower()
        if position_raw in ("team", ""):
            continue

        playername = row.get("playername", "").strip()
        if not playername:
            continue

        gamelength = _safe_float(row.get("gamelength", ""))
        dtpm = _safe_float(row.get("damagetakenperminute", ""))
        # damage_taken is not stored as an absolute value in OE — derive it
        damage_taken = round(dtpm * gamelength / 60) if gamelength > 0 else 0

        participant = {
            "gameid": gameid,
            "date": row.get("date", "").strip(),         # "2024-06-08 18:55:00"
            "date_day": row.get("date", "")[:10],         # "2024-06-08"
            "league": row.get("league", "").strip(),
            "teamname": row.get("teamname", "").strip(),
            "playername": playername,
            "position": _normalize_position(position_raw),
            "champion": row.get("champion", "").strip(),
            "damage_taken": damage_taken,
            "wardsplaced": _safe_int(row.get("wardsplaced", "")),
            "wardskilled": _safe_int(row.get("wardskilled", "")),
            "damagetochampions": _safe_int(row.get("damagetochampions", "")),
        }

        result.setdefault(gameid, []).append(participant)

    return result


def load_year(year: int) -> Dict[str, List[Dict]]:
    """Download (if not cached) and parse Oracle's Elixir CSV for a single year.

    Returns a dict of gameid -> list of participant dicts.
    Returns an empty dict if the download fails or the file is empty.
    """
    _ensure_cache_dir()
    local = _find_local_csv(year)

    if local:
        print(f"[OE] Using local file for {year}: {local.name} ({local.stat().st_size // 1024} KB)")
        data = local.read_bytes()
    else:
        data = _download_csv(year)
        if not data:
            print(f"[OE] Skipping {year} — no local file and download failed")
            return {}
        _cache_path(year).write_bytes(data)
        print(f"[OE] Cached {year} CSV ({len(data) // 1024} KB)")

    parsed = _parse_csv_bytes(data)
    print(f"[OE] Parsed {year}: {len(parsed)} unique gameids, "
          f"{sum(len(v) for v in parsed.values())} participant rows")
    return parsed


def load_all_years(years: Optional[List[int]] = None) -> Dict[str, List[Dict]]:
    """Load Oracle's Elixir data for multiple years, merging into a single dict.

    Args:
        years: list of years to load. If None, loads 2014 through the current year.

    Returns:
        Merged dict of gameid -> list of participant dicts across all years.
        Later years overwrite earlier years on gameid collision (should not occur in practice).
    """
    if years is None:
        current_year = datetime.now().year
        years = list(range(2014, current_year + 1))

    print(f"[OE] Loading years: {years}")
    merged: Dict[str, List[Dict]] = {}

    for year in years:
        year_data = load_year(year)
        # In practice gameids are globally unique, but log if a collision occurs
        collisions = [gid for gid in year_data if gid in merged]
        if collisions:
            print(f"[OE] Warning: {len(collisions)} gameid collisions merging year {year} — overwriting")
        merged.update(year_data)

    print(f"[OE] Total loaded: {len(merged)} unique gameids across {len(years)} years")
    return merged
