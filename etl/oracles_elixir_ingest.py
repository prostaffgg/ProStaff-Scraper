#!/usr/bin/env python3
"""
Oracle's Elixir Full Ingest

Creates ES documents in lol_pro_matches for all leagues from OE CSV files.
Each gameid becomes one document (one game, not a series).

Uses op_type='create' — re-runs are safe (duplicate gameids are skipped).

Default skip list: CBLOL, LTA S — already indexed via Leaguepedia.
"""
import argparse
import csv
import io
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from indexers.elasticsearch_client import bulk_index  # noqa: E402

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/oracles_elixir_ingest.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

INDEX = os.getenv("INDEX_NAME", "lol_pro_matches")
BATCH_SIZE = 200

_CACHE_DIR = Path(__file__).parent.parent / "data" / "oracles_elixir"

# OE abbreviated positions → Title case (matches existing CBLOL docs in ES)
_POSITION_MAP = {
    "top": "Top",
    "jng": "Jungle",
    "mid": "Mid",
    "bot": "Bot",
    "sup": "Support",
    "jungle": "Jungle",
    "support": "Support",
    "adc": "Bot",
}


# ─── helpers ────────────────────────────────────────────────────────────────


def _safe_int(value: str) -> int:
    try:
        return int(float(value)) if value and value.strip() else 0
    except (ValueError, TypeError):
        return 0


def _safe_float(value: str) -> float:
    try:
        return float(value) if value and value.strip() else 0.0
    except (ValueError, TypeError):
        return 0.0


def _normalize_role(raw: str) -> str:
    return _POSITION_MAP.get(raw.strip().lower(), raw.strip().title())


def _find_local_csv(year: int) -> Optional[Path]:
    candidates = [
        _CACHE_DIR / f"{year}_LoL_esports_match_data_from_OraclesElixir.csv",
        _CACHE_DIR / f"{year}.csv",
    ]
    for path in candidates:
        if path.exists() and path.stat().st_size > 0:
            return path
    return None


# ─── parsing ────────────────────────────────────────────────────────────────


def _parse_participant_row(row: Dict, gamelength_s: float) -> Dict:
    dtpm = _safe_float(row.get("damagetakenperminute", ""))
    damage_taken = round(dtpm * gamelength_s / 60) if gamelength_s > 0 else 0
    gamelength_min = gamelength_s / 60 if gamelength_s > 0 else 0.0

    cs = _safe_int(row.get("total cs", "") or row.get("totalcs", ""))
    cs_per_min = round(cs / gamelength_min, 2) if gamelength_min > 0 else 0.0
    damage = _safe_int(row.get("damagetochampions", ""))
    dpm = _safe_float(row.get("dpm", ""))

    return {
        "summoner_name": row.get("playername", "").strip(),
        "team_name": row.get("teamname", "").strip(),
        "role": _normalize_role(row.get("position", "")),
        "champion_name": row.get("champion", "").strip(),
        "win": row.get("result", "0") == "1",
        "kills": _safe_int(row.get("kills", "")),
        "deaths": _safe_int(row.get("deaths", "")),
        "assists": _safe_int(row.get("assists", "")),
        "gold": _safe_int(row.get("totalgold", "")),
        "cs": cs,
        "cs_per_min": cs_per_min,
        "damage": damage,
        "damage_per_min": round(dpm, 2),
        "damage_taken": damage_taken,
        "vision_score": _safe_int(row.get("visionscore", "")),
        "wards_placed": _safe_int(row.get("wardsplaced", "")),
        "wards_killed": _safe_int(row.get("wardskilled", "")),
        "items": [],
        "keystone": "",
        "primary_runes": [],
        "primary_tree": "",
        "secondary_tree": "",
        "secondary_runes": [],
        "summoner_spells": [],
    }


def _parse_csv_to_games(
    data: bytes,
    skip_leagues: Set[str],
    filter_leagues: Optional[Set[str]],
) -> Dict[str, Dict]:
    """Parse raw CSV bytes into a per-gameid dict of {meta, team_rows, player_rows}."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    games: Dict[str, Dict] = defaultdict(lambda: {"meta": None, "team_rows": [], "player_rows": []})

    for row in reader:
        gameid = row.get("gameid", "").strip()
        if not gameid:
            continue

        league = row.get("league", "").strip()
        league_norm = league.lower()

        if league_norm in skip_leagues:
            continue
        if filter_leagues and league_norm not in filter_leagues:
            continue

        position_raw = row.get("position", "").strip().lower()
        game = games[gameid]

        # Capture game-level metadata once from the first row
        if game["meta"] is None:
            gamelength_s = _safe_float(row.get("gamelength", ""))
            date_str = row.get("date", "").strip()
            split_raw = row.get("split", "").strip()
            year = row.get("year", "").strip()
            patch = row.get("patch", "").strip()
            playoffs = row.get("playoffs", "0").strip() in ("1", "True", "true")

            if date_str:
                iso = date_str.replace(" ", "T")
                start_time = iso if iso.endswith("Z") else iso + "Z"
            else:
                start_time = ""

            game["meta"] = {
                "league": league,
                "year": year,
                "patch": patch,
                "start_time": start_time,
                "game_duration_seconds": int(gamelength_s),
                "tournament_name": f"{league} {year} {split_raw}".strip(),
                "stage": "Playoffs" if playoffs else "Regular Season",
                "split_event": split_raw,
            }

        if position_raw in ("team", ""):
            game["team_rows"].append(row)
        else:
            game["player_rows"].append(row)

    return games


def _build_doc(gameid: str, game_data: Dict) -> Optional[Dict]:
    meta = game_data["meta"]
    if not meta:
        return None

    team_rows = game_data["team_rows"]
    player_rows = game_data["player_rows"]

    if len(team_rows) < 2 or len(player_rows) < 2:
        return None

    # Blue side = team1, Red side = team2
    blue = next((r for r in team_rows if r.get("side", "").lower() == "blue"), None)
    red = next((r for r in team_rows if r.get("side", "").lower() == "red"), None)

    # Fallback: sort by participantid (100=blue, 200=red)
    if blue is None or red is None:
        sorted_teams = sorted(team_rows, key=lambda r: _safe_int(r.get("participantid", "0")))
        if len(sorted_teams) >= 2:
            blue, red = sorted_teams[0], sorted_teams[1]
        else:
            return None

    blue_name = blue.get("teamname", "").strip()
    red_name = red.get("teamname", "").strip()
    blue_won = blue.get("result", "0") == "1"

    winner_name = blue_name if blue_won else red_name

    gamelength_s = meta["game_duration_seconds"]
    participants = []
    for row in player_rows:
        p = _parse_participant_row(row, gamelength_s)
        if p["summoner_name"]:
            participants.append(p)

    return {
        "_id": gameid,
        "match_id": gameid,
        "league": meta["league"],
        "year": meta["year"],
        "patch": meta["patch"],
        "start_time": meta["start_time"],
        "game_duration_seconds": gamelength_s,
        "gamelength": gamelength_s,
        "tournament_name": meta["tournament_name"],
        "stage": meta["stage"],
        "split_event": meta["split_event"],
        "team1": {
            "name": blue_name,
            "code": blue_name,
            "image": "",
            "game_wins": 1 if blue_won else 0,
        },
        "team2": {
            "name": red_name,
            "code": red_name,
            "image": "",
            "game_wins": 0 if blue_won else 1,
        },
        "winner_code": winner_name,
        "win_team": winner_name,
        "participants": participants,
        "oracles_elixir_enriched": True,
        "riot_enriched": False,
    }


# ─── per-year ingest ─────────────────────────────────────────────────────────


def ingest_year(
    year: int,
    skip_leagues: Set[str],
    filter_leagues: Optional[Set[str]],
    dry_run: bool,
) -> Dict[str, int]:
    stats = {"games_parsed": 0, "games_indexed": 0, "games_skipped": 0}

    local = _find_local_csv(year)
    if not local:
        logger.warning("[INGEST] No local CSV for year %d — skipping", year)
        return stats

    logger.info("[INGEST] %d: %s (%d KB)", year, local.name, local.stat().st_size // 1024)

    raw_games = _parse_csv_to_games(local.read_bytes(), skip_leagues, filter_leagues)
    docs = []
    for gameid, game_data in raw_games.items():
        doc = _build_doc(gameid, game_data)
        if doc:
            docs.append(doc)
        else:
            stats["games_skipped"] += 1

    stats["games_parsed"] = len(docs)
    logger.info("[INGEST] %d: %d games parsed, %d skipped", year, len(docs), stats["games_skipped"])

    if dry_run:
        logger.info("[INGEST] dry-run: would index %d documents for %d", len(docs), year)
        return stats

    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        bulk_index(INDEX, batch)
        stats["games_indexed"] += len(batch)
        logger.info(
            "[INGEST] %d: indexed %d/%d",
            year,
            min(i + BATCH_SIZE, len(docs)),
            len(docs),
        )

    return stats


# ─── main ────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Oracle's Elixir CSVs into Elasticsearch (one doc per game).",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        metavar="YEAR",
        help="Years to ingest (default: 2014 through current year).",
    )
    parser.add_argument(
        "--leagues",
        nargs="+",
        default=None,
        metavar="LEAGUE",
        help="Only ingest these leagues, case-insensitive (e.g. LCS LCK LEC).",
    )
    parser.add_argument(
        "--skip-leagues",
        nargs="+",
        default=["CBLOL", "LTA S"],
        metavar="LEAGUE",
        help="Skip these leagues (default: CBLOL 'LTA S' — already in ES via Leaguepedia).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse CSVs but do not write to Elasticsearch.",
    )
    args = parser.parse_args()

    skip = {s.lower() for s in args.skip_leagues}
    filter_leagues = {s.lower() for s in args.leagues} if args.leagues else None

    logger.info("[INGEST] Skip leagues: %s", skip)
    if filter_leagues:
        logger.info("[INGEST] Filter leagues (only): %s", filter_leagues)
    if args.dry_run:
        logger.info("[INGEST] DRY-RUN — no writes to ES")

    current_year = datetime.now().year
    years = args.years or list(range(2014, current_year + 1))
    logger.info("[INGEST] Years: %s", years)

    totals: Dict[str, int] = {"games_parsed": 0, "games_indexed": 0, "games_skipped": 0}
    for year in years:
        stats = ingest_year(year, skip, filter_leagues, args.dry_run)
        for k in totals:
            totals[k] += stats.get(k, 0)

    logger.info(
        "[INGEST] Complete — parsed=%d indexed=%d skipped=%d",
        totals["games_parsed"],
        totals["games_indexed"],
        totals["games_skipped"],
    )


if __name__ == "__main__":
    main()
