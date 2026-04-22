#!/usr/bin/env python3
"""
Oracle's Elixir Backfill

Fills damage_taken, wards_placed, wards_killed for participants where those
fields are zero. Joins OE CSV data to ES docs via composite key:
  (league, date_day, teamname, position, champion)

The OE gameid format (e.g. "LOLTMNT03_34116") differs from leaguepedia_page
("CBLOL/2024 Season/Split 2_Week 2_4_1") — direct gameid join is not possible.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from providers.oracles_elixir import load_all_years  # noqa: E402
from indexers.elasticsearch_client import get_client  # noqa: E402

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/oracles_elixir_backfill.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

INDEX = os.getenv("INDEX_NAME", "lol_pro_matches")

INTEGRITY_TOLERANCE = 0.05

# League rebrands: ES stores historical names, OE uses the current name at the time of the match
_LEAGUE_ALIASES: Dict[str, List[str]] = {
    "lta s": ["cblol"],   # CBLOL → LTA Sul (rebranded 2025)
    "lta n": ["lla"],     # LLA → LTA Norte (rebranded 2025)
    "lta":   ["cblol", "lla"],  # generic LTA umbrella
}
BULK_FLUSH_EVERY = 100
SCROLL_BATCH_SIZE = 500
SCROLL_TTL = "5m"

# Join key type: (league, date_day, teamname_norm, position_norm, champion_norm)
_JoinKey = Tuple[str, str, str, str, str]


def _norm(value: str) -> str:
    return (value or "").strip().lower()


def _league_keys(league_norm: str) -> List[str]:
    """Return all league name variants to index under (handles rebrands)."""
    keys = [league_norm]
    for alias_target, alias_sources in _LEAGUE_ALIASES.items():
        if league_norm == alias_target:
            keys.extend(alias_sources)
        elif league_norm in alias_sources:
            keys.append(alias_target)
    return list(set(keys))


def _build_participant_index(
    oe_data: Dict[str, List[Dict]],
) -> Dict[_JoinKey, List[Dict]]:
    """Flatten all OE participants into a composite-key index.

    Key: (league, date_day, teamname_norm, position_norm, champion_norm)
    Value: list of matching participants (list handles rare same-team/same-champ duplicates in Bo5).
    Each participant is indexed under all known league name variants to handle rebrands.
    """
    index: Dict[_JoinKey, List[Dict]] = {}
    for participants in oe_data.values():
        for p in participants:
            league_norm = _norm(p["league"])
            for league_variant in _league_keys(league_norm):
                key: _JoinKey = (
                    league_variant,
                    p["date_day"],
                    _norm(p["teamname"]),
                    _norm(p["position"]),
                    _norm(p["champion"]),
                )
                index.setdefault(key, []).append(p)
    return index


def _best_oe_match(
    candidates: List[Dict],
    es_damage: int,
) -> Optional[Dict]:
    """From a list of duplicate-key OE participants, pick the closest damage match.

    If only one candidate, return it directly. If multiple (rare Bo5 duplicate),
    pick the one where damagetochampions is nearest to es_damage.
    """
    if len(candidates) == 1:
        return candidates[0]
    if es_damage <= 0:
        return candidates[0]
    return min(candidates, key=lambda p: abs(p.get("damagetochampions", 0) - es_damage))


def _integrity_ok(oe_participant: Dict, es_participant: Dict) -> bool:
    es_damage = es_participant.get("damage", 0) or 0
    # Cannot validate when ES has no damage data — allow update
    if es_damage == 0:
        return True

    oe_damage = oe_participant.get("damagetochampions", 0) or 0
    delta = abs(oe_damage - es_damage) / es_damage
    return delta <= INTEGRITY_TOLERANCE


def _enrich_participants(
    es_participants: List[Dict],
    oe_index: Dict[_JoinKey, List[Dict]],
    doc_league: str,
    doc_date_day: str,
    doc_id: str,
) -> Tuple[List[Dict], int, int]:
    """Apply OE fields to ES participants where the join succeeds.

    Returns (updated_participants, updated_count, integrity_mismatch_count).
    """
    updated = 0
    mismatches = 0

    for p in es_participants:
        team = _norm(p.get("team_name", "") or p.get("team", "") or "")
        position = _norm(p.get("role", "") or p.get("position", "") or "")
        champion = _norm(p.get("champion_name", "") or p.get("champion", "") or "")

        if not (team and position and champion):
            continue

        key: _JoinKey = (doc_league, doc_date_day, team, position, champion)
        candidates = oe_index.get(key)
        if not candidates:
            continue

        es_damage = p.get("damage", 0) or 0
        oe = _best_oe_match(candidates, es_damage)

        if not _integrity_ok(oe, p):
            logger.warning(
                "[BACKFILL] integrity_mismatch doc=%s player=%s es_damage=%s oe_damage=%s",
                doc_id,
                p.get("summoner_name", str(key)),
                es_damage,
                oe.get("damagetochampions"),
            )
            mismatches += 1
            continue

        p["damage_taken"] = oe["damage_taken"]
        p["wards_placed"] = oe["wardsplaced"]
        p["wards_killed"] = oe["wardskilled"]
        updated += 1

    return es_participants, updated, mismatches


def _flush_bulk(es, bulk_actions: List[Dict], dry_run: bool) -> None:
    if not bulk_actions:
        return
    if dry_run:
        logger.info("[BACKFILL] dry-run: would flush %d bulk update(s)", len(bulk_actions))
        return

    body = []
    for action in bulk_actions:
        body.append({"update": {"_index": action["_index"], "_id": action["_id"]}})
        body.append({"doc": action["doc"]})

    es.bulk(body=body)
    logger.info("[BACKFILL] Flushed %d bulk update(s)", len(bulk_actions))


def run_backfill(
    years: Optional[List[int]] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    stats = {
        "processed": 0,
        "updated": 0,
        "not_found_in_oe": 0,
        "already_enriched": 0,
        "integrity_mismatch": 0,
        "missing_fields": 0,
    }

    logger.info("[BACKFILL] Loading Oracle's Elixir data (years=%s)", years or "all")
    oe_data = load_all_years(years)
    logger.info("[BACKFILL] OE data loaded: %d gameids", len(oe_data))

    logger.info("[BACKFILL] Building composite participant index...")
    oe_index = _build_participant_index(oe_data)
    logger.info("[BACKFILL] Participant index built: %d unique keys", len(oe_index))

    es = get_client()

    query = {
        "bool": {
            "must": {
                "nested": {
                    "path": "participants",
                    "query": {"term": {"participants.damage_taken": 0}},
                }
            },
            "must_not": {"term": {"oracles_elixir_enriched": True}},
        }
    }

    logger.info("[BACKFILL] Starting ES scroll (index=%s, batch=%d)", INDEX, SCROLL_BATCH_SIZE)

    resp = es.search(
        index=INDEX,
        query=query,
        scroll=SCROLL_TTL,
        size=SCROLL_BATCH_SIZE,
    )

    scroll_id = resp["_scroll_id"]
    hits = resp["hits"]["hits"]
    total_estimate = resp["hits"]["total"]["value"]
    logger.info("[BACKFILL] Scroll started — estimated %d docs to process", total_estimate)

    bulk_actions: List[Dict] = []

    try:
        while hits:
            for hit in hits:
                doc_id = hit["_id"]
                source = hit["_source"]
                stats["processed"] += 1

                if source.get("oracles_elixir_enriched"):
                    stats["already_enriched"] += 1
                    continue

                doc_league = _norm(source.get("league", "") or "")
                start_time = source.get("start_time", "") or ""
                doc_date_day = start_time[:10]  # "2024-06-08"

                if not (doc_league and doc_date_day):
                    stats["missing_fields"] += 1
                    continue

                es_participants = source.get("participants", [])

                updated_participants, participant_updates, mismatches = _enrich_participants(
                    es_participants, oe_index, doc_league, doc_date_day, doc_id
                )

                stats["integrity_mismatch"] += mismatches

                if participant_updates == 0:
                    stats["not_found_in_oe"] += 1
                    continue

                stats["updated"] += 1

                bulk_actions.append({
                    "_index": INDEX,
                    "_id": doc_id,
                    "doc": {
                        "participants": updated_participants,
                        "oracles_elixir_enriched": True,
                        "oracles_elixir_enriched_at": datetime.now(tz=timezone.utc).isoformat(),
                    },
                })

                if len(bulk_actions) >= BULK_FLUSH_EVERY:
                    _flush_bulk(es, bulk_actions, dry_run)
                    bulk_actions = []

            resp = es.scroll(scroll_id=scroll_id, scroll=SCROLL_TTL)
            scroll_id = resp["_scroll_id"]
            hits = resp["hits"]["hits"]

    finally:
        if bulk_actions:
            _flush_bulk(es, bulk_actions, dry_run)
        try:
            es.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass

    return stats


def _print_report(stats: Dict[str, int]) -> None:
    logger.info(
        "[BACKFILL] Report: processed=%d updated=%d not_found_in_oe=%d "
        "integrity_mismatch=%d already_enriched=%d missing_fields=%d",
        stats["processed"],
        stats["updated"],
        stats["not_found_in_oe"],
        stats["integrity_mismatch"],
        stats["already_enriched"],
        stats["missing_fields"],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Oracle's Elixir Backfill")
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        metavar="YEAR",
        help="Years to load from OE (e.g. --years 2024 2025). Default: all years.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and join data but do not write to Elasticsearch.",
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info("[BACKFILL] DRY-RUN mode — no writes will be made to ES")

    stats = run_backfill(years=args.years, dry_run=args.dry_run)
    _print_report(stats)


if __name__ == "__main__":
    main()
