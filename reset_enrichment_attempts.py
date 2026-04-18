"""
Reset enrichment_attempts to 0 for documents blocked at MAX_ATTEMPTS.

Run this after fixing the root cause of enrichment failures (e.g. a
Leaguepedia schema change that caused all ScoreboardPlayers queries to
return internal_api_error_MWException).

Usage:
    python reset_enrichment_attempts.py [--dry-run] [--league CBLOL]

Options:
    --dry-run    Print how many docs would be reset without modifying anything.
    --league     Only reset docs from a specific league (default: all leagues).

The script uses update_by_query to atomically set enrichment_attempts=0
and riot_enriched=false on all matching documents.
"""

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from indexers.elasticsearch_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RESET] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

INDEX = "lol_pro_matches"
MAX_ATTEMPTS = 10


def build_query(league: str = None) -> dict:
    """Build the ES query to target blocked documents."""
    must_clauses = [
        {"term": {"riot_enriched": False}},
        {"range": {"enrichment_attempts": {"gte": MAX_ATTEMPTS}}},
    ]
    if league:
        must_clauses.append({"term": {"league": league}})

    return {
        "query": {
            "bool": {
                "must": must_clauses,
            }
        }
    }


def count_blocked(es, league: str = None) -> int:
    """Count documents that are blocked from re-enrichment."""
    query = build_query(league)
    result = es.count(index=INDEX, body=query)
    return result["count"]


def reset_attempts(es, league: str = None, dry_run: bool = False) -> int:
    """Reset enrichment_attempts=0 on all blocked documents.

    Returns the number of documents updated (or that would be updated in
    dry-run mode).
    """
    query = build_query(league)

    blocked = count_blocked(es, league)
    scope = f"league={league}" if league else "all leagues"
    logger.info(f"Found {blocked} blocked documents ({scope})")

    if blocked == 0:
        logger.info("Nothing to reset.")
        return 0

    if dry_run:
        logger.info(f"[DRY RUN] Would reset {blocked} documents. Exiting without changes.")
        return blocked

    response = es.update_by_query(
        index=INDEX,
        body={
            **query,
            "script": {
                "source": "ctx._source.enrichment_attempts = 0; ctx._source.riot_enriched = false;",
                "lang": "painless",
            },
        },
        conflicts="proceed",
        wait_for_completion=True,
    )

    updated = response.get("updated", 0)
    failures = response.get("failures", [])

    if failures:
        logger.warning(f"Reset completed with {len(failures)} failures:")
        for f in failures[:10]:
            logger.warning(f"  {f}")

    logger.info(f"Reset complete: {updated} documents restored to enrichment_attempts=0")
    return updated


def main():
    parser = argparse.ArgumentParser(description="Reset blocked enrichment documents in Elasticsearch")
    parser.add_argument("--dry-run", action="store_true", help="Count without modifying")
    parser.add_argument("--league", default=None, help="Filter by league (e.g. CBLOL)")
    args = parser.parse_args()

    es = get_client()

    try:
        info = es.info()
        logger.info(f"Connected to Elasticsearch {info['version']['number']} at {os.getenv('ELASTICSEARCH_URL', 'localhost:9200')}")
    except Exception as e:
        logger.error(f"Cannot connect to Elasticsearch: {e}")
        sys.exit(1)

    reset_attempts(es, league=args.league, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
