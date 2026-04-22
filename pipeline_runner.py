"""Pipeline runner: discover jobs, dedup against Notion, sync new ones."""

import hashlib
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import requests

from models import Job

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


# ── Source registry ───────────────────────────────────────────────────────────
_SOURCE_REGISTRY: dict[str, Callable[[], list[Job]]] = {}


def register_source(name: str) -> Callable:
    """Decorator that registers a scraper function under *name*."""
    def decorator(fn: Callable[[], list[Job]]) -> Callable[[], list[Job]]:
        _SOURCE_REGISTRY[name] = fn
        return fn
    return decorator


# ── Source implementations ────────────────────────────────────────────────────
@register_source("direct_travel")
def _scrape_direct_travel() -> list[Job]:
    """Scrape DirectTravel.com careers page via Firecrawl."""
    firecrawl_key = os.getenv("FIRECRAWL_API_KEY", "")
    if not firecrawl_key:
        logger.warning("direct_travel: FIRECRAWL_API_KEY not set — returning 0 jobs")
        return []

    target_url = "https://www.directtravel.com/careers"
    logger.info("direct_travel: fetching %s via Firecrawl...", target_url)
    try:
        resp = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={
                "Authorization": f"Bearer {firecrawl_key}",
                "Content-Type": "application/json",
            },
            json={"url": target_url, "formats": ["markdown"]},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        markdown = (data.get("data") or {}).get("markdown", "")
        logger.debug("direct_travel: received %d bytes of markdown", len(markdown))
        # TODO: parse job listings out of markdown
        return []
    except requests.RequestException as exc:
        logger.error("direct_travel: HTTP error — %s", exc)
        return []


@register_source("indeed")
def _scrape_indeed() -> list[Job]:
    logger.info("indeed: not yet implemented — returning 0 jobs")
    return []


@register_source("linkedin")
def _scrape_linkedin() -> list[Job]:
    logger.info("linkedin: not yet implemented — returning 0 jobs")
    return []


# ── Job fingerprint ───────────────────────────────────────────────────────────
def _job_hash(job: Job) -> str:
    return hashlib.sha256(job.url.encode()).hexdigest()[:16]


# ── Notion helpers ────────────────────────────────────────────────────────────
def _notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _extract_url_from_page(page: dict) -> str:
    """Pull the job URL string out of a Notion page's properties."""
    for prop_name, prop_val in page.get("properties", {}).items():
        if prop_val.get("type") == "url":
            return prop_val.get("url") or ""
        if prop_name.lower() in ("url", "job url", "link"):
            for rt in prop_val.get("rich_text", []):
                text = rt.get("plain_text", "")
                if text:
                    return text
    return ""


def load_seen_hashes(token: str, database_id: str) -> set[str]:
    """Return SHA-256[:16] hashes of every job URL already in Notion."""
    hashes: set[str] = set()
    query_url = f"https://api.notion.com/v1/databases/{database_id}/query"
    cursor = None
    page_num = 0

    while True:
        payload: dict = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        try:
            resp = requests.post(
                query_url,
                headers=_notion_headers(token),
                json=payload,
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("Notion query (page %d) failed: %s", page_num, exc)
            break

        results = data.get("results", [])
        page_num += 1
        for page in results:
            job_url = _extract_url_from_page(page)
            if job_url:
                h = hashlib.sha256(job_url.encode()).hexdigest()[:16]
                hashes.add(h)
                logger.debug("  seen hash %s  ← %s", h, job_url)

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return hashes


def sync_job_to_notion(job: Job, token: str, database_id: str) -> bool:
    """Create a new Notion page for *job*. Returns True on success."""
    payload = {
        "parent": {"database_id": database_id},
        "properties": {
            "Name": {
                "title": [{"text": {"content": f"{job.title} @ {job.company}"}}]
            },
            "Company": {"rich_text": [{"text": {"content": job.company}}]},
            "Title": {"rich_text": [{"text": {"content": job.title}}]},
            "Location": {"rich_text": [{"text": {"content": job.location}}]},
            "URL": {"url": job.url},
            "Source": {"rich_text": [{"text": {"content": job.source}}]},
        },
    }
    if job.salary:
        payload["properties"]["Salary"] = {
            "rich_text": [{"text": {"content": job.salary}}]
        }
    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=_notion_headers(token),
            json=payload,
            timeout=20,
        )
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error(
            "  Notion sync failed for '%s @ %s': %s", job.title, job.company, exc
        )
        return False


# ── Stats ─────────────────────────────────────────────────────────────────────
@dataclass
class RunStats:
    discovered: int = 0
    new: int = 0
    relevant: int = 0
    qualified: int = 0
    drafted: int = 0
    synced: int = 0
    errors: int = 0


# ── Pipeline ──────────────────────────────────────────────────────────────────
def run_pipeline() -> RunStats:
    run_id = uuid.uuid4().hex[:8]
    stats = RunStats()
    t0 = time.monotonic()

    notion_token = os.getenv("NOTION_TOKEN", "")
    notion_db = os.getenv("NOTION_DATABASE_ID", "")
    sources_raw = os.getenv("PIPELINE_SOURCES", "")
    requested_sources = [
        s.strip()
        for s in sources_raw.replace(",", " ").split()
        if s.strip()
    ]
    if not requested_sources:
        requested_sources = list(_SOURCE_REGISTRY.keys())

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    logger.info(
        "[%s] Run %s | Sources: %s | %s", run_id, run_id, requested_sources, now_utc
    )

    # ── Load seen hashes from Notion ──────────────────────────────────────────
    seen_hashes: set[str] = set()
    if notion_token and notion_db:
        logger.info("[%s] Loading seen hashes from Notion...", run_id)
        seen_hashes = load_seen_hashes(notion_token, notion_db)
        logger.info("[%s]   %d already seen", run_id, len(seen_hashes))
    else:
        logger.warning(
            "[%s] NOTION_TOKEN or NOTION_DATABASE_ID not set — dedup disabled", run_id
        )

    # ── Discover jobs per source ──────────────────────────────────────────────
    all_jobs: list[Job] = []
    registered_names = sorted(_SOURCE_REGISTRY)

    for source_name in requested_sources:
        if source_name not in _SOURCE_REGISTRY:
            logger.warning(
                "[%s] Unknown source: %s  (registered: %s)",
                run_id,
                source_name,
                ", ".join(registered_names) if registered_names else "none",
            )
            continue

        logger.info("[%s] Running source '%s'...", run_id, source_name)
        try:
            jobs = _SOURCE_REGISTRY[source_name]()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[%s] Source '%s' raised: %s: %s",
                run_id,
                source_name,
                type(exc).__name__,
                exc,
            )
            stats.errors += 1
            jobs = []

        logger.info(
            "[%s]   '%s' discovered %d job(s)", run_id, source_name, len(jobs)
        )
        for job in jobs:
            logger.info(
                "[%s]     + %s @ %s  <%s>", run_id, job.title, job.company, job.url
            )

        all_jobs.extend(jobs)

    stats.discovered = len(all_jobs)
    logger.info("[%s] Total discovered: %d", run_id, stats.discovered)

    # ── Dedup: within-run URL uniqueness then Notion hash check ──────────────
    url_seen_this_run: set[str] = set()
    new_jobs: list[Job] = []

    for job in all_jobs:
        if job.url in url_seen_this_run:
            logger.info(
                "[%s]   SKIP (duplicate within run): %s @ %s  <%s>",
                run_id,
                job.title,
                job.company,
                job.url,
            )
            continue
        url_seen_this_run.add(job.url)

        h = _job_hash(job)
        if h in seen_hashes:
            logger.info(
                "[%s]   SKIP (already in Notion) [%s]: %s @ %s  <%s>",
                run_id,
                h,
                job.title,
                job.company,
                job.url,
            )
        else:
            logger.info(
                "[%s]   NEW [%s]: %s @ %s  <%s>",
                run_id,
                h,
                job.title,
                job.company,
                job.url,
            )
            new_jobs.append(job)

    stats.new = len(new_jobs)
    logger.info("[%s] %d new after dedup", run_id, stats.new)

    if not new_jobs:
        logger.info("[%s] Nothing new. Done.", run_id)
        _print_summary(run_id, stats, time.monotonic() - t0)
        return stats

    # ── Relevance filter (stub) ───────────────────────────────────────────────
    relevant_jobs = new_jobs  # TODO: keyword/role relevance scoring
    stats.relevant = len(relevant_jobs)
    logger.info("[%s] %d relevant after relevance filter", run_id, stats.relevant)

    # ── Qualification / NLU filter (stub) ─────────────────────────────────────
    max_nlu = int(os.getenv("MAX_NLU_CALLS", "25"))
    qualified_jobs = relevant_jobs[:max_nlu]  # TODO: LLM-based qualification
    stats.qualified = len(qualified_jobs)
    logger.info(
        "[%s] %d qualified (NLU cap: %d)", run_id, stats.qualified, max_nlu
    )

    # ── Draft generation (stub) ───────────────────────────────────────────────
    drafted_jobs = qualified_jobs  # TODO: generate cover-letter drafts via Claude
    stats.drafted = len(drafted_jobs)
    logger.info("[%s] %d drafted", run_id, stats.drafted)

    # ── Sync to Notion ────────────────────────────────────────────────────────
    if not notion_token or not notion_db:
        logger.warning(
            "[%s] Notion credentials missing — skipping sync of %d job(s)",
            run_id,
            len(drafted_jobs),
        )
    else:
        logger.info("[%s] Syncing %d job(s) to Notion...", run_id, len(drafted_jobs))
        for job in drafted_jobs:
            logger.info("[%s]   SYNC: %s @ %s", run_id, job.title, job.company)
            ok = sync_job_to_notion(job, notion_token, notion_db)
            if ok:
                stats.synced += 1
                logger.info("[%s]     -> synced OK", run_id)
            else:
                stats.errors += 1
                logger.error("[%s]     -> sync FAILED", run_id)

    _print_summary(run_id, stats, time.monotonic() - t0)
    return stats


def _print_summary(run_id: str, stats: RunStats, elapsed: float) -> None:
    bar = "=" * 52
    print(bar)
    print(f"  DONE -- run {run_id}")
    print(bar)
    rows = [
        ("discovered", stats.discovered),
        ("new", stats.new),
        ("relevant", stats.relevant),
        ("qualified", stats.qualified),
        ("drafted", stats.drafted),
        ("synced", stats.synced),
        ("errors", stats.errors),
    ]
    for label, value in rows:
        print(f"  {label:<14} {value}")
    print(bar)
    print(f"{int(elapsed)}s")


if __name__ == "__main__":
    sys.exit(0 if run_pipeline().errors == 0 else 1)
