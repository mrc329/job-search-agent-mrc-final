"""Pipeline runner: discover jobs, dedup against Notion, sync new ones.

Every stage uses _run_stage() which gives a uniform log pattern:
    [run_id] ── stage_name ── N in ──────────────────────
    [run_id]   KEEP / SKIP (reason): title @ company <url>
    [run_id]   → N kept, M skipped  (0.3s)

This makes it trivial to grep the log for any stage and see exactly
what passed through or was filtered and why.
"""

import hashlib
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
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

# Stage result type: jobs that passed + [(rejected_job, reason)]
_StageResult = tuple[list[Job], list[tuple[Job, str]]]


# ── Uniform stage executor ────────────────────────────────────────────────────
def _run_stage(
    run_id: str,
    name: str,
    jobs: list[Job],
    fn: Callable[[list[Job]], _StageResult],
    stats: "RunStats",
) -> list[Job]:
    """Run *fn* over *jobs* with consistent entry/exit logging for every stage.

    *fn* must return (kept, [(skipped_job, reason), ...]).
    Errors inside *fn* are caught, logged, and the input list is returned
    unchanged so downstream stages still run.
    """
    divider = "─" * 44
    logger.info("[%s] ── %s ── %d in %s", run_id, name.upper(), len(jobs), divider)
    t0 = time.monotonic()

    try:
        kept, skipped = fn(jobs)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "[%s]   Stage '%s' crashed (%s: %s) — passing input through unchanged",
            run_id,
            name,
            type(exc).__name__,
            exc,
        )
        stats.errors += 1
        kept, skipped = jobs, []

    elapsed = time.monotonic() - t0

    for job, reason in skipped:
        logger.info(
            "[%s]   SKIP (%-30s): %s @ %s  <%s>",
            run_id,
            reason,
            job.title,
            job.company,
            job.url,
        )
    for job in kept:
        logger.debug(
            "[%s]   KEEP: %s @ %s  <%s>",
            run_id,
            job.title,
            job.company,
            job.url,
        )

    logger.info(
        "[%s]   → %d kept, %d skipped  (%.1fs)",
        run_id,
        len(kept),
        len(skipped),
        elapsed,
    )
    return kept


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
        # TODO: parse job listings out of markdown into Job objects
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

        page_num += 1
        for page in data.get("results", []):
            job_url = _extract_url_from_page(page)
            if job_url:
                h = hashlib.sha256(job_url.encode()).hexdigest()[:16]
                hashes.add(h)
                logger.debug("  seen [%s] ← %s", h, job_url)

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
            "Notion sync failed for '%s @ %s': %s", job.title, job.company, exc
        )
        return False


# ── Stage implementations ─────────────────────────────────────────────────────

def _stage_dedup_run(jobs: list[Job]) -> _StageResult:
    """Remove duplicate URLs seen within this run (before Notion check)."""
    seen: set[str] = set()
    kept: list[Job] = []
    skipped: list[tuple[Job, str]] = []
    for job in jobs:
        if job.url in seen:
            skipped.append((job, "duplicate URL within run"))
        else:
            seen.add(job.url)
            kept.append(job)
    return kept, skipped


def _make_dedup_notion_stage(seen_hashes: set[str]) -> Callable[[list[Job]], _StageResult]:
    """Return a stage function that filters jobs already in Notion."""
    def _stage(jobs: list[Job]) -> _StageResult:
        kept: list[Job] = []
        skipped: list[tuple[Job, str]] = []
        for job in jobs:
            h = _job_hash(job)
            if h in seen_hashes:
                skipped.append((job, f"already in Notion [{h}]"))
            else:
                kept.append(job)
        return kept, skipped
    return _stage


def _make_relevance_stage(keywords: list[str]) -> Callable[[list[Job]], _StageResult]:
    """Return a stage that keeps jobs whose title or description match any keyword.

    With no keywords configured every job passes through.
    """
    def _stage(jobs: list[Job]) -> _StageResult:
        if not keywords:
            logger.info("  relevance: no PIPELINE_KEYWORDS set — all jobs pass")
            return jobs, []
        kept: list[Job] = []
        skipped: list[tuple[Job, str]] = []
        for job in jobs:
            haystack = f"{job.title} {job.description}".lower()
            matched = [kw for kw in keywords if kw.lower() in haystack]
            if matched:
                logger.debug(
                    "  relevance PASS  matched=%s  %s @ %s",
                    matched,
                    job.title,
                    job.company,
                )
                kept.append(job)
            else:
                skipped.append(
                    (job, f"no keyword match (checked: {', '.join(keywords[:5])}{'…' if len(keywords) > 5 else ''})")
                )
        return kept, skipped
    return _stage


def _make_qualification_stage(max_calls: int) -> Callable[[list[Job]], _StageResult]:
    """Return a stage that caps at *max_calls* jobs and stubs LLM qualification.

    Each job that would require an LLM call beyond the cap is logged as skipped
    so it is clear exactly which jobs were dropped by the NLU budget.
    """
    def _stage(jobs: list[Job]) -> _StageResult:
        kept: list[Job] = []
        skipped: list[tuple[Job, str]] = []
        for i, job in enumerate(jobs):
            if i >= max_calls:
                skipped.append(
                    (job, f"NLU cap reached ({max_calls} max)")
                )
                continue
            # TODO: call Claude/LLM to score job fit; reject if score < threshold
            logger.debug(
                "  qualification PASS (stub, call %d/%d): %s @ %s",
                i + 1,
                max_calls,
                job.title,
                job.company,
            )
            kept.append(job)
        return kept, skipped
    return _stage


def _make_draft_stage() -> Callable[[list[Job]], _StageResult]:
    """Return a stage that generates cover-letter drafts (stub)."""
    def _stage(jobs: list[Job]) -> _StageResult:
        kept: list[Job] = []
        skipped: list[tuple[Job, str]] = []
        for job in jobs:
            # TODO: call Claude to generate a tailored cover-letter draft
            logger.debug("  draft stub: %s @ %s", job.title, job.company)
            kept.append(job)
        return kept, skipped
    return _stage


def _make_sync_stage(
    token: str, database_id: str
) -> Callable[[list[Job]], _StageResult]:
    """Return a stage that pushes each job to Notion."""
    def _stage(jobs: list[Job]) -> _StageResult:
        kept: list[Job] = []
        skipped: list[tuple[Job, str]] = []
        for job in jobs:
            ok = sync_job_to_notion(job, token, database_id)
            if ok:
                kept.append(job)
            else:
                skipped.append((job, "Notion API error"))
        return kept, skipped
    return _stage


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
    skipped_per_stage: dict[str, int] = field(default_factory=dict)


# ── Discovery (not a _run_stage call — has its own per-source logging) ────────
def _discover(
    run_id: str,
    requested_sources: list[str],
    stats: RunStats,
) -> list[Job]:
    divider = "─" * 44
    logger.info("[%s] ── DISCOVERY ── %s", run_id, divider)
    registered_names = sorted(_SOURCE_REGISTRY)
    all_jobs: list[Job] = []

    for source_name in requested_sources:
        if source_name not in _SOURCE_REGISTRY:
            logger.warning(
                "[%s]   Unknown source: %s  (registered: %s)",
                run_id,
                source_name,
                ", ".join(registered_names) if registered_names else "none",
            )
            stats.errors += 1
            continue

        t0 = time.monotonic()
        logger.info("[%s]   source '%s': starting...", run_id, source_name)
        try:
            jobs = _SOURCE_REGISTRY[source_name]()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[%s]   source '%s' raised %s: %s",
                run_id,
                source_name,
                type(exc).__name__,
                exc,
            )
            stats.errors += 1
            jobs = []

        elapsed = time.monotonic() - t0
        logger.info(
            "[%s]   source '%s': %d job(s) in %.1fs",
            run_id,
            source_name,
            len(jobs),
            elapsed,
        )
        for job in jobs:
            logger.info(
                "[%s]     + %s @ %s  <%s>", run_id, job.title, job.company, job.url
            )
        all_jobs.extend(jobs)

    logger.info(
        "[%s]   → %d total discovered across %d source(s)",
        run_id,
        len(all_jobs),
        len(requested_sources),
    )
    return all_jobs


# ── Pipeline ──────────────────────────────────────────────────────────────────
def run_pipeline() -> RunStats:
    run_id = uuid.uuid4().hex[:8]
    stats = RunStats()
    t_run = time.monotonic()

    notion_token = os.getenv("NOTION_TOKEN", "")
    notion_db = os.getenv("NOTION_DATABASE_ID", "")
    sources_raw = os.getenv("PIPELINE_SOURCES", "")
    keywords_raw = os.getenv("PIPELINE_KEYWORDS", "")
    max_nlu = int(os.getenv("MAX_NLU_CALLS", "25"))

    requested_sources = [
        s.strip() for s in sources_raw.replace(",", " ").split() if s.strip()
    ] or list(_SOURCE_REGISTRY.keys())

    keywords = [k.strip() for k in keywords_raw.replace(",", " ").split() if k.strip()]

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    logger.info(
        "[%s] Run %s | Sources: %s | Keywords: %s | %s",
        run_id,
        run_id,
        requested_sources,
        keywords or "(none)",
        now_utc,
    )

    # ── Load seen hashes from Notion ──────────────────────────────────────────
    seen_hashes: set[str] = set()
    if notion_token and notion_db:
        logger.info("[%s] Loading seen hashes from Notion...", run_id)
        seen_hashes = load_seen_hashes(notion_token, notion_db)
        logger.info("[%s]   %d already seen", run_id, len(seen_hashes))
    else:
        logger.warning(
            "[%s] NOTION_TOKEN or NOTION_DATABASE_ID not set — dedup against Notion disabled",
            run_id,
        )

    # ── 1. Discovery ──────────────────────────────────────────────────────────
    all_jobs = _discover(run_id, requested_sources, stats)
    stats.discovered = len(all_jobs)

    if not all_jobs:
        logger.info("[%s] Nothing new. Done.", run_id)
        _print_summary(run_id, stats, time.monotonic() - t_run)
        return stats

    # ── 2. Dedup: within-run duplicates ───────────────────────────────────────
    jobs = _run_stage(run_id, "dedup:run", all_jobs, _stage_dedup_run, stats)
    stats.skipped_per_stage["dedup:run"] = len(all_jobs) - len(jobs)

    # ── 3. Dedup: already in Notion ───────────────────────────────────────────
    before = len(jobs)
    jobs = _run_stage(
        run_id,
        "dedup:notion",
        jobs,
        _make_dedup_notion_stage(seen_hashes),
        stats,
    )
    stats.new = len(jobs)
    stats.skipped_per_stage["dedup:notion"] = before - len(jobs)
    logger.info("[%s] %d new after dedup", run_id, stats.new)

    if not jobs:
        logger.info("[%s] Nothing new. Done.", run_id)
        _print_summary(run_id, stats, time.monotonic() - t_run)
        return stats

    # ── 4. Relevance filter ───────────────────────────────────────────────────
    before = len(jobs)
    jobs = _run_stage(
        run_id,
        "relevance",
        jobs,
        _make_relevance_stage(keywords),
        stats,
    )
    stats.relevant = len(jobs)
    stats.skipped_per_stage["relevance"] = before - len(jobs)

    if not jobs:
        logger.info("[%s] Nothing passed relevance filter. Done.", run_id)
        _print_summary(run_id, stats, time.monotonic() - t_run)
        return stats

    # ── 5. Qualification / NLU ────────────────────────────────────────────────
    before = len(jobs)
    jobs = _run_stage(
        run_id,
        "qualification",
        jobs,
        _make_qualification_stage(max_nlu),
        stats,
    )
    stats.qualified = len(jobs)
    stats.skipped_per_stage["qualification"] = before - len(jobs)

    if not jobs:
        logger.info("[%s] Nothing passed qualification. Done.", run_id)
        _print_summary(run_id, stats, time.monotonic() - t_run)
        return stats

    # ── 6. Draft generation ───────────────────────────────────────────────────
    before = len(jobs)
    jobs = _run_stage(run_id, "draft", jobs, _make_draft_stage(), stats)
    stats.drafted = len(jobs)
    stats.skipped_per_stage["draft"] = before - len(jobs)

    # ── 7. Sync to Notion ─────────────────────────────────────────────────────
    if not notion_token or not notion_db:
        logger.warning(
            "[%s] Notion credentials missing — skipping sync of %d job(s)",
            run_id,
            len(jobs),
        )
    else:
        before = len(jobs)
        jobs = _run_stage(
            run_id,
            "sync:notion",
            jobs,
            _make_sync_stage(notion_token, notion_db),
            stats,
        )
        stats.synced = len(jobs)
        stats.skipped_per_stage["sync:notion"] = before - len(jobs)
        stats.errors += stats.skipped_per_stage["sync:notion"]

    _print_summary(run_id, stats, time.monotonic() - t_run)
    return stats


# ── Summary ───────────────────────────────────────────────────────────────────
def _print_summary(run_id: str, stats: RunStats, elapsed: float) -> None:
    bar = "=" * 52
    print(bar)
    print(f"  DONE -- run {run_id}")
    print(bar)
    stage_rows = [
        ("discovered",   stats.discovered),
        ("new",          stats.new),
        ("relevant",     stats.relevant),
        ("qualified",    stats.qualified),
        ("drafted",      stats.drafted),
        ("synced",       stats.synced),
        ("errors",       stats.errors),
    ]
    for label, value in stage_rows:
        print(f"  {label:<14} {value}")

    if stats.skipped_per_stage:
        print()
        print("  skipped per stage:")
        for stage, count in stats.skipped_per_stage.items():
            print(f"    {stage:<20} {count}")

    print(bar)
    print(f"{int(elapsed)}s")


if __name__ == "__main__":
    sys.exit(0 if run_pipeline().errors == 0 else 1)
