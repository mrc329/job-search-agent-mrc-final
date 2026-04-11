"""
Job Search Agent - searches multiple job boards and aggregates results.
"""

import logging
from typing import Optional

import requests

from models import Job, SearchConfig

logger = logging.getLogger(__name__)


class ConfigValidationError(ValueError):
    """Raised when a SearchConfig fails validation."""


class JobSearchAgent:
    """Agent that searches for jobs across multiple sources."""

    SUPPORTED_SOURCES = {"indeed", "linkedin"}

    def __init__(self, config: SearchConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (compatible; JobSearchBot/1.0)"}
        )

    def validate_config(self) -> None:
        """Validate search configuration before running a search.

        Raises:
            ConfigValidationError: if any field is invalid.
        """
        # TODO: Validate that each keyword is a non-empty string (strip whitespace)
        if not self.config.keywords:
            raise ConfigValidationError("keywords must be a non-empty list")

        # TODO: Validate that location is a non-empty string
        if not isinstance(self.config.location, str) or not self.config.location.strip():
            raise ConfigValidationError("location must be a non-empty string")

        # TODO: Validate that max_results is a positive integer
        if not isinstance(self.config.max_results, int) or self.config.max_results <= 0:
            raise ConfigValidationError("max_results must be a positive integer")

        # TODO: Validate that sources only contain supported values
        unknown = set(self.config.sources) - self.SUPPORTED_SOURCES
        if unknown:
            raise ConfigValidationError(
                f"unsupported sources: {', '.join(sorted(unknown))}. "
                f"Supported: {', '.join(sorted(self.SUPPORTED_SOURCES))}"
            )

    def search(self) -> list:
        """Run the job search and return deduplicated results."""
        self.validate_config()

        # TODO: Add caching so repeated searches with identical params skip
        #       the network calls and return a stored result instead.

        results = []
        if "indeed" in self.config.sources:
            results.extend(self._search_indeed())
        if "linkedin" in self.config.sources:
            results.extend(self._search_linkedin())

        # Deduplicate by URL
        seen_urls: set = set()
        unique: list = []
        for job in results:
            if job.url not in seen_urls:
                seen_urls.add(job.url)
                unique.append(job)

        return unique

    def _search_indeed(self) -> list:
        """Search Indeed for matching jobs."""
        # TODO: Implement Indeed scraping / API integration
        logger.info("Indeed search not yet implemented")
        return []

    def _search_linkedin(self) -> list:
        """Search LinkedIn for matching jobs."""
        # TODO: Implement LinkedIn scraping / API integration
        logger.info("LinkedIn search not yet implemented")
        return []

    def filter_results(self, jobs: list) -> list:
        """Filter job results based on config criteria."""
        filtered = []
        for job in jobs:
            if self.config.remote_only and "remote" not in job.location.lower():
                continue
            # TODO: Add salary filtering once salary parsing is implemented
            filtered.append(job)
        return filtered

    def rank_results(self, jobs: list) -> list:
        """Rank job results by relevance.

        TODO: Implement a relevance scoring algorithm that weighs:
              - keyword match frequency in title and description
              - recency of the posting date
              - salary alignment with min_salary config
        """
        return jobs

    def run(self) -> list:
        """Full pipeline: search, filter, and rank."""
        logger.info("Starting job search for: %s", self.config.keywords)
        jobs = self.search()
        jobs = self.filter_results(jobs)
        jobs = self.rank_results(jobs)
        logger.info("Found %d jobs after filtering", len(jobs))
        return jobs
