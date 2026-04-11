"""Data models for the job search agent."""

from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class Job:
    """Represents a single job listing."""

    title: str
    company: str
    location: str
    url: str
    description: str = ""
    salary: Optional[str] = None
    posted_date: Optional[datetime] = None
    source: str = ""

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, other):
        if not isinstance(other, Job):
            return False
        return self.url == other.url


@dataclass
class SearchConfig:
    """Configuration for a job search run."""

    keywords: list
    location: str
    max_results: int = 50
    remote_only: bool = False
    min_salary: Optional[int] = None
    sources: list = field(default_factory=lambda: ["indeed", "linkedin"])
