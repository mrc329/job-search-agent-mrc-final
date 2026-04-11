"""Entry point for the job search agent."""

import argparse
import logging

from job_agent import JobSearchAgent
from models import SearchConfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args():
    parser = argparse.ArgumentParser(description="Search for jobs across multiple boards.")
    parser.add_argument("keywords", nargs="+", help="Search keywords (e.g. python engineer)")
    parser.add_argument("--location", default="Remote", help="Job location")
    parser.add_argument("--max-results", type=int, default=50, help="Max results to return")
    parser.add_argument("--remote-only", action="store_true", help="Only show remote jobs")
    parser.add_argument(
        "--sources",
        nargs="+",
        default=["indeed", "linkedin"],
        help="Job boards to search",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = SearchConfig(
        keywords=args.keywords,
        location=args.location,
        max_results=args.max_results,
        remote_only=args.remote_only,
        sources=args.sources,
    )
    agent = JobSearchAgent(config)
    jobs = agent.run()

    if not jobs:
        print("No jobs found.")
        return

    for job in jobs:
        print(f"{job.title} @ {job.company} — {job.location}")
        print(f"  {job.url}")
        if job.salary:
            print(f"  Salary: {job.salary}")
        print()


if __name__ == "__main__":
    main()
