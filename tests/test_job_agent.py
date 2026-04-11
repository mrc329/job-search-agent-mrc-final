"""Tests for JobSearchAgent, focusing on validate_config."""

import pytest

from job_agent import ConfigValidationError, JobSearchAgent
from models import SearchConfig


def make_agent(**kwargs):
    defaults = dict(
        keywords=["python", "engineer"],
        location="New York",
        max_results=10,
        sources=["indeed"],
    )
    defaults.update(kwargs)
    return JobSearchAgent(SearchConfig(**defaults))


class TestValidateConfig:
    def test_valid_config_passes(self):
        make_agent().validate_config()  # should not raise

    def test_empty_keywords_raises(self):
        with pytest.raises(ConfigValidationError, match="keywords"):
            make_agent(keywords=[]).validate_config()

    def test_blank_location_raises(self):
        with pytest.raises(ConfigValidationError, match="location"):
            make_agent(location="   ").validate_config()

    def test_empty_location_raises(self):
        with pytest.raises(ConfigValidationError, match="location"):
            make_agent(location="").validate_config()

    def test_zero_max_results_raises(self):
        with pytest.raises(ConfigValidationError, match="max_results"):
            make_agent(max_results=0).validate_config()

    def test_negative_max_results_raises(self):
        with pytest.raises(ConfigValidationError, match="max_results"):
            make_agent(max_results=-5).validate_config()

    def test_unsupported_source_raises(self):
        with pytest.raises(ConfigValidationError, match="unsupported sources"):
            make_agent(sources=["glassdoor"]).validate_config()

    def test_mixed_valid_invalid_sources_raises(self):
        with pytest.raises(ConfigValidationError, match="glassdoor"):
            make_agent(sources=["indeed", "glassdoor"]).validate_config()

    def test_both_supported_sources_pass(self):
        make_agent(sources=["indeed", "linkedin"]).validate_config()
