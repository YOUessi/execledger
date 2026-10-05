from __future__ import annotations

import pytest
from pydantic import ValidationError

from execledger.models import (
    ExecutionSpec,
    ExecutionStatus,
    ResourceLimits,
    RetryPolicy,
)


def test_retry_policy_defaults_are_opt_in_by_attempt_budget():
    policy = RetryPolicy()
    assert policy.max_attempts == 1
    assert set(policy.retry_on) == {
        ExecutionStatus.FAILED,
        ExecutionStatus.TIMED_OUT,
        ExecutionStatus.INTERRUPTED,
    }


def test_retry_statuses_are_canonicalized_for_stable_request_hashes():
    first = ExecutionSpec(
        argv=["echo", "ok"],
        retry_policy={
            "max_attempts": 2,
            "retry_on": ["TIMED_OUT", "FAILED", "FAILED"],
        },
    )
    second = ExecutionSpec(
        argv=["echo", "ok"],
        retry_policy={
            "max_attempts": 2,
            "retry_on": ["FAILED", "TIMED_OUT"],
        },
    )
    assert first.model_dump(mode="json") == second.model_dump(mode="json")


def test_retry_policy_rejects_non_failure_statuses():
    with pytest.raises(ValidationError):
        RetryPolicy(
            max_attempts=2,
            retry_on=["SUCCEEDED"],
        )


def test_retry_delay_is_exponential_and_capped():
    policy = RetryPolicy(
        max_attempts=5,
        backoff_initial_seconds=2,
        backoff_multiplier=3,
        backoff_max_seconds=10,
    )
    assert policy.delay_after_attempt(1) == 2
    assert policy.delay_after_attempt(2) == 6
    assert policy.delay_after_attempt(3) == 10
    assert policy.delay_after_attempt(4) == 10


def test_resource_limits_are_opt_in_and_validate_bounds():
    limits = ResourceLimits()
    assert limits.configured() is False

    configured = ResourceLimits(
        max_cpu_seconds=5,
        max_memory_bytes=128 * 1024 * 1024,
        max_file_bytes=1024 * 1024,
        max_open_files=64,
        max_processes=8,
    )
    assert configured.configured() is True

    with pytest.raises(ValidationError):
        ResourceLimits(max_cpu_seconds=0)
    with pytest.raises(ValidationError):
        ResourceLimits(max_memory_bytes=1024)


def test_execution_spec_resource_limit_defaults_are_stable():
    first = ExecutionSpec(argv=["echo", "ok"])
    second = ExecutionSpec(argv=["echo", "ok"], resource_limits={})
    assert first.model_dump(mode="json") == second.model_dump(mode="json")
