from __future__ import annotations

from types import SimpleNamespace

import pytest
from celery.exceptions import Retry

from pretix_nocodb.client import NocoDBAPIError
from pretix_nocodb.tasks import MAX_RETRY_DELAY, _retry_if_transient


class StubTask:
    default_retry_delay = 10

    def __init__(self, retries: int = 0):
        self.request = SimpleNamespace(retries=retries)
        self.retry_kwargs: dict | None = None

    def retry(self, *, exc, countdown):
        self.retry_kwargs = {"exc": exc, "countdown": countdown}
        return Retry(exc=exc, when=countdown)


def test_config_error_is_raised_without_retry():
    task = StubTask()
    exc = NocoDBAPIError("HTTP 404", status_code=404)

    with pytest.raises(NocoDBAPIError):
        _retry_if_transient(task, exc)

    assert task.retry_kwargs is None


def test_transient_error_is_retried_with_exponential_backoff():
    exc = NocoDBAPIError("HTTP 503", status_code=503)

    for retries, expected_countdown in [(0, 10), (1, 20), (2, 40), (10, MAX_RETRY_DELAY)]:
        task = StubTask(retries=retries)
        with pytest.raises(Retry):
            _retry_if_transient(task, exc)
        assert task.retry_kwargs == {"exc": exc, "countdown": expected_countdown}


def test_retry_after_header_overrides_backoff():
    task = StubTask()
    exc = NocoDBAPIError("HTTP 429", status_code=429, retry_after=42)

    with pytest.raises(Retry):
        _retry_if_transient(task, exc)

    assert task.retry_kwargs == {"exc": exc, "countdown": 42}


def test_network_error_is_retried():
    task = StubTask(retries=1)
    exc = NocoDBAPIError("connection refused")

    with pytest.raises(Retry):
        _retry_if_transient(task, exc)

    assert task.retry_kwargs == {"exc": exc, "countdown": 20}
