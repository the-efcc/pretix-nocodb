from __future__ import annotations

import pytest
from django.test import override_settings

from pretix_nocodb.locking import (
    SchemaSyncBusy,
    claim_schema_sync,
    release_schema_sync_claim,
    schema_sync_lock,
)


def schema_lock_is_held(event_id: int) -> bool:
    try:
        with schema_sync_lock(event_id):
            return False
    except SchemaSyncBusy:
        return True


def test_lock_is_exclusive_per_event(real_cache):
    with schema_sync_lock(7):
        assert schema_lock_is_held(7) is True


def test_lock_does_not_block_another_event(real_cache):
    with schema_sync_lock(7):
        assert schema_lock_is_held(8) is False


def test_lock_is_released_after_the_block(real_cache):
    with schema_sync_lock(7):
        pass

    assert schema_lock_is_held(7) is False


def test_lock_is_released_when_the_block_raises(real_cache):
    with pytest.raises(ValueError), schema_sync_lock(7):
        raise ValueError("boom")

    # A sync that died must not wedge the event until the key expires.
    assert schema_lock_is_held(7) is False


def test_only_the_first_claim_of_a_burst_wins(real_cache):
    assert claim_schema_sync(7) is True
    assert claim_schema_sync(7) is False
    assert claim_schema_sync(7) is False


def test_claims_are_per_event(real_cache):
    assert claim_schema_sync(7) is True
    assert claim_schema_sync(8) is True


def test_releasing_a_claim_reopens_the_window(real_cache):
    assert claim_schema_sync(7) is True
    release_schema_sync_claim(7)
    assert claim_schema_sync(7) is True


def test_a_cacheless_deploy_degrades_to_no_deduplication(real_cache):
    # pretix falls back to a dummy cache when neither redis nor memcached is
    # configured. The lock then has to stay out of the way -- the old behaviour,
    # racy but working -- rather than become one nobody can ever take.
    with override_settings(
        CACHES={"default": {"BACKEND": "pretix.helpers.cache.CustomDummyCache"}}
    ):
        with schema_sync_lock(7):
            assert schema_lock_is_held(7) is False

        assert claim_schema_sync(7) is True
        assert claim_schema_sync(7) is True
