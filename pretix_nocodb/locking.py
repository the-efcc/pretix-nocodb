from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from django.core.cache import cache

# Long enough to outlive a slow schema sync (a handful of NocoDB meta calls),
# short enough that a worker killed mid-sync does not wedge the event until
# someone notices.
SCHEMA_LOCK_TTL = 300

# Saving one question fires a post_save per option, so the duplicate enqueues all
# land in the same commit; a few seconds also absorbs a user saving several
# questions in a row.
SCHEMA_DEBOUNCE_DELAY = 10


class SchemaSyncBusy(RuntimeError):
    """Another worker is already syncing this event's schema."""


def _lock_key(event_id: int) -> str:
    return f"pretix_nocodb:schema_lock:{event_id}"


def _claim_key(event_id: int) -> str:
    return f"pretix_nocodb:schema_claim:{event_id}"


@contextmanager
def schema_sync_lock(event_id: int) -> Iterator[None]:
    """Serialize schema syncs of one event across workers.

    NocoDB rewrites a select column by deleting and re-inserting every one of its
    options, so two overlapping writes to the same column collide on the
    ``(base_id, id)`` primary key behind it. Holding this lock is what keeps them
    from overlapping.
    """
    # cache.add is a single SETNX, so exactly one worker wins. Where no real
    # cache backend is configured pretix falls back to a dummy whose add() always
    # reports success: that degrades to the unsynchronized behaviour rather than
    # to a lock nobody can ever take.
    if not cache.add(_lock_key(event_id), True, timeout=SCHEMA_LOCK_TTL):
        raise SchemaSyncBusy(f"a schema sync is already running for event {event_id}")
    try:
        yield
    finally:
        cache.delete(_lock_key(event_id))


def claim_schema_sync(event_id: int) -> bool:
    """Return True for the first caller of a burst, False for the duplicates.

    The claim only has to survive until the task it belongs to starts running, so
    it expires with the debounce window. Letting it lapse early costs one
    redundant task, which the lock serializes; holding it longer would swallow
    real edits whenever a task is lost.
    """
    return bool(cache.add(_claim_key(event_id), True, timeout=SCHEMA_DEBOUNCE_DELAY))


def release_schema_sync_claim(event_id: int) -> None:
    cache.delete(_claim_key(event_id))
