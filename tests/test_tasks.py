from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from celery.exceptions import Retry
from pretix.base.models import Item, OrderPosition

from pretix_nocodb.client import NocoDBAPIError
from pretix_nocodb.sync import PARTICIPANT_KEY_FIELD, TABLE_PARTICIPANTS, NocoDBSyncService
from pretix_nocodb.tasks import (
    MAX_RETRY_DELAY,
    _known_position_ids,
    _retry_if_transient,
    sync_all_orders_to_nocodb,
)
from tests.test_sync import FakeNocoDBClient, _attach_base


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


def _participants(client: FakeNocoDBClient) -> tuple[str, list[dict]]:
    table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    return table["id"], client.records[table["id"]]


def _row_for(client: FakeNocoDBClient, position_pk: int) -> dict:
    _, rows = _participants(client)
    matching = [row for row in rows if row[PARTICIPANT_KEY_FIELD] == position_pk]
    assert len(matching) == 1, f"expected exactly one row for position {position_pk}"
    return matching[0]


def _run_full_sync(event, client: FakeNocoDBClient, monkeypatch) -> None:
    monkeypatch.setattr(
        "pretix_nocodb.tasks.NocoDBSyncService",
        lambda ev: NocoDBSyncService(ev, client=client),
    )
    sync_all_orders_to_nocodb.apply(kwargs={"event": event.pk}).get()


@pytest.mark.django_db
def test_known_position_ids_includes_canceled_positions(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    active = OrderPosition.objects.create(order=order, item=item, price=Decimal("10"))
    canceled = OrderPosition.objects.create(order=order, item=item, price=Decimal("10"))
    canceled.canceled = True
    canceled.save(update_fields=["canceled"])

    # OrderPosition.objects would hide the canceled one and get its row pruned.
    assert _known_position_ids(event) == {active.pk, canceled.pk}


@pytest.mark.django_db
def test_full_sync_keeps_rows_of_canceled_positions(event, order, monkeypatch):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada"
    )
    position.canceled = True
    position.save(update_fields=["canceled"])
    # A second, active position keeps the snapshot non-empty, so the prune really
    # runs instead of being skipped by the empty-snapshot guard.
    still_active = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Grace"
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    _run_full_sync(event, client, monkeypatch)

    _, rows = _participants(client)
    assert sorted(row[PARTICIPANT_KEY_FIELD] for row in rows) == sorted(
        [position.pk, still_active.pk]
    )


@pytest.mark.django_db
def test_full_sync_keeps_hand_added_data_across_cancel_and_return(event, order, monkeypatch):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada"
    )
    # Keeps the position snapshot non-empty once `position` is canceled below, so
    # the prune is exercised rather than skipped by the empty-snapshot guard.
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Grace"
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    _run_full_sync(event, client, monkeypatch)

    table_id, _ = _participants(client)
    row_id = _row_for(client, position.pk)["Id"]

    # A column added by hand in NocoDB, filled in for this participant.
    client.create_column(table_id, {"title": "Dietary notes", "uidt": "SingleLineText"})
    client.update_records(table_id, [{"Id": row_id, "Dietary notes": "vegan"}])

    position.canceled = True
    position.save(update_fields=["canceled"])
    _run_full_sync(event, client, monkeypatch)

    row = _row_for(client, position.pk)
    assert row["Id"] == row_id
    assert row["canceled"] is True
    assert row["Dietary notes"] == "vegan"

    position.canceled = False
    position.save(update_fields=["canceled"])
    _run_full_sync(event, client, monkeypatch)

    # Same NocoDB record throughout, so the hand-added cell is still on it.
    row = _row_for(client, position.pk)
    assert row["Id"] == row_id
    assert row["canceled"] is False
    assert row["Dietary notes"] == "vegan"


@pytest.mark.django_db
def test_full_sync_skips_prune_when_the_position_snapshot_is_empty(event, order, monkeypatch):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada"
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    _run_full_sync(event, client, monkeypatch)
    assert len(_participants(client)[1]) == 1

    monkeypatch.setattr("pretix_nocodb.tasks._known_position_ids", lambda _event: set())
    _run_full_sync(event, client, monkeypatch)

    _, rows = _participants(client)
    assert [row[PARTICIPANT_KEY_FIELD] for row in rows] == [position.pk]
