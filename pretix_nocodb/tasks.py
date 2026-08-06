from __future__ import annotations

import logging
from typing import Any, NoReturn

from django_scopes import scopes_disabled
from pretix.base.models import Order, OrderPosition
from pretix.base.services.tasks import EventTask
from pretix.celery_app import app

from .client import NocoDBAPIError
from .sync import NocoDBSyncService

logger = logging.getLogger(__name__)

MAX_RETRY_DELAY = 300


def _known_position_ids(event) -> set[int]:
    # sync_order writes a row for every position in order.all_positions, canceled
    # ones included, so the reconciliation snapshot has to use the same unfiltered
    # set. OrderPosition.objects is pretix' ActivePositionManager (canceled=False);
    # using it here makes prune_deleted_rows delete the rows sync_order has just
    # written for canceled positions, and any column added by hand in NocoDB goes
    # with them. OrderPosition.all is the manager that keeps them.
    with scopes_disabled():
        return set(
            OrderPosition.all.filter(order__event=event).values_list("pk", flat=True)
        )


def _retry_if_transient(task: Any, exc: NocoDBAPIError) -> NoReturn:
    # Rate limits, outages and network failures are retried with exponential
    # backoff; configuration errors (invalid token, deleted base, ...) are
    # re-raised immediately so they surface without pointless retries.
    if not exc.is_transient:
        raise exc
    countdown = exc.retry_after or min(
        task.default_retry_delay * (2 ** task.request.retries), MAX_RETRY_DELAY
    )
    raise task.retry(exc=exc, countdown=countdown)


@app.task(base=EventTask, bind=True, max_retries=5, default_retry_delay=10)
def sync_event_schema(self, event) -> None:
    try:
        NocoDBSyncService(event).sync_schema()
    except NocoDBAPIError as exc:
        _retry_if_transient(self, exc)


@app.task(base=EventTask, bind=True, max_retries=5, default_retry_delay=10)
def sync_order_to_nocodb(self, event, order_id: int) -> None:
    with scopes_disabled():
        order = (
            Order.objects.select_related("event", "event__organizer", "sales_channel")
            .filter(pk=order_id, event=event)
            .first()
        )
    if order is None:
        # The order was deleted before the task ran; the delete task cleans up.
        return
    try:
        NocoDBSyncService(event).sync_order(order)
    except NocoDBAPIError as exc:
        _retry_if_transient(self, exc)


@app.task(base=EventTask, bind=True, max_retries=5, default_retry_delay=10)
def delete_order_from_nocodb(
    self, event, order_code: str, position_ids: list[int] | None = None
) -> None:
    try:
        NocoDBSyncService(event).delete_order(order_code, position_ids=position_ids)
    except NocoDBAPIError as exc:
        _retry_if_transient(self, exc)


@app.task(base=EventTask, bind=True, max_retries=5, default_retry_delay=10)
def sync_all_orders_to_nocodb(self, event) -> None:
    service = NocoDBSyncService(event)
    try:
        schema = service.sync_schema()
        if schema is None:
            return
        with scopes_disabled():
            orders = list(Order.objects.filter(event=event))
        for order in orders:
            service.sync_order(order, schema=schema)
        # Snapshot the known positions after the sync loop so orders placed
        # while it ran (and synced concurrently) are not pruned as stale.
        position_ids = _known_position_ids(event)
        if orders and not position_ids:
            # Every order of the event losing every position at once is far more
            # likely a broken query than real data. Pruning on that snapshot
            # empties the participants table and destroys whatever was added to
            # those rows in NocoDB; leaving stale rows behind is recoverable.
            logger.warning(
                "Skipping NocoDB prune for event %s: %d orders but no positions found",
                event.slug,
                len(orders),
            )
            return
        service.prune_deleted_rows(active_position_ids=position_ids)
    except NocoDBAPIError as exc:
        _retry_if_transient(self, exc)
