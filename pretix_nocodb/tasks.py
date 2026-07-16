from __future__ import annotations

from typing import Any, NoReturn

from django_scopes import scopes_disabled
from pretix.base.models import Order, OrderPosition
from pretix.base.services.tasks import EventTask
from pretix.celery_app import app

from .client import NocoDBAPIError
from .sync import NocoDBSyncService

MAX_RETRY_DELAY = 300


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
        if service.sync_schema() is None:
            return
        with scopes_disabled():
            orders = list(Order.objects.filter(event=event))
            position_ids = set(
                OrderPosition.objects.filter(order__event=event).values_list("pk", flat=True)
            )
        for order in orders:
            service.sync_order(order)
        service.prune_deleted_rows(active_position_ids=position_ids)
    except NocoDBAPIError as exc:
        _retry_if_transient(self, exc)
