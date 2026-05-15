from __future__ import annotations

from django_scopes import scopes_disabled
from pretix.base.models import Order, OrderPosition
from pretix.base.services.tasks import EventTask
from pretix.celery_app import app

from .sync import NocoDBSyncService


@app.task(base=EventTask, max_retries=3, default_retry_delay=10)
def sync_event_schema(event) -> None:
    NocoDBSyncService(event).sync_schema()


@app.task(base=EventTask, max_retries=3, default_retry_delay=10)
def sync_order_to_nocodb(event, order_id: int) -> None:
    with scopes_disabled():
        order = Order.objects.select_related("event", "event__organizer", "sales_channel").get(
            pk=order_id, event=event
        )
    NocoDBSyncService(event).sync_order(order)


@app.task(base=EventTask, max_retries=3, default_retry_delay=10)
def delete_order_from_nocodb(event, order_code: str, position_ids: list[int] | None = None) -> None:
    NocoDBSyncService(event).delete_order(order_code, position_ids=position_ids)


@app.task(base=EventTask, max_retries=3, default_retry_delay=10)
def sync_all_orders_to_nocodb(event) -> None:
    service = NocoDBSyncService(event)
    if service.sync_schema() is None:
        return
    with scopes_disabled():
        orders = list(Order.objects.filter(event=event))
        position_ids = set(
            OrderPosition.objects.filter(order__event=event).values_list("pk", flat=True)
        )
    for order in orders:
        service.sync_order(order)
    service.prune_deleted_rows(
        active_order_codes={str(order.code) for order in orders},
        active_position_ids=position_ids,
    )
