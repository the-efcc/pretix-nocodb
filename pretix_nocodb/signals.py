from __future__ import annotations

from typing import Any, cast

from django.db import transaction
from django.db.models.signals import m2m_changed, post_delete, post_save
from django.dispatch import receiver
from django.urls import resolve, reverse
from django.utils.translation import gettext_lazy as _
from pretix.base.models import Question, QuestionOption
from pretix.base.signals import (
    order_approved,
    order_canceled,
    order_changed,
    order_denied,
    order_expired,
    order_expiry_changed,
    order_modified,
    order_paid,
    order_placed,
    order_reactivated,
)
from pretix.control.signals import nav_event_settings

from .plugin_settings import NocoDBConfig
from .tasks import sync_event_schema, sync_order_to_nocodb

QUESTION_ITEMS_THROUGH = cast(Any, Question.items).through


def _event_is_sync_enabled(event) -> bool:
    return NocoDBConfig.from_event(event).can_sync


def _enqueue_schema_sync(event) -> None:
    if not _event_is_sync_enabled(event):
        return
    transaction.on_commit(lambda: sync_event_schema.apply_async(kwargs={"event": event.pk}))


def _enqueue_order_sync(order) -> None:
    if not _event_is_sync_enabled(order.event):
        return
    transaction.on_commit(
        lambda: sync_order_to_nocodb.apply_async(
            kwargs={"event": order.event.pk, "order_id": order.pk}
        )
    )


@receiver(order_placed, dispatch_uid="nocodb_order_placed")
@receiver(order_paid, dispatch_uid="nocodb_order_paid")
@receiver(order_canceled, dispatch_uid="nocodb_order_canceled")
@receiver(order_reactivated, dispatch_uid="nocodb_order_reactivated")
@receiver(order_expired, dispatch_uid="nocodb_order_expired")
@receiver(order_expiry_changed, dispatch_uid="nocodb_order_expiry_changed")
@receiver(order_modified, dispatch_uid="nocodb_order_modified")
@receiver(order_changed, dispatch_uid="nocodb_order_changed")
@receiver(order_approved, dispatch_uid="nocodb_order_approved")
@receiver(order_denied, dispatch_uid="nocodb_order_denied")
def sync_order_on_change(sender, order, **kwargs) -> None:
    _enqueue_order_sync(order)


@receiver(post_save, sender=Question, dispatch_uid="nocodb_question_saved")
@receiver(post_delete, sender=Question, dispatch_uid="nocodb_question_deleted")
def sync_schema_on_question_change(sender, instance: Question, **kwargs) -> None:
    _enqueue_schema_sync(instance.event)


@receiver(post_save, sender=QuestionOption, dispatch_uid="nocodb_question_option_saved")
@receiver(post_delete, sender=QuestionOption, dispatch_uid="nocodb_question_option_deleted")
def sync_schema_on_question_option_change(sender, instance: QuestionOption, **kwargs) -> None:
    question = cast(Any, instance.question)
    _enqueue_schema_sync(question.event)


@receiver(m2m_changed, sender=QUESTION_ITEMS_THROUGH, dispatch_uid="nocodb_question_items_changed")
def sync_schema_on_question_items_change(sender, instance: Question, action: str, **kwargs) -> None:
    if action in {"post_add", "post_remove", "post_clear"}:
        _enqueue_schema_sync(instance.event)


@receiver(nav_event_settings, dispatch_uid="nocodb_nav_event_settings")
def add_event_settings_nav(sender, request, **kwargs):
    if not request.user.has_event_permission(
        request.organizer, request.event, "event.settings.general:write", request=request
    ):
        return []
    url = resolve(request.path_info)
    return [
        {
            "label": _("NocoDB"),
            "url": reverse(
                "plugins:pretix_nocodb:settings",
                kwargs={
                    "event": request.event.slug,
                    "organizer": request.organizer.slug,
                },
            ),
            "active": url.namespace == "plugins:pretix_nocodb",
        }
    ]
