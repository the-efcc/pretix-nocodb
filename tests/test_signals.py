from __future__ import annotations

import pytest
from django.utils.timezone import now
from pretix.base.models import Event

from pretix_nocodb.plugin_settings import BASE_MODE_DUPLICATE, NocoDBConfig
from pretix_nocodb.signals import record_source_base_on_copy

pytestmark = pytest.mark.django_db


def _new_event(source: Event, slug: str) -> Event:
    return Event.objects.create(
        organizer=source.organizer,
        name="Copy",
        slug=slug,
        date_from=now(),
        plugins="pretix_nocodb",
    )


def test_record_source_base_on_copy_stores_source_base_id(event):
    event.settings.set("plugin_nocodb_base_id", "p_template")
    copy = _new_event(event, "copy")

    record_source_base_on_copy(sender=copy, other=event)

    assert copy.settings.get("plugin_nocodb_source_base_id") == "p_template"


def test_record_source_base_on_copy_is_noop_without_source_base(event):
    # The source event never provisioned a base.
    copy = _new_event(event, "copy-empty")

    record_source_base_on_copy(sender=copy, other=event)

    assert copy.settings.get("plugin_nocodb_source_base_id", default="") == ""


def test_config_should_duplicate_requires_mode_and_source(event):
    event.settings.set("plugin_nocodb_source_base_id", "p_template")
    event.settings.set("plugin_nocodb_base_creation_mode", BASE_MODE_DUPLICATE)
    assert NocoDBConfig.from_event(event).should_duplicate_source_base is True

    # Mode set but no source recorded.
    event.settings.set("plugin_nocodb_source_base_id", "")
    assert NocoDBConfig.from_event(event).should_duplicate_source_base is False
