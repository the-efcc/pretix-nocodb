from __future__ import annotations

import pytest
from django.utils.timezone import now
from pretix.base.models import Event

from pretix_nocodb.forms import NocoDBSettingsForm
from pretix_nocodb.locking import SCHEMA_DEBOUNCE_DELAY, release_schema_sync_claim
from pretix_nocodb.plugin_settings import (
    BASE_MODE_DUPLICATE,
    BASE_MODE_NEW,
    NocoDBConfig,
)
from pretix_nocodb.signals import _dispatch_schema_sync

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def nocodb_enabled_for_organizer(organizer):
    # The plugin is a hybrid event/organizer plugin, so pretix only routes
    # event_copy_data to it when it is enabled on the organizer too.
    organizer.plugins = "pretix_nocodb"
    organizer.save(update_fields=["plugins"])


def _copy_of(source: Event, slug: str) -> Event:
    """Clone ``source`` the way pretix' event wizard and clone API do."""
    copy = Event.objects.create(
        organizer=source.organizer,
        name="Copy",
        slug=slug,
        date_from=now(),
        plugins="pretix_nocodb",
    )
    # Copies the whole settings store of the source event, then fires
    # event_copy_data -- which is what our receiver reacts to.
    copy.copy_data_from(source)
    return copy


def test_copy_does_not_inherit_the_source_base(event):
    event.settings.set("plugin_nocodb_base_id", "p_template")
    event.settings.set("plugin_nocodb_participants_table_id", "m_participants")
    event.settings.set("plugin_nocodb_participants_view_defaults_view_id", "v_all")
    event.settings.set("plugin_nocodb_base_duplication_pending", True)
    copy = _copy_of(event, "copy")

    config = NocoDBConfig.from_event(copy)
    assert config.base_id == ""
    assert config.participants_table_id == ""
    assert copy.settings.get("plugin_nocodb_participants_view_defaults_view_id") == ""
    # The copy has no base yet, so it cannot be waiting for one to be copied.
    assert config.base_duplication_pending is False
    # The source keeps its own base.
    assert NocoDBConfig.from_event(event).base_id == "p_template"


def test_copy_records_the_source_base_and_preselects_duplication(event):
    event.settings.set("plugin_nocodb_base_id", "p_template")
    copy = _copy_of(event, "copy")

    config = NocoDBConfig.from_event(copy)
    assert config.source_base_id == "p_template"
    assert config.base_creation_mode == BASE_MODE_DUPLICATE
    assert config.should_duplicate_source_base is True


def test_copy_without_source_base_keeps_creating_a_new_base(event):
    # The source event never provisioned a base.
    copy = _copy_of(event, "copy-empty")

    config = NocoDBConfig.from_event(copy)
    assert config.source_base_id == ""
    assert config.base_creation_mode == BASE_MODE_NEW
    assert config.should_duplicate_source_base is False


def test_copy_can_choose_how_to_provision_its_base(event):
    # The provisioning choice is only offered while the event has no base of its
    # own; inheriting the source's base used to hide it on every copy.
    event.settings.set("plugin_nocodb_base_id", "p_template")
    copy = _copy_of(event, "copy-form")

    form = NocoDBSettingsForm(obj=copy)
    assert "plugin_nocodb_base_creation_mode" in form.fields
    assert form.initial["plugin_nocodb_base_creation_mode"] == BASE_MODE_DUPLICATE


def test_config_should_duplicate_requires_mode_and_source(event):
    event.settings.set("plugin_nocodb_source_base_id", "p_template")
    event.settings.set("plugin_nocodb_base_creation_mode", BASE_MODE_DUPLICATE)
    assert NocoDBConfig.from_event(event).should_duplicate_source_base is True

    # Mode set but no source recorded.
    event.settings.set("plugin_nocodb_source_base_id", "")
    assert NocoDBConfig.from_event(event).should_duplicate_source_base is False


def test_a_burst_of_edits_enqueues_one_schema_sync(real_cache, monkeypatch):
    # Saving a question with ten options fires a post_save per option. Each one
    # used to enqueue its own sync_event_schema, and those then raced each
    # other's select-column writes inside NocoDB.
    enqueued: list[dict] = []
    monkeypatch.setattr(
        "pretix_nocodb.signals.sync_event_schema.apply_async",
        lambda **kwargs: enqueued.append(kwargs),
    )

    for _ in range(11):
        _dispatch_schema_sync(34)

    assert enqueued == [{"kwargs": {"event": 34}, "countdown": SCHEMA_DEBOUNCE_DELAY}]


def test_each_event_debounces_on_its_own(real_cache, monkeypatch):
    enqueued: list[dict] = []
    monkeypatch.setattr(
        "pretix_nocodb.signals.sync_event_schema.apply_async",
        lambda **kwargs: enqueued.append(kwargs),
    )

    _dispatch_schema_sync(34)
    _dispatch_schema_sync(35)

    assert [call["kwargs"]["event"] for call in enqueued] == [34, 35]


def test_an_edit_after_the_claim_is_released_enqueues_again(real_cache, monkeypatch):
    enqueued: list[dict] = []
    monkeypatch.setattr(
        "pretix_nocodb.signals.sync_event_schema.apply_async",
        lambda **kwargs: enqueued.append(kwargs),
    )

    _dispatch_schema_sync(34)
    # What the task does when it starts running.
    release_schema_sync_claim(34)
    _dispatch_schema_sync(34)

    assert len(enqueued) == 2
