from __future__ import annotations

import pytest
from django.utils.timezone import now
from pretix.base.models import Event

from pretix_nocodb.forms import NocoDBSettingsForm
from pretix_nocodb.plugin_settings import (
    BASE_MODE_DUPLICATE,
    BASE_MODE_NEW,
    NocoDBConfig,
)

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
