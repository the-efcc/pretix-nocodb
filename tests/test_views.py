from __future__ import annotations

import pytest

from pretix_nocodb.client import NocoDBAPIError
from pretix_nocodb.forms import NocoDBSettingsForm
from pretix_nocodb.sync import TABLE_PARTICIPANTS, NocoDBSyncService
from pretix_nocodb.tasks import sync_all_orders_to_nocodb
from pretix_nocodb.views import NocoDBSyncNowView
from tests.test_sync import FakeNocoDBClient

pytestmark = pytest.mark.django_db


@pytest.fixture
def fake_client(monkeypatch):
    """Hand every NocoDBSyncService built without an explicit client the same fake."""
    client = FakeNocoDBClient()
    monkeypatch.setattr("pretix_nocodb.sync.NocoDBClient", lambda *args, **kwargs: client)
    return client


def _sync_now(rf, event):
    request = rf.post("/control/event/dummy/dummy/nocodb/sync")
    request.event = event
    request.organizer = event.organizer
    return NocoDBSyncNowView().post(request)


def _post_settings(event, **data) -> NocoDBSettingsForm:
    form = NocoDBSettingsForm(data=data, obj=event)
    assert form.is_valid(), form.errors
    form.save()
    return form


def test_sync_now_persists_the_base_id_before_returning(rf, event, fake_client):
    # The settings page is rendered again right after this response, so the id
    # of the base the sync will use has to be stored by then.
    response = _sync_now(rf, event)

    assert response.status_code == 302
    assert len(fake_client.bases) == 1
    assert event.settings.get("plugin_nocodb_base_id") == fake_client.bases[0]["id"]

    form = NocoDBSettingsForm(obj=event)
    assert form.initial["plugin_nocodb_base_id"] == fake_client.bases[0]["id"]


def test_sync_now_twice_reuses_the_same_base(rf, event, fake_client):
    _sync_now(rf, event)
    _sync_now(rf, event)

    assert len(fake_client.bases) == 1


def test_sync_now_does_not_provision_when_sync_is_disabled(rf, event, fake_client):
    event.settings.set("plugin_nocodb_enabled", False)

    _sync_now(rf, event)

    assert fake_client.bases == []
    assert event.settings.get("plugin_nocodb_base_id") == ""


def test_sync_now_reports_a_failing_base_creation(rf, event, monkeypatch):
    class FailingClient(FakeNocoDBClient):
        def create_base(self, title, *, workspace_id=""):  # noqa: ARG002
            raise NocoDBAPIError("NocoDB API error: HTTP 401", status_code=401)

    monkeypatch.setattr("pretix_nocodb.sync.NocoDBClient", lambda *a, **kw: FailingClient())
    errors = []
    monkeypatch.setattr(
        "pretix_nocodb.views.messages.error", lambda request, message: errors.append(message)
    )

    response = _sync_now(rf, event)

    assert response.status_code == 302
    assert len(errors) == 1
    assert "HTTP 401" in str(errors[0])
    assert event.settings.get("plugin_nocodb_base_id") == ""


def test_sync_now_duplicates_without_waiting_for_the_copy_job(rf, event, fake_client, monkeypatch):
    template = fake_client.create_base("Template")
    event.settings.set("plugin_nocodb_source_base_id", template["id"])
    event.settings.set("plugin_nocodb_base_creation_mode", "duplicate")

    # The full sync runs in a worker, so keep it out of this request the way
    # celery does outside of tests...
    enqueued = []
    monkeypatch.setattr(
        sync_all_orders_to_nocodb, "apply_async", lambda **kwargs: enqueued.append(kwargs)
    )
    # ...leaving the request with the duplication call alone: waiting for
    # NocoDB's background copy job belongs in the sync task.
    monkeypatch.setattr(
        NocoDBSyncService,
        "_wait_for_participants_table",
        lambda *args, **kwargs: pytest.fail("the request must not wait for the copy job"),
    )

    _sync_now(rf, event)

    assert enqueued == [{"kwargs": {"event": event.pk}}]

    new_base_id = event.settings.get("plugin_nocodb_base_id")
    assert new_base_id
    assert new_base_id != template["id"]
    assert event.settings.get("plugin_nocodb_base_duplication_pending", as_type=bool) is True


def test_sync_after_duplication_waits_for_the_copied_tables(event, monkeypatch):
    monkeypatch.setattr("pretix_nocodb.sync.BASE_DUPLICATION_POLL_INTERVAL", 0)
    polls = []

    class SlowCopyClient(FakeNocoDBClient):
        def __init__(self):
            super().__init__()
            self.copying: dict[str, dict] = {}

        def duplicate_base(self, base_id, **kwargs):
            # NocoDB answers with the new base long before its tables exist.
            result = super().duplicate_base(base_id, **kwargs)
            self.copying = {
                table_id: self.tables.pop(table_id)
                for table_id, table in list(self.tables.items())
                if table["base_id"] == result["base_id"]
            }
            return result

        def list_tables(self, base_id, *, _page_size=200):
            polls.append(base_id)
            tables = super().list_tables(base_id, _page_size=_page_size)
            # The copy job finishes while the sync polls for it.
            self.tables.update(self.copying)
            self.copying = {}
            return tables

    client = SlowCopyClient()
    template_base = client.create_base("Template")
    client.create_table(template_base["id"], title=TABLE_PARTICIPANTS, columns=[])
    event.settings.set("plugin_nocodb_source_base_id", template_base["id"])
    event.settings.set("plugin_nocodb_base_creation_mode", "duplicate")

    # The base is provisioned by one process (the settings page)...
    NocoDBSyncService(event, client=client).ensure_base()
    new_base_id = event.settings.get("plugin_nocodb_base_id")
    assert event.settings.get("plugin_nocodb_base_duplication_pending", as_type=bool) is True

    # ...and the sync that adopts the copied tables runs in another, so it has to
    # learn from the stored flag that a copy job is still in flight.
    NocoDBSyncService(event, client=client).sync_schema()

    assert polls.count(new_base_id) > 1
    copied_tables = [t for t in client.tables.values() if t["base_id"] == new_base_id]
    assert len(copied_tables) == 1
    assert event.settings.get("plugin_nocodb_base_duplication_pending", as_type=bool) is False


def test_settings_form_keeps_the_base_id_when_the_field_is_left_empty(event, fake_client):
    # A page rendered before the first sync stored the id, submitted afterwards.
    stale_page = {"plugin_nocodb_enabled": "on", "plugin_nocodb_base_id": ""}
    event.settings.set("plugin_nocodb_base_id", "p_existing")

    _post_settings(event, **stale_page)

    assert event.settings.get("plugin_nocodb_base_id") == "p_existing"


def test_settings_form_moves_the_event_to_a_pasted_base(event):
    event.settings.set("plugin_nocodb_base_id", "p_existing")

    _post_settings(event, plugin_nocodb_enabled="on", plugin_nocodb_base_id="  p_other  ")

    assert event.settings.get("plugin_nocodb_base_id") == "p_other"


def test_settings_form_provisions_when_no_base_is_bound_yet(event):
    _post_settings(event, plugin_nocodb_enabled="on", plugin_nocodb_base_id="")

    assert event.settings.get("plugin_nocodb_base_id") == ""


def test_settings_form_saving_a_stale_page_does_not_cause_a_second_base(rf, event, fake_client):
    # The whole reported failure, end to end: sync now, save the settings page
    # that was rendered from before the base existed, sync again.
    _sync_now(rf, event)
    first_base_id = event.settings.get("plugin_nocodb_base_id")

    _post_settings(event, plugin_nocodb_enabled="on", plugin_nocodb_base_id="")
    _sync_now(rf, event)

    assert event.settings.get("plugin_nocodb_base_id") == first_base_id
    assert len(fake_client.bases) == 1
