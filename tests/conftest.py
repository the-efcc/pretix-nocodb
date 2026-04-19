import inspect
from datetime import timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from django.test import RequestFactory
from django.utils import translation
from django.utils.timezone import now
from django_scopes import scopes_disabled
from pretix.base.models import Event, Order, Organizer


@pytest.hookimpl(hookwrapper=True)
def pytest_fixture_setup(fixturedef, request):
    if inspect.isgeneratorfunction(fixturedef.func):
        yield
    else:
        with scopes_disabled():
            yield


@pytest.fixture(autouse=True)
def reset_locale():
    translation.activate("en")


@pytest.fixture(autouse=True)
def no_messages(monkeypatch):
    monkeypatch.setattr("django.contrib.messages.api.add_message", lambda *args, **kwargs: None)


@pytest.fixture(autouse=True)
def disable_scopes():
    with scopes_disabled():
        yield


# Request factory


@pytest.fixture
def rf():
    return RequestFactory()


# Database fixtures


@pytest.fixture
def organizer():
    return Organizer.objects.create(name="Dummy", slug="dummy")


@pytest.fixture
def event(organizer):
    event = Event.objects.create(
        organizer=organizer,
        name="Dummy",
        slug="dummy",
        date_from=now(),
        live=True,
        plugins="pretix_nocodb",
    )
    event.settings.set("plugin_nocodb_enabled", True)
    event.settings.set("plugin_nocodb_api_url", "https://app.nocodb.test")
    event.settings.set("plugin_nocodb_api_token", "test-secret")
    event.settings.set("plugin_nocodb_workspace_id", "workspace-1")
    return event


@pytest.fixture
def order(event, organizer):
    return Order.objects.create(
        code="FOOBAR",
        event=event,
        email="dummy@dummy.test",
        status=Order.STATUS_PENDING,
        datetime=now(),
        expires=now() + timedelta(days=10),
        total=Decimal("13.37"),
        sales_channel=organizer.sales_channels.get(identifier="web"),
    )


@pytest.fixture
def env(event, order):
    return event, order


# Test mode fixtures


@pytest.fixture
def testmode_organizer():
    return Organizer.objects.create(name="TestOrg", slug="testorg")


@pytest.fixture
def testmode_event(testmode_organizer):
    event = Event.objects.create(
        organizer=testmode_organizer,
        name="Test Event",
        slug="testevent",
        date_from=now(),
        live=False,
        testmode=True,
        plugins="pretix_nocodb",
    )
    event.settings.set("plugin_nocodb_enabled", True)
    event.settings.set("plugin_nocodb_api_url", "https://app.nocodb.test")
    event.settings.set("plugin_nocodb_api_token", "live-secret")
    event.settings.set("plugin_nocodb_workspace_id", "workspace-1")
    return event


@pytest.fixture
def testmode_order(testmode_event, testmode_organizer):
    return Order.objects.create(
        code="TESTORDER",
        event=testmode_event,
        email="test@test.test",
        status=Order.STATUS_PENDING,
        datetime=now(),
        expires=now() + timedelta(days=10),
        total=Decimal("10.00"),
        sales_channel=testmode_organizer.sales_channels.get(identifier="web"),
    )


@pytest.fixture
def testmode_env(testmode_event, testmode_order):
    return testmode_event, testmode_order


# Convenience fixtures using factories


@pytest.fixture
def mock_nocodb_config():
    return {}


@pytest.fixture
def mock_request():
    request = MagicMock()
    request.session = {}
    request.META = {"CSRF_COOKIE": "test-csrf-token"}
    request.POST = {}
    request.headers = {}
    request.body = b"{}"
    request.content_type = "application/json"
    return request


@pytest.fixture
def mock_event():
    event = MagicMock()
    event.slug = "test-event"
    event.currency = "CHF"
    event.organizer = MagicMock()
    event.organizer.slug = "test-org"
    event.settings = MagicMock()
    return event
