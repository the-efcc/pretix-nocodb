from __future__ import annotations

import pytest
import requests

from pretix_nocodb.client import NocoDBAPIError, NocoDBClient


class StubSession:
    def __init__(self, response=None, exc=None):
        self.headers = {}
        self.response = response
        self.exc = exc
        self.calls = []

    def request(self, **kwargs):
        self.calls.append(kwargs)
        if self.exc is not None:
            raise self.exc
        return self.response


def _response(status_code: int, *, content: bytes = b"", headers: dict | None = None):
    response = requests.Response()
    response.status_code = status_code
    response._content = content
    response.headers.update(headers or {})
    return response


def _client(session) -> NocoDBClient:
    return NocoDBClient("https://app.nocodb.test", "token", session=session)


def test_error_message_includes_status_and_payload_message():
    session = StubSession(
        response=_response(404, content=b'{"msg": "Base not found"}'),
    )

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).list_tables("pbase404")

    assert str(excinfo.value) == (
        "NocoDB API error on GET /api/v2/meta/bases/pbase404/tables: HTTP 404 - Base not found"
    )
    assert excinfo.value.status_code == 404
    assert not excinfo.value.is_transient


def test_error_message_includes_non_json_body():
    session = StubSession(response=_response(502, content=b"Bad  Gateway\n"))

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).list_tables("pbase502")

    assert "HTTP 502 - Bad Gateway" in str(excinfo.value)
    assert excinfo.value.is_transient


def test_rate_limit_is_transient_and_carries_retry_after():
    session = StubSession(
        response=_response(
            429,
            content=b'{"message": "Too many requests"}',
            headers={"Retry-After": "17"},
        ),
    )

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).list_tables("pbase429")

    assert excinfo.value.is_transient
    assert excinfo.value.retry_after == 17


def test_auth_error_is_not_transient():
    session = StubSession(response=_response(401, content=b'{"msg": "Invalid token"}'))

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).list_tables("pbase401")

    assert not excinfo.value.is_transient
    assert excinfo.value.retry_after is None


def test_concurrent_select_column_write_is_transient():
    # What NocoDB answers when two schema syncs rewrite the same select column:
    # a 400 wrapping the Postgres unique violation on the option table.
    session = StubSession(
        response=_response(
            400,
            content=(
                b'{"code": "23505", "error": "ERR_DATABASE_OP_FAILED", "message":'
                b" \"base_id field unique constraint violation. Value 'pbase,"
                b" sopt' already exists.\"}"
            ),
        ),
    )

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).update_column("ccol", {"title": "Variation name"})

    assert excinfo.value.is_write_conflict
    assert excinfo.value.is_transient


def test_other_bad_requests_stay_non_transient():
    session = StubSession(response=_response(400, content=b'{"msg": "Invalid column type"}'))

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).update_column("ccol", {"uidt": "Nonsense"})

    assert not excinfo.value.is_write_conflict
    assert not excinfo.value.is_transient


def test_non_json_error_body_is_not_a_write_conflict():
    session = StubSession(response=_response(400, content=b"23505"))

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).update_column("ccol", {"title": "Variation name"})

    assert not excinfo.value.is_write_conflict


def test_network_error_is_wrapped_and_transient():
    session = StubSession(exc=requests.ConnectionError("connection refused"))

    with pytest.raises(NocoDBAPIError) as excinfo:
        _client(session).list_tables("pbase")

    assert "ConnectionError" in str(excinfo.value)
    assert excinfo.value.status_code is None
    assert excinfo.value.is_transient


def test_repr_hides_the_api_token():
    session = StubSession(response=_response(200))
    client = NocoDBClient("https://app.nocodb.test", "nc_pat_secret", session=session)

    rendered = repr(client)

    assert "nc_pat_secret" not in rendered
    assert rendered == "NocoDBClient(base_url='https://app.nocodb.test', api_token='***')"


def test_successful_request_returns_json():
    session = StubSession(response=_response(200, content=b'{"list": [{"id": "m_1"}]}'))

    assert _client(session).list_tables("pbase") == [{"id": "m_1"}]


def test_duplicate_base_posts_options_and_returns_ids():
    session = StubSession(
        response=_response(200, content=b'{"id": "job_1", "base_id": "p_new"}'),
    )

    result = _client(session).duplicate_base("p_src")

    assert result == {"id": "job_1", "base_id": "p_new"}
    call = session.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "https://app.nocodb.test/api/v2/meta/duplicate/p_src"
    assert call["json"] == {
        "options": {
            "excludeData": True,
            "excludeViews": False,
            "excludeHooks": True,
        }
    }


def test_update_base_patches_the_base_meta():
    session = StubSession(response=_response(200, content=b'{"id": "pbase", "title": "Retreat"}'))

    _client(session).update_base("pbase", {"title": "Retreat"})

    call = session.calls[0]
    assert call["method"] == "PATCH"
    assert call["url"] == "https://app.nocodb.test/api/v2/meta/bases/pbase"
    assert call["json"] == {"title": "Retreat"}
