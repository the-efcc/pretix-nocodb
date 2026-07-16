from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

PAYLOAD_SUMMARY_LENGTH = 500
TRANSIENT_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class NocoDBAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        payload: Any = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload
        self.retry_after = retry_after

    @property
    def is_transient(self) -> bool:
        # status_code None means the request never got an HTTP response
        # (connection error, timeout), which is worth retrying too.
        return self.status_code is None or self.status_code in TRANSIENT_STATUS_CODES


def _summarize_payload(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, dict):
        for key in ("message", "msg", "error"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value[:PAYLOAD_SUMMARY_LENGTH]
    text = payload if isinstance(payload, str) else repr(payload)
    return " ".join(text.split())[:PAYLOAD_SUMMARY_LENGTH]


def _parse_retry_after(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return max(int(value), 1)
    except ValueError:
        return None


@dataclass(slots=True)
class NocoDBClient:
    base_url: str
    api_token: str
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        if self.session is None:
            self.session = requests.Session()
        assert self.session is not None
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "xc-token": self.api_token,
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> Any:
        assert self.session is not None
        try:
            response = self.session.request(
                method=method,
                url=f"{self.base_url}{path}",
                params=params,
                json=json,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise NocoDBAPIError(
                f"NocoDB request failed on {method} {path}: {type(exc).__name__}: {exc}"
            ) from exc
        if response.status_code >= 400:
            try:
                payload = response.json()
            except ValueError:
                payload = response.text
            message = f"NocoDB API error on {method} {path}: HTTP {response.status_code}"
            summary = _summarize_payload(payload)
            if summary:
                message = f"{message} - {summary}"
            raise NocoDBAPIError(
                message,
                status_code=response.status_code,
                payload=payload,
                retry_after=_parse_retry_after(response.headers.get("Retry-After")),
            )
        if not response.content:
            return None
        return response.json()

    def list_bases(self, workspace_id: str = "", *, page_size: int = 200) -> list[dict[str, Any]]:
        path = (
            f"/api/v2/meta/workspaces/{workspace_id}/bases"
            if workspace_id
            else "/api/v2/meta/bases/"
        )
        response = self._request("GET", path, params={"pageSize": page_size})
        return response.get("list", [])

    def create_base(self, title: str, *, workspace_id: str = "") -> dict[str, Any]:
        path = (
            f"/api/v2/meta/workspaces/{workspace_id}/bases"
            if workspace_id
            else "/api/v2/meta/bases/"
        )
        payload: dict[str, Any] = {"title": title}
        if workspace_id:
            payload["fk_workspace_id"] = workspace_id
        return self._request("POST", path, json=payload)

    def list_tables(self, base_id: str, *, page_size: int = 200) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/api/v2/meta/bases/{base_id}/tables",
            params={"pageSize": page_size},
        )
        return response.get("list", [])

    def get_table(self, table_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v2/meta/tables/{table_id}")

    def create_table(
        self,
        base_id: str,
        *,
        title: str,
        columns: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v2/meta/bases/{base_id}/tables",
            json={"title": title, "table_name": title, "columns": columns},
        )

    def create_column(self, table_id: str, column: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/api/v2/meta/tables/{table_id}/columns", json=column)

    def create_link_column(
        self,
        table_id: str,
        *,
        title: str,
        child_id: str,
        parent_id: str,
        relation_type: str = "mo",
    ) -> dict[str, Any]:
        # NocoDB v2 stores all link columns junction-backed. The link column is
        # created on `table_id`; childId/parentId are reversed vs the legacy
        # naming (childId = referenced table, parentId = holder table).
        return self._request(
            "POST",
            f"/api/v2/meta/tables/{table_id}/columns",
            json={
                "title": title,
                "childId": child_id,
                "parentId": parent_id,
                "type": relation_type,
                "uidt": "Links",
            },
        )

    def link_records(
        self,
        table_id: str,
        link_column_id: str,
        record_id: int,
        linked_id: int,
    ) -> Any:
        return self._request(
            "POST",
            f"/api/v2/tables/{table_id}/links/{link_column_id}/records/{record_id}",
            json={"Id": linked_id},
        )

    def list_linked_records(
        self,
        table_id: str,
        link_column_id: str,
        record_id: int,
        *,
        fields: list[str] | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if fields:
            params["fields"] = ",".join(fields)
        response = self._request(
            "GET",
            f"/api/v2/tables/{table_id}/links/{link_column_id}/records/{record_id}",
            params=params,
        )
        if isinstance(response, dict):
            return response.get("list", [])
        return response or []

    def list_views(self, table_id: str) -> list[dict[str, Any]]:
        response = self._request("GET", f"/api/v2/meta/tables/{table_id}/views")
        return response.get("list", [])

    def update_view(self, view_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("PATCH", f"/api/v2/meta/views/{view_id}", json=payload)

    def list_view_columns(self, view_id: str) -> list[dict[str, Any]]:
        response = self._request("GET", f"/api/v2/meta/views/{view_id}/columns")
        return response.get("list", [])

    def update_view_column(
        self,
        view_id: str,
        view_column_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._request(
            "PATCH",
            f"/api/v2/meta/views/{view_id}/columns/{view_column_id}",
            json=payload,
        )

    def update_column(self, column_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("PATCH", f"/api/v2/meta/columns/{column_id}", json=payload)

    def set_primary_column(self, column_id: str) -> Any:
        return self._request("POST", f"/api/v2/meta/columns/{column_id}/primary")

    def delete_column(self, column_id: str) -> Any:
        return self._request("DELETE", f"/api/v2/meta/columns/{column_id}")

    def list_records(
        self,
        table_id: str,
        *,
        where: str | None = None,
        fields: list[str] | None = None,
        offset: int = 0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if offset:
            params["offset"] = offset
        if where:
            params["where"] = where
        if fields:
            params["fields"] = ",".join(fields)
        response = self._request("GET", f"/api/v2/tables/{table_id}/records", params=params)
        return response.get("list", [])

    def create_records(self, table_id: str, records: list[dict[str, Any]]) -> Any:
        return self._request("POST", f"/api/v2/tables/{table_id}/records", json=records)

    def update_records(self, table_id: str, records: list[dict[str, Any]]) -> Any:
        return self._request("PATCH", f"/api/v2/tables/{table_id}/records", json=records)

    def delete_records(self, table_id: str, records: list[dict[str, Any]]) -> Any:
        return self._request("DELETE", f"/api/v2/tables/{table_id}/records", json=records)
