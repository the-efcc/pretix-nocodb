from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


class NocoDBAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        payload: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


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
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=params,
            json=json,
            timeout=30,
        )
        if response.status_code >= 400:
            try:
                payload = response.json()
            except ValueError:
                payload = response.text
            raise NocoDBAPIError(
                f"NocoDB API error on {method} {path}",
                status_code=response.status_code,
                payload=payload,
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
        relation_type: str = "bt",
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v2/meta/tables/{table_id}/columns",
            json={
                "title": title,
                "childId": child_id,
                "parentId": parent_id,
                "type": relation_type,
                "uidt": "LinkToAnotherRecord",
            },
        )

    def update_column(self, column_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("PATCH", f"/api/v2/meta/columns/{column_id}", json=payload)

    def delete_column(self, column_id: str) -> Any:
        return self._request("DELETE", f"/api/v2/meta/columns/{column_id}")

    def list_records(
        self,
        table_id: str,
        *,
        where: str | None = None,
        fields: list[str] | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
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
