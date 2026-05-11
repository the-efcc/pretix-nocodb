from __future__ import annotations

from dataclasses import dataclass

from pretix.base.settings import SettingsSandbox, settings_hierarkey

PLUGIN_SETTINGS_PREFIX = ("plugin", "nocodb")

_DEFAULTS = {
    "plugin_nocodb_enabled": ("False", bool),
    "plugin_nocodb_api_url": ("https://app.nocodb.com", str),
    "plugin_nocodb_api_token": ("", str),
    "plugin_nocodb_workspace_id": ("", str),
    "plugin_nocodb_base_id": ("", str),
    "plugin_nocodb_orders_table_id": ("", str),
    "plugin_nocodb_participants_table_id": ("", str),
    "plugin_nocodb_questions_table_id": ("", str),
}


def register_settings_defaults() -> None:
    for key, (value, value_type) in _DEFAULTS.items():
        settings_hierarkey.add_default(key, value, value_type)


def settings_for_event(event) -> SettingsSandbox:
    return SettingsSandbox(*PLUGIN_SETTINGS_PREFIX, event)


@dataclass(slots=True)
class NocoDBConfig:
    enabled: bool
    api_url: str
    api_token: str
    workspace_id: str
    base_id: str
    orders_table_id: str
    participants_table_id: str
    questions_table_id: str

    @classmethod
    def from_event(cls, event) -> NocoDBConfig:
        settings = settings_for_event(event)
        return cls(
            enabled=settings.get("enabled", as_type=bool),
            api_url=settings.get("api_url", default="https://app.nocodb.com"),
            api_token=settings.get("api_token", default=""),
            workspace_id=settings.get("workspace_id", default=""),
            base_id=settings.get("base_id", default=""),
            orders_table_id=settings.get("orders_table_id", default=""),
            participants_table_id=settings.get("participants_table_id", default=""),
            questions_table_id=settings.get("questions_table_id", default=""),
        )

    @property
    def can_sync(self) -> bool:
        return (
            self.enabled
            and bool(self.api_url.strip())
            and bool(self.api_token.strip())
            and bool(self.base_id.strip())
        )
