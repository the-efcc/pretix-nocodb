from __future__ import annotations

from dataclasses import dataclass

from pretix.base.settings import SettingsSandbox, settings_hierarkey

PLUGIN_SETTINGS_PREFIX = ("plugin", "nocodb")

# How a new base is provisioned when an event has no base yet.
BASE_MODE_NEW = "new"
BASE_MODE_DUPLICATE = "duplicate"

_DEFAULTS = {
    "plugin_nocodb_enabled": ("False", bool),
    # api_url/token/workspace are configured once per organizer and inherited by
    # every event through pretix' settings cascade; they may still be overridden
    # per event if needed.
    "plugin_nocodb_api_url": ("https://app.nocodb.com", str),
    "plugin_nocodb_api_token": ("", str),
    "plugin_nocodb_workspace_id": ("", str),
    "plugin_nocodb_base_id": ("", str),
    "plugin_nocodb_participants_table_id": ("", str),
    # Base of the event this one was copied from (recorded on event copy). When
    # set, the event may duplicate that base's structure instead of starting
    # from scratch.
    "plugin_nocodb_source_base_id": ("", str),
    "plugin_nocodb_base_creation_mode": (BASE_MODE_NEW, str),
}


def register_settings_defaults() -> None:
    for key, (value, value_type) in _DEFAULTS.items():
        settings_hierarkey.add_default(key, value, value_type)


def settings_for_event(event) -> SettingsSandbox:
    return SettingsSandbox(*PLUGIN_SETTINGS_PREFIX, event)


def settings_for_organizer(organizer) -> SettingsSandbox:
    return SettingsSandbox(*PLUGIN_SETTINGS_PREFIX, organizer)


@dataclass(slots=True)
class NocoDBConfig:
    enabled: bool
    api_url: str
    api_token: str
    workspace_id: str
    base_id: str
    participants_table_id: str
    source_base_id: str
    base_creation_mode: str

    @classmethod
    def from_event(cls, event) -> NocoDBConfig:
        settings = settings_for_event(event)
        return cls(
            enabled=settings.get("enabled", as_type=bool),
            api_url=settings.get("api_url", default="https://app.nocodb.com"),
            api_token=settings.get("api_token", default=""),
            workspace_id=settings.get("workspace_id", default=""),
            base_id=settings.get("base_id", default=""),
            participants_table_id=settings.get("participants_table_id", default=""),
            source_base_id=settings.get("source_base_id", default=""),
            base_creation_mode=settings.get("base_creation_mode", default=BASE_MODE_NEW),
        )

    @property
    def should_duplicate_source_base(self) -> bool:
        return (
            self.base_creation_mode == BASE_MODE_DUPLICATE
            and bool(self.source_base_id.strip())
        )

    @property
    def can_sync(self) -> bool:
        return (
            self.enabled
            and bool(self.api_url.strip())
            and bool(self.api_token.strip())
        )
