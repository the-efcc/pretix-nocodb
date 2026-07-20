from __future__ import annotations

from typing import Any, cast

from django import forms
from django.utils.translation import gettext_lazy as _
from pretix.base.forms import SecretKeySettingsField, SettingsForm

from .plugin_settings import BASE_MODE_DUPLICATE, BASE_MODE_NEW


class OrganizerNocoDBSettingsForm(SettingsForm):
    plugin_nocodb_api_url = forms.URLField(
        label=_("NocoDB URL"),
        help_text=_("Base URL of your NocoDB instance, e.g. https://app.nocodb.com."),
        required=False,
    )
    plugin_nocodb_api_token = SecretKeySettingsField(
        label=_("API token"),
        help_text=_("Personal API token used to authenticate against NocoDB."),
        required=False,
    )
    plugin_nocodb_workspace_id = forms.CharField(
        label=_("Workspace ID"),
        help_text=_(
            "Workspace to create bases in (required on NocoDB cloud when no base "
            "ID is set). Leave empty on self-hosted instances."
        ),
        required=False,
    )


class NocoDBSettingsForm(SettingsForm):
    plugin_nocodb_enabled = forms.BooleanField(
        label=_("Enable NocoDB sync"),
        help_text=_("When enabled, participants are synced to NocoDB on every change."),
        required=False,
    )
    plugin_nocodb_base_id = forms.CharField(
        label=_("Base ID"),
        help_text=_(
            "Optional. ID of an existing NocoDB base to sync this event's data "
            "into. Leave empty to let the plugin provision a base for this event "
            "automatically on the next sync."
        ),
        required=False,
    )
    plugin_nocodb_base_creation_mode = forms.ChoiceField(
        label=_("How to provision the base"),
        required=False,
        widget=forms.RadioSelect,
        choices=(
            (BASE_MODE_NEW, _("Create a new empty base")),
            (
                BASE_MODE_DUPLICATE,
                _(
                    "Duplicate the base of the event this one was copied from "
                    "(keeps its tables, views and extra columns, but not the data)"
                ),
            ),
        ),
    )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # The provisioning choice only makes sense while the event has no base
        # yet and it was copied from an event that already had one.
        settings = cast(Any, self.obj).settings
        has_source = bool(settings.get("plugin_nocodb_source_base_id", default=""))
        has_base = bool(settings.get("plugin_nocodb_base_id", default=""))
        if not has_source or has_base:
            self.fields.pop("plugin_nocodb_base_creation_mode")
