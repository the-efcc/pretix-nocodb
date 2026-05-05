from __future__ import annotations

from django import forms
from django.utils.translation import gettext_lazy as _
from pretix.base.forms import SecretKeySettingsField, SettingsForm


class NocoDBSettingsForm(SettingsForm):
    plugin_nocodb_enabled = forms.BooleanField(
        label=_("Enable NocoDB sync"),
        help_text=_("When enabled, orders are synced to NocoDB on every change."),
        required=False,
    )
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
        help_text=_("Optional workspace ID. Leave empty to use the default workspace."),
        required=False,
    )
    plugin_nocodb_base_id = forms.CharField(
        label=_("Base ID"),
        help_text=_(
            "Optional. If empty, the plugin looks up or creates a base named after this event."
        ),
        required=False,
    )
    plugin_nocodb_base_title = forms.CharField(
        label=_("Base title"),
        help_text=_("Title used when creating a new base. Defaults to pretix_<organizer>_<event>."),
        required=False,
    )
