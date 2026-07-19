from __future__ import annotations

from django import forms
from django.utils.translation import gettext_lazy as _
from pretix.base.forms import SecretKeySettingsField, SettingsForm


class NocoDBSettingsForm(SettingsForm):
    plugin_nocodb_enabled = forms.BooleanField(
        label=_("Enable NocoDB sync"),
        help_text=_("When enabled, participants are synced to NocoDB on every change."),
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
        help_text=_(
            "Workspace to create the base in (required on NocoDB cloud when no "
            "base ID is set). Leave empty on self-hosted instances."
        ),
        required=False,
    )
    plugin_nocodb_base_id = forms.CharField(
        label=_("Base ID"),
        help_text=_(
            "Optional. ID of an existing NocoDB base to sync this event's data "
            "into. Leave empty to let the plugin create a base for this event "
            "automatically."
        ),
        required=False,
    )
