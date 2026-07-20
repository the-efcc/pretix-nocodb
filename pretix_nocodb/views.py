from __future__ import annotations

from django.contrib import messages
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from django.views import View
from pretix.base.models import Event, Organizer
from pretix.control.permissions import EventPermissionRequiredMixin
from pretix.control.views.event import EventSettingsFormView, EventSettingsViewMixin
from pretix.control.views.organizer import OrganizerSettingsFormView

from .forms import NocoDBSettingsForm, OrganizerNocoDBSettingsForm
from .plugin_settings import NocoDBConfig
from .tasks import sync_all_orders_to_nocodb


class NocoDBOrganizerSettingsView(OrganizerSettingsFormView):
    model = Organizer
    form_class = OrganizerNocoDBSettingsForm
    template_name = "pretix_nocodb/organizer_settings.html"
    permission = "organizer.settings.general:write"

    def get_success_url(self) -> str:
        return reverse(
            "plugins:pretix_nocodb:organizer.settings",
            kwargs={"organizer": self.request.organizer.slug},
        )


class NocoDBSettingsView(EventSettingsViewMixin, EventSettingsFormView):
    model = Event
    form_class = NocoDBSettingsForm
    template_name = "pretix_nocodb/settings.html"
    permission = "event.settings.general:write"

    def get_success_url(self) -> str:
        return reverse(
            "plugins:pretix_nocodb:settings",
            kwargs={
                "organizer": self.request.event.organizer.slug,
                "event": self.request.event.slug,
            },
        )


class NocoDBSyncNowView(EventPermissionRequiredMixin, View):
    permission = "event.settings.general:write"

    def post(self, request, *args, **kwargs):
        if NocoDBConfig.from_event(request.event).can_sync:
            sync_all_orders_to_nocodb.apply_async(kwargs={"event": request.event.pk})
            messages.success(
                request, _("Sync started. All orders will be synced to NocoDB shortly.")
            )
        else:
            messages.error(
                request,
                _(
                    "NocoDB sync is not configured. Enable it and fill in the "
                    "NocoDB URL and API token first."
                ),
            )
        return redirect(reverse(
            "plugins:pretix_nocodb:settings",
            kwargs={
                "organizer": request.organizer.slug,
                "event": request.event.slug,
            },
        ))
