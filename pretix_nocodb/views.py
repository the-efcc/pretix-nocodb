from __future__ import annotations

import logging

from django.contrib import messages
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from django.views import View
from pretix.base.models import Event, Organizer
from pretix.control.permissions import EventPermissionRequiredMixin
from pretix.control.views.event import EventSettingsFormView, EventSettingsViewMixin
from pretix.control.views.organizer import OrganizerSettingsFormView

from .client import NocoDBAPIError
from .forms import NocoDBSettingsForm, OrganizerNocoDBSettingsForm
from .sync import NocoDBSyncService
from .tasks import sync_all_orders_to_nocodb

logger = logging.getLogger(__name__)


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
        service = NocoDBSyncService(request.event)
        if service.config.can_sync:
            # Provision the base here rather than leaving it to the background
            # task: the settings page is rendered again as soon as this returns,
            # and it has to show the base id the sync will use. Rendering it
            # empty invites a save that stores the empty value back, which
            # unbinds the event and makes the next sync create another base.
            try:
                service.ensure_base()
            except NocoDBAPIError as exc:
                logger.exception("Provisioning the NocoDB base for %s failed", request.event.slug)
                messages.error(
                    request,
                    _("Could not create the NocoDB base: {error}").format(error=exc),
                )
                return self._redirect_to_settings(request)

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
        return self._redirect_to_settings(request)

    def _redirect_to_settings(self, request):
        return redirect(reverse(
            "plugins:pretix_nocodb:settings",
            kwargs={
                "organizer": request.organizer.slug,
                "event": request.event.slug,
            },
        ))
