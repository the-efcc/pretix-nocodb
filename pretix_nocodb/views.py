from __future__ import annotations

from django.urls import reverse
from pretix.base.models import Event
from pretix.control.views.event import EventSettingsFormView, EventSettingsViewMixin

from .forms import NocoDBSettingsForm


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
