from __future__ import annotations

from django.urls import re_path

from .views import NocoDBSettingsView

urlpatterns = [
    re_path(
        r"^control/event/(?P<organizer>[^/]+)/(?P<event>[^/]+)/nocodb/settings$",
        NocoDBSettingsView.as_view(),
        name="settings",
    ),
]
