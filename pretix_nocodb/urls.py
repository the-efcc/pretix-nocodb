from __future__ import annotations

from django.urls import re_path

from .views import NocoDBOrganizerSettingsView, NocoDBSettingsView, NocoDBSyncNowView

urlpatterns = [
    re_path(
        r"^control/event/(?P<organizer>[^/]+)/(?P<event>[^/]+)/nocodb/settings$",
        NocoDBSettingsView.as_view(),
        name="settings",
    ),
    re_path(
        r"^control/event/(?P<organizer>[^/]+)/(?P<event>[^/]+)/nocodb/sync$",
        NocoDBSyncNowView.as_view(),
        name="sync",
    ),
    re_path(
        r"^control/organizer/(?P<organizer>[^/]+)/nocodb/settings$",
        NocoDBOrganizerSettingsView.as_view(),
        name="organizer.settings",
    ),
]
