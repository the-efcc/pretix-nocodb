from __future__ import annotations

from typing import ClassVar

from django.utils.translation import gettext_lazy as _

from . import __version__

try:
    from pretix.base.plugins import PluginConfig
except ImportError:
    raise RuntimeError("Please use pretix 2026.3 or above to run this plugin!") from None


class PluginApp(PluginConfig):
    default = True
    name = "pretix_nocodb"
    verbose_name = "NocoDB"

    class PretixPluginMeta:
        name = _("NocoDB")
        author = "Sweenu"
        description = _("Pretix data sync provider for NocoDB")
        visible = True
        picture = "pretix_nocodb/nocodb_logo.png"
        version = __version__
        category = "INTEGRATION"
        compatibility = "pretix>=2026.3.0"
        settings_links: ClassVar = [
            ((_("Settings"), _("NocoDB")), "plugins:pretix_nocodb:settings", {}),
        ]

    def ready(self) -> None:
        from .plugin_settings import register_settings_defaults

        register_settings_defaults()
        from . import (
            signals,  # noqa: F401
            tasks,  # noqa: F401
        )
