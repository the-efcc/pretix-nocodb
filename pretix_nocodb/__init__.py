from __future__ import annotations

__version__ = "0.1.0"

def __getattr__(name: str) -> object:
    if name == "PretixPluginMeta":
        from .apps import PluginApp

        return PluginApp.PretixPluginMeta
    raise AttributeError(name)

__all__ = ["PretixPluginMeta", "__version__"]
