from pathlib import Path

from r2x_plexos.plugin_config import PLEXOSConfig


def test_get_config_path_prefers_resolve_method(monkeypatch, tmp_path):
    """Use _resolve_config_path when available."""

    monkeypatch.setattr(
        PLEXOSConfig,
        "_resolve_config_path",
        lambda _unused: Path(tmp_path),
        raising=False,
    )

    resolved = PLEXOSConfig.get_config_path()
    assert resolved == Path(tmp_path)
