from intelipump_fdc.core.config import ControllerMode, Settings


def test_default_mode_is_listen_only(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    # Default-value tests must not inherit bench-host INTELIPUMP_CONTROLLER__MODE.
    monkeypatch.delenv("INTELIPUMP_CONTROLLER__MODE", raising=False)
    settings = Settings(_env_file=None)
    assert settings.controller.mode is ControllerMode.LISTEN_ONLY
    assert settings.safety.active_commands_enabled is False


def test_lab_identity_uses_lab_station(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("INTELIPUMP_CONTROLLER__STATION_ID", raising=False)
    settings = Settings(_env_file=None)
    assert settings.controller.station_id.endswith("-Lab")


def test_controller_mode_env_override_still_works(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("INTELIPUMP_CONTROLLER__MODE", "CONTINUOUS_POLL_BENCH")
    settings = Settings(_env_file=None)
    assert settings.controller.mode is ControllerMode.CONTINUOUS_POLL_BENCH
