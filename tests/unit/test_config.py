from intelipump_fdc.core.config import ControllerMode, Settings


def test_default_mode_is_listen_only() -> None:
    settings = Settings(_env_file=None)
    assert settings.controller.mode is ControllerMode.LISTEN_ONLY
    assert settings.safety.active_commands_enabled is False

def test_lab_identity_uses_lab_station() -> None:
    settings = Settings(_env_file=None)
    assert settings.controller.station_id.endswith("-Lab")
